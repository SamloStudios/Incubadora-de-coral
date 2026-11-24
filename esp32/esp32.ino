#include <WiFi.h>
#include <PubSubClient.h>
#include <Arduino_JSON.h>
#include <WebServer.h>
#include <LiquidCrystal.h>
#include <OneWire.h>
#include <DallasTemperature.h>

// [FiltroPromedioMovil class]
class FiltroPromedioMovil {
  private:
    static const int SIZE = 10;
    int buffer[SIZE];
    int indice;
    long suma;
    int lecturas;
  public:
    FiltroPromedioMovil() {
      indice = 0; suma = 0; lecturas = 0;
      for (int i = 0; i < SIZE; i++) buffer[i] = 0;
    }
    float filtrar(int nuevoValor) {
      suma -= buffer[indice]; buffer[indice] = nuevoValor; suma += nuevoValor;
      indice = (indice + 1) % SIZE;
      if (lecturas < SIZE) lecturas++;
      return (float)suma / lecturas;
    }
};

FiltroPromedioMovil filtroAgua; FiltroPromedioMovil filtroTemp;
FiltroPromedioMovil filtroLdr; FiltroPromedioMovil filtroPh;

const char* ssid = "INFINITUM2C4B";
const char* password = "k3RNhG452C";

WiFiClient espClient;
PubSubClient mqttClient(espClient);
const char* mqtt_server = "192.168.1.68";

#define POWER_PIN_WATER 32
#define TMP_PIN         33
#define LDR_PIN         36
#define PH_PIN          35
#define WATER_LEVEL_PIN 34
#define RELAY_PIN       26     // Motor de agua

// PINES AÑADIDOS PARA CONTROLES DEFAULT DASHBOARD (2 LEDs)
#define LED1_PIN        27
#define LED2_PIN        12

const int rs = 13, en = 14, d4 = 18, d5 = 19, d6 = 21, d7 = 22;
LiquidCrystal lcd(rs, en, d4, d5, d6, d7);

OneWire oneWire(TMP_PIN);
DallasTemperature sensors(&oneWire);

#define SENSOR_MIN 0
#define SENSOR_MAX 521

WebServer server(80);

unsigned long previousTxMillis = 0;
const long txInterval = 5000;
unsigned long previousSampleMillis = 0;
const long sampleInterval = 100;
bool waterSensorActive = false;
unsigned long waterSensorStartTime = 0;

int finalWaterLevel = 0;
float finalTemperatureC = 0.0;
int finalLdr = 0;
float finalPh = 0.0;
int digitTemp = 0;
unsigned long sequence_id = 0; // Añadido para métricas de paquetes

void handleMotorOn() { digitalWrite(RELAY_PIN, HIGH); server.send(200, "application/json", "{\"status\":\"motor encendido\"}"); Serial.print("MOTOR ON");}
void handleMotorOff() { digitalWrite(RELAY_PIN, LOW); server.send(200, "application/json", "{\"status\":\"motor apagado\"}"); Serial.print("MOTOR ON");}
void handleLed1On() { digitalWrite(LED1_PIN, HIGH); server.send(200, "application/json", "{\"status\":\"led 1 encendido\"}"); Serial.print("LED1 ON");}
void handleLed1Off() { digitalWrite(LED1_PIN, LOW); server.send(200, "application/json", "{\"status\":\"led 1 apagado\"}"); Serial.print("LED1 ON");}
void handleLed2On() { digitalWrite(LED2_PIN, HIGH); server.send(200, "application/json", "{\"status\":\"led 2 encendido\"}"); Serial.print("LED1 ON");}
void handleLed2Off() { digitalWrite(LED2_PIN, LOW); server.send(200, "application/json", "{\"status\":\"led 2 apagado\"}"); Serial.print("LED1 ON");}

double calcular_ldr_a_lumenes(int adc_valor, double r_fija, double adc_max, double v_in) {
    // Protección contra divisiones por cero o valores fuera de rango
    if (adc_valor <= 0) {
        adc_valor = 1;
    }
    if (adc_valor >= adc_max) {
        adc_valor = (int)adc_max - 1; 
    }

    // 1. Calcular el voltaje medido por el ADC 
    // Forzamos la conversión a double casteando 'adc_valor'
    double v_out = ((double)adc_valor / adc_max) * v_in;
    
    // 2. Calcular la resistencia del LDR
    double r_ldr = r_fija * ((v_in / v_out) - 1.0);
    
    // 3. Calcular Luxes mediante el modelo empírico: Lux = A * (R_ldr ^ B)
    double A = 1.25e7;  
    double B = -1.405;  
    
    double luxes = A * pow(r_ldr, B);
    
    return luxes;
}

void mqttCallback(char* topic, byte* payload, unsigned int length) {
  String msg = "";
  for (unsigned int i = 0; i < length; i++) {
    msg += (char)payload[i];
  }
  JSONVar cmd = JSON.parse(msg);
  if (JSON.typeof(cmd) == "undefined") return;

  String target = (const char*)cmd["target"];
  String command = (const char*)cmd["command"];
  bool encender = (command == "on");

  if (target == "motor") digitalWrite(RELAY_PIN, encender ? HIGH : LOW);
  else if (target == "led1") digitalWrite(LED1_PIN, encender ? HIGH : LOW);
  else if (target == "led2") digitalWrite(LED2_PIN, encender ? HIGH : LOW);
}

void setup() {
  Serial.begin(115200); delay(1000);
  lcd.begin(16, 2); lcd.clear(); lcd.print("Iniciando...");

  analogSetAttenuation(ADC_11db);
  sensors.begin(); sensors.setWaitForConversion(false);

  pinMode(POWER_PIN_WATER, OUTPUT); pinMode(RELAY_PIN, OUTPUT);
  pinMode(LED1_PIN, OUTPUT); pinMode(LED2_PIN, OUTPUT);
  digitalWrite(POWER_PIN_WATER, LOW); digitalWrite(RELAY_PIN, LOW);
  digitalWrite(LED1_PIN, LOW); digitalWrite(LED2_PIN, LOW);

  WiFi.mode(WIFI_STA); WiFi.disconnect(true); delay(100);
  WiFi.setSleep(false); WiFi.begin(ssid, password);
  
  while (WiFi.status() != WL_CONNECTED) { delay(500); }
  
  server.on("/motor/on", HTTP_GET, handleMotorOn);
  server.on("/motor/off", HTTP_GET, handleMotorOff);
  server.on("/led1/on", HTTP_GET, handleLed1On);
  server.on("/led1/off", HTTP_GET, handleLed1Off);
  server.on("/led2/on", HTTP_GET, handleLed2On);
  server.on("/led2/off", HTTP_GET, handleLed2Off);
  server.begin();

  mqttClient.setServer(mqtt_server, 1883);
  mqttClient.setCallback(mqttCallback);
}

void loop() {
  server.handleClient();
  unsigned long currentMillis = millis();

  if (WiFi.status() == WL_CONNECTED) {
    if (!mqttClient.connected()) { mqttClient.connect("ESP32_Coral"); Serial.print(WiFi.localIP()); }
    mqttClient.loop();
  }

  if (currentMillis - previousSampleMillis >= sampleInterval && !waterSensorActive) {
    digitalWrite(POWER_PIN_WATER, HIGH);
    waterSensorActive = true; waterSensorStartTime = currentMillis;
  }

  if (waterSensorActive && (currentMillis - waterSensorStartTime >= 20)) {
    int rawWater = analogRead(WATER_LEVEL_PIN);
    digitalWrite(POWER_PIN_WATER, LOW);
    waterSensorActive = false; previousSampleMillis = currentMillis;
 
    // Ejecutar los filtros en los datos de los sensores
    float avgWaterRaw = filtroAgua.filtrar(rawWater);
    float avgLdrRaw   = filtroLdr.filtrar(analogRead(LDR_PIN));
    float avgPhRaw    = filtroPh.filtrar(analogRead(PH_PIN));

    int ldr_cap = ((int)avgLdrRaw - 500 > 0) ? (int)avgLdrRaw - 500 : 0;

    int mis_luxes = calcular_ldr_a_lumenes(ldr_cap, 10000.0, 4095.0, 3.3);

    finalWaterLevel = constrain(map(avgWaterRaw, SENSOR_MIN, SENSOR_MAX, 0, 4), 0, 4);
    finalLdr = mis_luxes;
    float voltagePh = (avgPhRaw / 4095.0) * 3.3;
    finalPh = 7.0 + ((2.5 - voltagePh) / 0.18);
  }

  if (currentMillis - previousTxMillis >= txInterval) {
    previousTxMillis = currentMillis;
    sensors.requestTemperatures(); digitTemp = sensors.getTempCByIndex(0);

    JSONVar doc;
    JSONVar data;
    
    // Payload formateado según requerimientos del Caso de Uso (Estructura Anidada)
    doc["node_id"] = "ESP32_CORAL_01";
    doc["sequence_id"] = sequence_id++;
    doc["timestamp_ms"] = currentMillis;
    
    data["water_level"] = finalWaterLevel;
    data["temperature"] = digitTemp;
    data["ldr_raw"]     = finalLdr;
    data["ph_level"]    = round(finalPh * 100.0) / 100.0;
    doc["data"] = data;

    String jsonPayload = JSON.stringify(doc);
    if (mqttClient.connected()) {
      mqttClient.publish("sensores/esp32/telemetria", jsonPayload.c_str());

      // New Lines
      if (mqttClient.connect("ESP32_Coral")) {
        mqttClient.subscribe("dispositivos/coral_01/control");
        Serial.print(WiFi.localIP());
      }
    }
  }

  // Tratar con comandos
  if (Serial.available() > 0) {
    // Lee lo que haya en el buffer hasta el salto de línea sin detener el loop
    String input = Serial.readStringUntil('\n');
    input.trim(); // Quita espacios o caracteres ocultos como \r

    if (input.length() > 0) {
      Serial.print("Comando recibido por Serial: ");
      Serial.println(input);

      // Ejemplo de acciones basadas en lo que ingrese el usuario
      if (input == "REBOOT") {
        ESP.restart();
      } 
      else if (input == "STATUS") {
        Serial.print("IP Local: "); Serial.println(WiFi.localIP());
        Serial.print("MQTT Conectado: "); Serial.println(mqttClient.connected() ? "SI" : "NO");
      }
      
      // Puedes agregar más comandos aquí (ej. cambiar txInterval o sampleInterval)
      
    }
  }
}
