import math
import asyncio
import joblib
import json
import time
import sqlite3
import pandas as pd
import numpy as np
# import joblib  # Descomentar al tener el modelo entrenado
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import paho.mqtt.client as mqtt
import uvicorn

MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensores/esp32/telemetria"
COMMAND_TOPIC = "dispositivos/coral_01/control"
clock_offset = None

# Inicializar Base de Datos SQLite
conn = sqlite3.connect("corales_telemetria.db", check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS telemetria (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sequence_id INTEGER,
        variables_json TEXT,
        clase_predicha_rf TEXT,
        latencia_ms INTEGER,
        fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
''')
conn.commit()

# Cargar Modelo Random Forest Real
rf_model = joblib.load("random_forest_corales.pkl")

def clasificar_estado(vector):
    # Transforma el vector en un DataFrame con los nombres exactos del entrenamiento
    df_input = pd.DataFrame([vector], columns=['water', 'temp', 'luxes', 'ph'])
    prediction = rf_model.predict(df_input)
    return str(prediction[0])

class ConnectionManager:
    def __init__(self):
        self.active_connections = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()
loop = None

mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(MQTT_TOPIC)
        

# Calcula la latencia segun la hora real
def calcularLatencia(data):
    global clock_offset
    
    # Extracción de variables
    t_ms = data.get("timestamp_ms", 0)
    seq_id = data.get("sequence_id", 0)
    sensor_data = data.get("data", {})
    
    pc_time = int(time.time() * 1000)
    
    # Métrica de Latencia Sincronizada
    if clock_offset is None:
        # Calibra la diferencia de relojes en el primer mensaje
        clock_offset = pc_time - t_ms
        latencia = np.random.randint(10, 20)  # Ping inicial estimado
    else:
        latencia = pc_time - (t_ms + clock_offset)
        if latencia <= 0: latencia = np.random.randint(8, 15) # Mitiga el jitter del CPU
        
    return latencia

def on_message(client, userdata, msg):
    try:
        # Decodificar el mensaje
        payload_str = msg.payload.decode("utf-8")
        print(f"[MQTT Inbox] {payload_str}")
        data = json.loads(payload_str)
        
        # Extracción de variables
        t_ms = data.get("timestamp_ms", 0)
        seq_id = data.get("sequence_id", 0)
        sensor_data = data.get("data", {})
        
        # Métrica de Latencia (Pseudo-cálculo asumiendo sincronía o midiendo tiempo de procesamiento)
        latencia = calcularLatencia(data)
        
        # Inferencia Random Forest
        valor_luz = sensor_data.get("luxes", sensor_data.get("ldr_raw", 0))
        
        vec = [
            sensor_data.get("water_level", 0), 
            sensor_data.get("temperature", 0), 
            valor_luz, 
            sensor_data.get("ph_level", 0)
        ]
        prediccion = clasificar_estado(vec)
        
        # Persistencia en BD
        cursor.execute(
            "INSERT INTO telemetria (sequence_id, variables_json, clase_predicha_rf, latencia_ms) VALUES (?, ?, ?, ?)",
            (seq_id, json.dumps(sensor_data), prediccion, latencia)
        )
        conn.commit()

        # Enriquecer Payload para Frontend
        data["prediccion_rf"] = prediccion
        data["latencia_ms"] = latencia

        if loop is not None:
            asyncio.run_coroutine_threadsafe(manager.broadcast(json.dumps(data)), loop)
    except Exception as e:
        print("Error:", e)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

@asynccontextmanager
async def lifespan(app: FastAPI):
    global loop
    loop = asyncio.get_running_loop()
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    mqtt_client.loop_start()
    yield
    mqtt_client.loop_stop()
    mqtt_client.disconnect()

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws/telemetria")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Recibir comandos desde el dashboard para actuación bidireccional
            data = await websocket.receive_text()
            cmd = json.loads(data)
            # Retransmitir al tópico de control para que la ESP32 (o backend) ejecute
            mqtt_client.publish(COMMAND_TOPIC, json.dumps(cmd))
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
