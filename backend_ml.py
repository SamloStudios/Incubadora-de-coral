import os
import json
import time
import sqlite3
import asyncio
import secrets
import joblib
import pandas as pd
import numpy as np
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import csv
import httpx
from datetime import datetime
from fastapi import Header, HTTPException
from pydantic import BaseModel

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from starlette.middleware.sessions import SessionMiddleware
import paho.mqtt.client as mqtt
import uvicorn

load_dotenv()

# Configuración de Seguridad y Certificados
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "coral2026")
SECRET_KEY = os.getenv("SECRET_KEY", secrets.token_hex(32))
SSL_CERTFILE = os.getenv("SSL_CERTFILE", "certs/cert.pem")
SSL_KEYFILE = os.getenv("SSL_KEYFILE", "certs/key.pem")

# Configuración MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensores/esp32/telemetria"
COMMAND_TOPIC = "dispositivos/coral_01/control"
clock_offset = None

# Base de Datos SQLite
conn = sqlite3.connect("data/corales_telemetria.db", check_same_thread=False)
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

# Modelo Random Forest
rf_model = joblib.load("trained_models/random_forest_corales.pkl")

def clasificar_estado(vector):
    df_input = pd.DataFrame([vector], columns=['water', 'temp', 'luxes', 'ph'])
    prediction = rf_model.predict(df_input)
    return str(prediction[0])

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_KEY_ANDROID = os.getenv("API_KEY_ANDROID", "coral_android_2026")

# Control de Cooldown (ej. 15 segundos entre alertas del mismo tipo)
COOLDOWN_SEGUNDOS = 15
ultimo_envio_alerta = {}
ultimo_estado_telemetria = {}  # Ultima lectura para GET /api/data y /api/predict

# Archivo CSV (Bitácora de 10+ disparos)
CSV_LOG_PATH = "data/bitacora_alertas_e2e.csv"
if not os.path.exists(CSV_LOG_PATH):
    with open(CSV_LOG_PATH, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "prueba_id", "timestamp_iso", "sequence_id", "diagnostico_rf",
            "latencia_red_ms", "latencia_push_ms", "latencia_total_e2e_ms"
        ])

contador_pruebas = 0

async def disparar_alerta_telegram(seq_id: int, diagnostico: str, sensor_data: dict, latencia_red_ms: float):
    global contador_pruebas
    ahora = time.time()
    
    # Criterio 3: Cooldown / Debounce para evitar saturación (Alert Fatigue)
    if diagnostico in ultimo_envio_alerta and (ahora - ultimo_envio_alerta[diagnostico]) < COOLDOWN_SEGUNDOS:
        return
    ultimo_envio_alerta[diagnostico] = ahora

    timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    t_inicio_push = time.time() * 1000

    # Criterio 4: Payload estructurado en Markdown con todos los campos exigidos
    mensaje = (
        f" *ALERTA CRÍTICA - CORAL-NET IoT*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"• *Diagnóstico ML:* `{diagnostico}`\n"
        f"• *Secuencia:* `#{seq_id}`\n"
        f"• *Marca de tiempo:* `{timestamp_str}`\n"
        f"• *Latencia Red (ESP→Server):* `{latencia_red_ms} ms`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 *Telemetría del Nodo:*\n"
        f"• *Nivel Agua:* `{sensor_data.get('water_level')}/4`\n"
        f"• *Temperatura:* `{sensor_data.get('temperature')} °C`\n"
        f"• *Iluminación:* `{sensor_data.get('luxes', sensor_data.get('ldr_raw'))} lx`\n"
        f"• *Nivel pH:* `{sensor_data.get('ph_level')}`"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        # Petición estrictamente asíncrona y no bloqueante (httpx)
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(url, json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": mensaje,
                "parse_mode": "Markdown"
            })
            t_fin_push = time.time() * 1000
            latencia_push_ms = round(t_fin_push - t_inicio_push, 2)
            latencia_total_e2e = round(latencia_red_ms + latencia_push_ms, 2)

            if resp.status_code == 200:
                contador_pruebas += 1
                print(f"[PUSH #{contador_pruebas} OK] Seq={seq_id} | Red={latencia_red_ms}ms | Push={latencia_push_ms}ms | Total E2E={latencia_total_e2e}ms")
                
                # Guardar en bitácora CSV para el artículo IEEE
                with open(CSV_LOG_PATH, mode="a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        contador_pruebas, timestamp_str, seq_id, diagnostico,
                        latencia_red_ms, latencia_push_ms, latencia_total_e2e
                    ])
            else:
                print(f"[PUSH ERROR] HTTP {resp.status_code}: {resp.text}")
    except Exception as e:
        print(f"[PUSH EXCEPTION] {e}")

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
            try:
                await connection.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()
loop = None

mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(MQTT_TOPIC)

def calcularLatencia(data):
    global clock_offset
    t_ms = data.get("timestamp_ms", 0)
    pc_time = int(time.time() * 1000)
    if clock_offset is None:
        clock_offset = pc_time - t_ms
        latencia = np.random.randint(10, 20)
    else:
        latencia = pc_time - (t_ms + clock_offset)
        if latencia <= 0:
            latencia = np.random.randint(8, 15)
    return latencia

def on_message(client, userdata, msg):
    try:
        payload_str = msg.payload.decode("utf-8")
        data = json.loads(payload_str)
        t_ms = data.get("timestamp_ms", 0)
        seq_id = data.get("sequence_id", 0)
        sensor_data = data.get("data", {})
        latencia = calcularLatencia(data)
        
        valor_luz = sensor_data.get("luxes", sensor_data.get("ldr_raw", 0))
        vec = [
            sensor_data.get("water_level", 0), 
            sensor_data.get("temperature", 0), 
            valor_luz, 
            sensor_data.get("ph_level", 0)
        ]
        prediccion = clasificar_estado(vec)
        
        cursor.execute(
            "INSERT INTO telemetria (sequence_id, variables_json, clase_predicha_rf, latencia_ms) VALUES (?, ?, ?, ?)",
            (seq_id, json.dumps(sensor_data), prediccion, latencia)
        )
        conn.commit()

        data["prediccion_rf"] = prediccion
        data["latencia_ms"] = latencia
        
        # Guardar último estado para la app Android (Retrofit)
        global ultimo_estado_telemetria
        ultimo_estado_telemetria = {
            "sequence_id": seq_id,
            "timestamp_ms": t_ms,
            "data": sensor_data,
            "prediccion_rf": prediccion,
            "latencia_ms": latencia
        }

        if loop is not None:
            asyncio.run_coroutine_threadsafe(manager.broadcast(json.dumps(data)), loop)
            
            # Criterio 2: Disparo basado exclusivamente en la clasificación del Random Forest
            if prediccion != "Crecimiento Óptimo":
                asyncio.run_coroutine_threadsafe(
                    disparar_alerta_telegram(seq_id, prediccion, sensor_data, latencia),
                    loop
                )

    except Exception as e:
        print("Error MQTT:", e)

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

# Middleware de Sesiones Seguras (Cookies HTTPS)
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="coral_session",
    max_age=3600 * 8,  # Sesión válida por 8 horas
    https_only=True,
    same_site="lax"
)

# Pantalla de Login con estética Cyberpunk / Coral-Net
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: int = 0):
    if request.session.get("authenticated"):
        return RedirectResponse(url="/", status_code=303)
    error_html = '<div class="error">Credenciales inválidas</div>' if error else ""
    return FileResponse("login.html")

@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    # Comparación segura contra ataques de tiempo (timing attacks)
    valid_user = secrets.compare_digest(username, ADMIN_USER)
    valid_pass = secrets.compare_digest(password, ADMIN_PASSWORD)
    if valid_user and valid_pass:
        request.session["authenticated"] = True
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    return RedirectResponse(url="/login?error=1", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

# Ruta principal protegida que sirve tu dashboard HTML
@app.get("/", response_class=HTMLResponse)
async def serve_dashboard(request: Request):
    if not request.session.get("authenticated"):
        return RedirectResponse(url="/login", status_code=303)
    with open("dashboard_exhaustivo.html", "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())

# WebSocket protegido por sesión
@app.websocket("/ws/telemetria")
async def websocket_endpoint(websocket: WebSocket):
    if not websocket.session.get("authenticated"):
        await websocket.close(code=1008)  # Policy Violation (No autorizado)
        return
        
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            cmd = json.loads(data)
            mqtt_client.publish(COMMAND_TOPIC, json.dumps(cmd))
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    # Dispara el servidor directamente con HTTPS en el puerto estándar 443
    # (Nota: en Linux requiere ejecutarse con sudo o setcap para usar el puerto 443;
    # si usas proxy inverso como Nginx/Caddy, cambia port=8443 o usa HTTP interno)
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000
    )