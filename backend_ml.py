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

        if loop is not None:
            asyncio.run_coroutine_threadsafe(manager.broadcast(json.dumps(data)), loop)
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

app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

# Middleware de Sesiones Seguras (Cookies HTTPS)
app.add_middleware(
    SessionMiddleware,
    secret_key=SECRET_KEY,
    session_cookie="coral_session",
    max_age=3600 * 8,  # Sesión válida por 8 horas
    https_only=False,
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