import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib

# Generar Dataset Sintético
np.random.seed(42)
muestras = 2000

# Variables: Nivel de Agua (0-4), Temp (°C), Luxes (0-480), pH (0-14)
agua = np.random.randint(0, 5, muestras)
temp = np.random.normal(26.0, 3.0, muestras)
luxes = np.random.randint(0, 480, muestras)
ph = np.random.normal(8.0, 1.0, muestras)

df = pd.DataFrame({'water': agua, 'temp': temp, 'luxes': luxes, 'ph': ph})

# Definir condiciones de falla individuales
falla_agua = df['water'] < 2
falla_ph = (df['ph'] < 7.5) | (df['ph'] > 8.5)
falla_temp = (df['temp'] > 28.0) | (df['temp'] < 22.0)
falla_luz = (df['luxes'] < 50) | (df['luxes'] > 300)

df['estado'] = "Crecimiento Óptimo"

# Asignación de fallas individuales
df.loc[falla_agua, 'estado'] = "Nivel de Agua Bajo"
df.loc[falla_ph, 'estado'] = "Riesgo de pH"
df.loc[falla_temp, 'estado'] = "Estrés Térmico"
df.loc[falla_luz, 'estado'] = "Alerta Lumínica"

# Asignación de combinaciones de múltiples fallas
df.loc[falla_agua & falla_temp, 'estado'] = "Alerta: Agua y Temperatura"
df.loc[falla_agua & falla_ph, 'estado'] = "Alerta: Agua y pH"
df.loc[falla_ph & falla_temp, 'estado'] = "Alerta: pH y Temperatura"
df.loc[falla_ph & falla_luz, 'estado'] = "Alerta: pH e Iluminación"
df.loc[falla_temp & falla_luz, 'estado'] = "Alerta: Temp e Iluminación"

# Fallas múltiples severas (3 o más parámetros descalibrados)
fallas_totales = falla_agua.astype(int) + falla_ph.astype(int) + falla_temp.astype(int) + falla_luz.astype(int)
df.loc[fallas_totales >= 3, 'estado'] = "EMERGENCIA: Falla Múltiple"

X = df[['water', 'temp', 'luxes', 'ph']]
y = df['estado']

# Entrenar Random Forest
print("Entrenando modelo con todas las variables...")
modelo = RandomForestClassifier(n_estimators=150, max_depth=8, random_state=42)
modelo.fit(X, y)

# Exportar Modelo
joblib.dump(modelo, "random_forest_corales.pkl")
print("Modelo guardado exitosamente como 'random_forest_corales.pkl'")