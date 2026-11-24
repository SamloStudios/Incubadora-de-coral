import pandas as pd
import numpy as np

df = pd.read_csv("data/bitacora_alertas_e2e.csv")
col = "latencia_total_e2e_ms"

print(f"Total de pruebas: {len(df)}")
print(f"Media:   {df[col].mean():.2f} ms")
print(f"Mediana: {df[col].median():.2f} ms")
print(f"P95:     {np.percentile(df[col], 95):.2f} ms")