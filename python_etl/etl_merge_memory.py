from sqlalchemy import create_engine
from pymongo import MongoClient
import pandas as pd
import unicodedata

# ----------------------------
# 1) Leer SQL
# ----------------------------
engine = create_engine("postgresql+psycopg2://lab:lab@localhost:5432/lab05")

df_sql = pd.read_sql("""
    SELECT
        id_pais,
        nombre_pais,
        tasa_de_envejecimiento
    FROM pais_envejecimiento
""", engine)

# ----------------------------
# 2) Leer Mongo
# ----------------------------
MONGO_URI = "URI_MONGO"

client = MongoClient(MONGO_URI)
db = client["lab05"]
collection = db["costos_turisticos"]

docs = list(collection.find({}, {
    "_id": 1,
    "continente": 1,
    "región": 1,
    "país": 1,
    "capital": 1,
    "población": 1,
    "costos_diarios_estimados_en_dólares": 1
}))

df_mongo = pd.DataFrame(docs)

# ----------------------------
# 3) Función para normalizar nombres
# ----------------------------
def normalize_country_name(name: str) -> str:
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return name

# ----------------------------
# 4) Normalizar llave de merge
# ----------------------------
df_sql["pais_key"] = df_sql["nombre_pais"].apply(normalize_country_name)
df_mongo["pais_key"] = df_mongo["país"].apply(normalize_country_name)

# ----------------------------
# 5) Aplanar campos anidados de Mongo
# ----------------------------
df_mongo_flat = pd.DataFrame({
    "pais_key": df_mongo["pais_key"],
    "mongo_id": df_mongo["_id"].astype(str),
    "continente": df_mongo["continente"],
    "region": df_mongo["región"],
    "pais_mongo": df_mongo["país"],
    "capital": df_mongo["capital"],
    "poblacion": df_mongo["población"],

    "hospedaje_bajo": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["hospedaje"]["precio_bajo_usd"]),
    "hospedaje_promedio": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["hospedaje"]["precio_promedio_usd"]),
    "hospedaje_alto": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["hospedaje"]["precio_alto_usd"]),

    "comida_bajo": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["comida"]["precio_bajo_usd"]),
    "comida_promedio": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["comida"]["precio_promedio_usd"]),
    "comida_alto": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["comida"]["precio_alto_usd"]),

    "transporte_bajo": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["transporte"]["precio_bajo_usd"]),
    "transporte_promedio": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["transporte"]["precio_promedio_usd"]),
    "transporte_alto": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["transporte"]["precio_alto_usd"]),

    "entretenimiento_bajo": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["entretenimiento"]["precio_bajo_usd"]),
    "entretenimiento_promedio": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["entretenimiento"]["precio_promedio_usd"]),
    "entretenimiento_alto": df_mongo["costos_diarios_estimados_en_dólares"].apply(lambda x: x["entretenimiento"]["precio_alto_usd"]),
})

# ----------------------------
# 6) Integrar en memoria
# ----------------------------
df_integrado = pd.merge(
    df_sql,
    df_mongo_flat,
    on="pais_key",
    how="left"
)

# ----------------------------
# 7) Validaciones
# ----------------------------
print("Filas SQL:", len(df_sql))
print("Filas Mongo:", len(df_mongo_flat))
print("Filas integradas:", len(df_integrado))

print("\nPrimeras 5 filas integradas:")
print(df_integrado.head())

print("\nValores nulos por columna:")
print(df_integrado.isnull().sum())

print("\nColumnas finales:")
print(df_integrado.columns.tolist())