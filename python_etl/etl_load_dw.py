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
MONGO_URI = "mongodb+srv://dlopreinoso_db_user:QGNmgTThcmA4LHpt@cluster0.etael0e.mongodb.net/"

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
# 3) Normalizar nombres
# ----------------------------
def normalize_country_name(name: str) -> str:
    if pd.isna(name):
        return ""
    name = str(name).strip().lower()
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    return name

df_sql["pais_key"] = df_sql["nombre_pais"].apply(normalize_country_name)
df_mongo["pais_key"] = df_mongo["país"].apply(normalize_country_name)

# ----------------------------
# 4) Aplanar Mongo
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
# 5) Integrar
# ----------------------------
df_integrado = pd.merge(
    df_sql,
    df_mongo_flat,
    on="pais_key",
    how="left"
)

# quitar llave auxiliar antes de cargar
df_final = df_integrado.drop(columns=["pais_key"])

# ----------------------------
# 6) Cargar al DW
# ----------------------------
rows_written = df_final.to_sql(
    name="paises_integrados_python",
    con=engine,
    schema="dw",
    if_exists="append",
    index=False
)

print("Filas cargadas al DW:", rows_written)
print("Filas en dataframe final:", len(df_final))