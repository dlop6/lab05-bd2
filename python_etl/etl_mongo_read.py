from pymongo import MongoClient
import pandas as pd

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

print("Documentos leídos:", len(df_mongo))
print("\nPrimeros 5 documentos:")
print(df_mongo.head())

print("\nColumnas:")
print(df_mongo.columns.tolist())

print("\nValores nulos por columna:")
print(df_mongo.isnull().sum())