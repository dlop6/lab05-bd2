from sqlalchemy import create_engine
import pandas as pd

# conexión a PostgreSQL
engine = create_engine("postgresql+psycopg2://lab:lab@localhost:5432/lab05")

# leer tabla SQL
df_sql = pd.read_sql("""
    SELECT
        id_pais,
        nombre_pais,
        tasa_de_envejecimiento
    FROM pais_envejecimiento
""", engine)

print("Filas leídas:", len(df_sql))
print("\nPrimeras 5 filas:")
print(df_sql.head())

print("\nTipos de datos:")
print(df_sql.dtypes)

print("\nValores nulos por columna:")
print(df_sql.isnull().sum())