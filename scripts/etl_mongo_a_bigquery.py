"""
Que es: el script ETL del proyecto (Extract, Transform, Load).

Que hace: lee las tablas dim_tiempo, dim_cliente, dim_producto y fact_ventas
en MongoDB Atlas y las sube a BigQuery (dataset urbanrush_dw).
MongoDB guarda las ventas del dia a dia; BigQuery sirve para analizar.

Como usarlo: python scripts/etl_mongo_a_bigquery.py
"""
import os
from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pymongo
from bson import ObjectId
from bson.decimal128 import Decimal128
from dotenv import load_dotenv
from pymongo.read_preferences import ReadPreference
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")
BASE_DE_DATOS = "urbanrush_db"
COLECCIONES_DW = ["dim_tiempo", "dim_cliente", "dim_producto", "fact_ventas"]
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "alex-512401")
BQ_DATASET = os.getenv("BQ_DATASET", "urbanrush_dw")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CREDS_PATH = os.getenv(
    "GOOGLE_APPLICATION_CREDENTIALS",
    os.path.join(RAIZ, "alex-512401-e673258a555d.json"),
)
if not os.path.isabs(CREDS_PATH):
    CREDS_PATH = os.path.join(RAIZ, CREDS_PATH)


def normalizar_valor(valor):
    if valor is None:
        return None
    if isinstance(valor, ObjectId):
        return str(valor)
    if isinstance(valor, Decimal128):
        return float(valor.to_decimal())
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, (datetime, date)):
        return valor
    if isinstance(valor, dict):
        return {k: normalizar_valor(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [normalizar_valor(v) for v in valor]
    return valor


def extraer_coleccion(db, nombre):
    filas = []
    for doc in db[nombre].find({}):
        plano = {k: normalizar_valor(v) for k, v in doc.items()}
        if "_id" in plano:
            plano["mongo_id"] = str(plano.pop("_id"))
        filas.append(plano)
    df = pd.DataFrame(filas)
    if df.empty:
        return df
    return df.convert_dtypes()


def cliente_bigquery():
    if not os.path.isfile(CREDS_PATH):
        raise FileNotFoundError(
            f"No se encontro el JSON de cuenta de servicio: {CREDS_PATH}"
        )
    credenciales = service_account.Credentials.from_service_account_file(CREDS_PATH)
    return bigquery.Client(project=GCP_PROJECT_ID, credentials=credenciales)


def asegurar_dataset(bq):
    dataset_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = BQ_LOCATION
    dataset.description = (
        "Data Warehouse UrbanRush. Ingesta OLTP en MongoDB Atlas; "
        "analisis OLAP en BigQuery."
    )
    return bq.create_dataset(dataset, exists_ok=True)


def cargar_tabla(bq, nombre, df):
    tabla_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{nombre}"
    if df.empty:
        print(f"  [AVISO] {nombre}: origen vacio, se omite la carga")
        return 0

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    job = bq.load_table_from_dataframe(df, tabla_id, job_config=job_config)
    job.result()
    tabla = bq.get_table(tabla_id)
    print(f"  {nombre}: {len(df)} filas origen -> {tabla.num_rows} filas en BigQuery")
    return tabla.num_rows


def consulta_prueba(bq):
    sql = f"""
    SELECT
      f.ciudad_entrega,
      COUNT(*) AS lineas,
      ROUND(SUM(f.subtotal_linea), 2) AS ingresos
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.fact_ventas` AS f
    GROUP BY f.ciudad_entrega
    ORDER BY ingresos DESC
    LIMIT 10
    """
    print("=" * 60)
    print("CONSULTA OLAP DE PRUEBA (ingresos por ciudad)")
    print("=" * 60)
    filas = list(bq.query(sql).result())
    if not filas:
        print("  Sin resultados (tabla vacia)")
        return
    for fila in filas:
        print(f"  {fila.ciudad_entrega:<20} {fila.lineas:>8} lineas  {fila.ingresos:>14,.2f}")


def ejecutar_etl():
    if not MONGO_URI:
        raise RuntimeError("Falta MONGO_URI o MONGODB_URI en el archivo .env")

    mongo = pymongo.MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=60000,
        connectTimeoutMS=30000,
        socketTimeoutMS=60000,
        retryReads=True,
        read_preference=ReadPreference.SECONDARY_PREFERRED,
    )
    db = mongo[BASE_DE_DATOS]
    db.command("ping", read_preference=ReadPreference.SECONDARY_PREFERRED)
    print(f"[OK] Conectado a MongoDB Atlas (base: {BASE_DE_DATOS})")

    bq = cliente_bigquery()
    asegurar_dataset(bq)
    print(f"[OK] Dataset BigQuery: {GCP_PROJECT_ID}.{BQ_DATASET} ({BQ_LOCATION})")

    print("=" * 60)
    print("ETL MongoDB Atlas -> BigQuery")
    print("=" * 60)
    for nombre in COLECCIONES_DW:
        df = extraer_coleccion(db, nombre)
        cargar_tabla(bq, nombre, df)

    consulta_prueba(bq)
    mongo.close()
    print("=" * 60)
    print("DATA WAREHOUSE EN BIGQUERY LISTO")
    print("=" * 60)


if __name__ == "__main__":
    ejecutar_etl()
