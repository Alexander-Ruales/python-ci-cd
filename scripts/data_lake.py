import os
import pymongo
import pandas as pd
from bson import ObjectId
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI") or os.getenv("MONGODB_URI")

# -------------------------------------------------------------------
# BLOQUE 2. Definir el Data Lake (zona principal + 3 zonas)
# -------------------------------------------------------------------
# Si DATA_LAKE_BUCKET esta definido en el .env, el lake vive en AWS S3;
# si no, se usa la carpeta local "data_lake" (comportamiento anterior).
S3_BUCKET = os.getenv("DATA_LAKE_BUCKET", "").strip()
USA_S3 = bool(S3_BUCKET)
DATA_LAKE_ROOT = f"s3://{S3_BUCKET}" if USA_S3 else "data_lake"
ZONAS = {
    zona: f"{DATA_LAKE_ROOT}/{zona}"
    for zona in ("raw", "processed", "curated")
}


def ruta_lake(zona, nombre_archivo):
    """Ruta final del archivo (funciona igual para disco local y s3://)."""
    return f"{ZONAS[zona]}/{nombre_archivo}"


# -------------------------------------------------------------------
# BLOQUE 3. Crear la estructura del Data Lake
# -------------------------------------------------------------------
def crear_estructura_lake():
    for zona, ruta in ZONAS.items():
        if not USA_S3:  # en S3 los prefijos se crean al subir el primer objeto
            os.makedirs(ruta, exist_ok=True)
        print(f"Zona '{zona}' lista en {ruta}")


def convertir_objectids(valor):
    """Convierte ObjectId a string recursivamente (Parquet no lo entiende)"""
    if isinstance(valor, ObjectId):
        return str(valor)
    if isinstance(valor, list):
        return [convertir_objectids(v) for v in valor]
    if isinstance(valor, dict):
        return {k: convertir_objectids(v) for k, v in valor.items()}
    return valor


# -------------------------------------------------------------------
# BLOQUE 4 y 5. Exportar una coleccion a Parquet
# -------------------------------------------------------------------
def exportar_a_parquet(coleccion, nombre_archivo, zona):
    print(f"Exportando '{coleccion}' -> {nombre_archivo} ({zona})...")

    client = pymongo.MongoClient(MONGO_URI)
    db = client["urbanrush_db"]

    # Traer todos los documentos de la coleccion
    documentos = list(db[coleccion].find({}))

    # JSON -> DataFrame
    df = pd.json_normalize(documentos)

    # Parquet no entiende ObjectId: los convertimos a string
    df = df.map(convertir_objectids)

    ruta = ruta_lake(zona, nombre_archivo)
    df.to_parquet(ruta, compression="snappy",
                  coerce_timestamps="us", allow_truncated_timestamps=True)

    print(f"   OK -> {len(df)} registros guardados en {ruta}")

    client.close()
    return df


# -------------------------------------------------------------------
# BLOQUE 6. Construccion completa del Data Lake (ETL)
# -------------------------------------------------------------------
def generar_data_lake_completo():
    print("=" * 60)
    print("GENERANDO DATA LAKE (Fase 2 - Big Data)")
    print("=" * 60)

    # 1. Crear carpetas
    crear_estructura_lake()
    print()

    # 2. ETAPA RAW: copia exacta de MongoDB (sin modificar)
    print(">> ETAPA RAW (copia exacta de la base de datos)\n")
    exportar_a_parquet("clientes", "clientes.parquet", "raw")
    exportar_a_parquet("productos", "productos.parquet", "raw")
    exportar_a_parquet("pedidos", "pedidos.parquet", "raw")
    print()

    # 3. ETAPA PROCESSED: transformaciones (filtro + columnas nuevas)
    print(">> ETAPA PROCESSED (datos limpios y transformados)\n")
    client = pymongo.MongoClient(MONGO_URI)
    db = client["urbanrush_db"]

    # Filtro: solo pedidos entregados (equivalente a 'Completada')
    documentos = list(db["pedidos"].find({"estado": "Entregado"}))
    df = pd.json_normalize(documentos)
    df = df.map(convertir_objectids)
    df["fecha_pedido"] = pd.to_datetime(df["fecha_pedido"])

    # Agregar dimensiones de tiempo (simula trabajo de Spark)
    df["anio"] = df["fecha_pedido"].dt.year
    df["mes"] = df["fecha_pedido"].dt.month
    df["fecha_procesamiento"] = datetime.now()

    df.to_parquet(ruta_lake("processed", "pedidos_procesados.parquet"),
                  compression="snappy",
                  coerce_timestamps="us", allow_truncated_timestamps=True)
    print(f"   OK -> {len(df)} pedidos entregados procesados "
          f"con anio/mes/fecha_procesamiento\n")
    client.close()

    # 4. ETAPA CURATED: modelo estrella listo para BI
    print(">> ETAPA CURATED (modelo analitico listo para BI)\n")
    exportar_a_parquet("pedidos_analiticos", "pedidos_analiticos.parquet", "curated")
    print()

    # 5. BLOQUE 7. Resumen del Data Lake
    resumen_del_lake()


# -------------------------------------------------------------------
# BLOQUE 7. Resumen del Data Lake (archivos, tamanos, espacio)
# -------------------------------------------------------------------
def resumen_del_lake():
    print("=" * 60)
    print("RESUMEN DEL DATA LAKE")
    print("=" * 60)

    total_archivos = 0
    total_bytes = 0

    if USA_S3:
        import s3fs
        fs = s3fs.S3FileSystem()

    for zona, ruta in ZONAS.items():
        if USA_S3:
            objetos = [o for o in fs.ls(ruta.replace("s3://", ""), detail=True)
                       if o["type"] == "file" and o["size"] > 0]
            n_archivos = len(objetos)
            tamano_bytes = sum(o["size"] for o in objetos)
        else:
            archivos = [f for f in os.listdir(ruta) if os.path.isfile(os.path.join(ruta, f))]
            n_archivos = len(archivos)
            tamano_bytes = sum(os.path.getsize(os.path.join(ruta, f)) for f in archivos)

        total_archivos += n_archivos
        total_bytes += tamano_bytes

        print(f"{zona.upper()}: {n_archivos} archivo(s) - {tamano_bytes/1024:.2f} KB")

    print(f"\nTOTAL: {total_archivos} archivos - {total_bytes/1024:.2f} KB "
          f"({total_bytes/1024/1024:.2f} MB)")
    print("=" * 60)


# -------------------------------------------------------------------
# PUNTO DE ENTRADA
# -------------------------------------------------------------------
if __name__ == "__main__":
    generar_data_lake_completo()