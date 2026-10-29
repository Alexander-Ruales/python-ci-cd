"""Pruebas rapidas (sin MongoDB ni AWS) que corre el pipeline CI."""
import importlib
import os
import sys

import pandas as pd
from bson import ObjectId

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))


def cargar(bucket=""):
    os.environ["DATA_LAKE_BUCKET"] = bucket
    import data_lake
    return importlib.reload(data_lake)


def test_objectid_a_string_recursivo():
    dl = cargar()
    oid = ObjectId()
    r = dl.convertir_objectids({"a": oid, "b": [oid, {"c": oid}]})
    assert r == {"a": str(oid), "b": [str(oid), {"c": str(oid)}]}


def test_rutas_locales_por_defecto():
    dl = cargar("")
    assert not dl.USA_S3
    assert dl.ruta_lake("raw", "x.parquet") == "data_lake/raw/x.parquet"


def test_rutas_s3_cuando_hay_bucket():
    dl = cargar("mi-bucket")
    assert dl.USA_S3
    assert dl.ruta_lake("curated", "x.parquet") == "s3://mi-bucket/curated/x.parquet"
    cargar("")


def test_parquet_roundtrip(tmp_path):
    df = pd.DataFrame({"id": [1, 2], "total": [10.5, 20.0]})
    ruta = tmp_path / "t.parquet"
    df.to_parquet(ruta)
    assert pd.read_parquet(ruta).equals(df)
