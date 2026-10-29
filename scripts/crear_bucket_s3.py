"""Crea el bucket S3 del Data Lake (con las zonas raw/processed/curated).

Uso: python scripts/crear_bucket_s3.py
Requiere en .env: DATA_LAKE_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
AWS_DEFAULT_REGION.
"""
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()
BUCKET = os.getenv("DATA_LAKE_BUCKET", "").strip()
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")


def crear_bucket(s3=None):
    s3 = s3 or boto3.client("s3", region_name=REGION)
    try:
        if REGION == "us-east-1":
            s3.create_bucket(Bucket=BUCKET)
        else:
            s3.create_bucket(Bucket=BUCKET,
                             CreateBucketConfiguration={"LocationConstraint": REGION})
        print(f"Bucket '{BUCKET}' creado en {REGION}")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou",):
            print(f"Bucket '{BUCKET}' ya existe (tuyo), se reutiliza")
        else:
            raise
    # Bloquear acceso publico: un data lake nunca debe ser publico
    s3.put_public_access_block(
        Bucket=BUCKET,
        PublicAccessBlockConfiguration={
            "BlockPublicAcls": True, "IgnorePublicAcls": True,
            "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    for zona in ("raw", "processed", "curated"):
        s3.put_object(Bucket=BUCKET, Key=f"{zona}/")
    print("Zonas raw/, processed/, curated/ listas")


if __name__ == "__main__":
    if not BUCKET:
        raise SystemExit("Define DATA_LAKE_BUCKET en el .env")
    crear_bucket()
