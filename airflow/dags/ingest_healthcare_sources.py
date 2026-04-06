from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, text
from minio import Minio


BASE_DIR = Path("/opt/airflow")
DATA_DIR = BASE_DIR / "data" / "generated"


def upload_files_to_minio() -> None:
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
        secure=False,
    )
    bucket = os.getenv("MINIO_BUCKET", "healthcare-lakehouse")

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    for file_path in DATA_DIR.glob("*"):
        if file_path.is_file():
            object_name = f"bronze/{file_path.name}"
            client.fput_object(bucket, object_name, str(file_path))


def register_files_in_postgres() -> None:
    db_url = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'healthcare')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'healthcare')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'healthcare_dw')}"
    )
    engine = create_engine(db_url)

    with engine.begin() as conn:
        for file_path in DATA_DIR.glob("*"):
            if file_path.is_file():
                file_format = file_path.suffix.replace(".", "")
                conn.execute(
                    text("""
                        INSERT INTO bronze.raw_file_registry
                        (source_name, object_path, file_format)
                        VALUES (:source_name, :object_path, :file_format)
                    """),
                    {
                        "source_name": "synthetic_generator",
                        "object_path": f"bronze/{file_path.name}",
                        "file_format": file_format,
                    },
                )

                if file_format == "csv":
                    row_count = len(pd.read_csv(file_path))
                else:
                    row_count = 1

                conn.execute(
                    text("""
                        INSERT INTO audit.ingestion_log
                        (source_name, file_name, row_count, status)
                        VALUES (:source_name, :file_name, :row_count, :status)
                    """),
                    {
                        "source_name": "synthetic_generator",
                        "file_name": file_path.name,
                        "row_count": row_count,
                        "status": "INGESTED",
                    },
                )


with DAG(
    dag_id="ingest_healthcare_sources",
    start_date=datetime(2026, 4, 1),
    schedule="@daily",
    catchup=False,
    tags=["healthcare", "bronze", "ingestion"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_files_to_minio",
        python_callable=upload_files_to_minio,
    )

    register_task = PythonOperator(
        task_id="register_files_in_postgres",
        python_callable=register_files_in_postgres,
    )

    upload_task >> register_task