from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bronze_to_silver_patients",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "spark"],
) as dag:

    run_spark_job = BashOperator(
    task_id="run_spark_patients",
    bash_command="""
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /opt/airflow/spark/jobs/bronze_to_silver_patients.py
    """,
)