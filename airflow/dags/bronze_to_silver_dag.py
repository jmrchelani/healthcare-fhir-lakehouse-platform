from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


SPARK_SUBMIT = r"""
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  {job_path}
"""


with DAG(
    dag_id="bronze_to_silver_and_gold",
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    tags=["healthcare", "spark", "silver", "gold"],
) as dag:

    run_patients = BashOperator(
        task_id="run_patients_silver",
        bash_command=SPARK_SUBMIT.format(
            job_path="/opt/airflow/spark/jobs/bronze_to_silver_patients.py"
        ),
    )

    run_encounters = BashOperator(
        task_id="run_encounters_silver",
        bash_command=SPARK_SUBMIT.format(
            job_path="/opt/airflow/spark/jobs/bronze_to_silver_encounters.py"
        ),
    )

    run_observations = BashOperator(
        task_id="run_observations_silver",
        bash_command=SPARK_SUBMIT.format(
            job_path="/opt/airflow/spark/jobs/bronze_to_silver_observations.py"
        ),
    )

    build_patient_360 = BashOperator(
        task_id="build_patient_360_gold",
        bash_command=SPARK_SUBMIT.format(
            job_path="/opt/airflow/spark/jobs/build_patient_360.py"
        ),
    )

    load_to_postgres = BashOperator(
    task_id="load_silver_to_postgres",
    bash_command="""
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --packages org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=minioadmin \
            --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            /opt/airflow/spark/jobs/load_silver_to_postgres.py
        """,
    )
    
    [run_patients, run_encounters, run_observations] >> load_to_postgres >> build_patient_360