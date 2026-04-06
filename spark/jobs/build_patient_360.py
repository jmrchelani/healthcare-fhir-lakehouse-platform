from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("build_patient_360")
        .getOrCreate()
    )
    return spark


def configure_s3(spark: SparkSession) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def main() -> None:
    bucket = "healthcare-lakehouse"
    spark = get_spark()
    configure_s3(spark)

    patients = spark.read.parquet(f"s3a://{bucket}/silver/patients/")
    encounters = spark.read.parquet(f"s3a://{bucket}/silver/encounters/")
    observations = spark.read.parquet(f"s3a://{bucket}/silver/observations/")

    enc_summary = encounters.groupBy("patient_id").agg(
        count("*").alias("total_encounters")
    )

    obs_summary = observations.groupBy("patient_id").agg(
        count("*").alias("total_observations"),
        avg(col("observation_value")).alias("avg_observation_value")
    )

    patient_360 = (
        patients
        .join(enc_summary, on="patient_id", how="left")
        .join(obs_summary, on="patient_id", how="left")
        .fillna({
            "total_encounters": 0,
            "total_observations": 0,
        })
    )

    patient_360.write.mode("overwrite").parquet(
        f"s3a://{bucket}/gold/patient_360/"
    )

    print("Gold patient_360 built successfully.")
    spark.stop()


if __name__ == "__main__":
    main()