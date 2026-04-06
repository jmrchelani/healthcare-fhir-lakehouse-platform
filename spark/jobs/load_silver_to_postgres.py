from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("load_silver_to_postgres")
        .getOrCreate()
    )


def configure_s3(spark: SparkSession) -> None:
    hadoop_conf = spark._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
    hadoop_conf.set("fs.s3a.access.key", "minioadmin")
    hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


def main() -> None:
    spark = get_spark()
    configure_s3(spark)

    bucket = "healthcare-lakehouse"

    jdbc_url = "jdbc:postgresql://postgres:5432/healthcare_dw"
    jdbc_properties = {
        "user": "healthcare",
        "password": "healthcare",
        "driver": "org.postgresql.Driver",
    }

    patients = spark.read.parquet(f"s3a://{bucket}/silver/patients/")
    encounters = spark.read.parquet(f"s3a://{bucket}/silver/encounters/")
    observations = spark.read.parquet(f"s3a://{bucket}/silver/observations/")

    patients.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "public.silver_patients", properties=jdbc_properties)

    encounters.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "public.silver_encounters", properties=jdbc_properties)

    observations.write \
        .mode("overwrite") \
        .option("truncate", "true") \
        .jdbc(jdbc_url, "public.silver_observations", properties=jdbc_properties)

    print("Silver tables loaded into Postgres successfully.")
    spark.stop()


if __name__ == "__main__":
    main()