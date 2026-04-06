from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver_encounters")
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

    df = spark.read.csv(
        f"s3a://{bucket}/bronze/encounters.csv",
        header=True,
        inferSchema=True,
    )

    df_clean = (
        df
        .withColumn("encounter_id", trim(col("encounter_id")))
        .withColumn("patient_id", trim(col("patient_id")))
        .withColumn("provider_id", trim(col("provider_id")))
        .withColumn("encounter_type", trim(col("encounter_type")))
        .withColumn("hospital_unit", trim(col("hospital_unit")))
        .withColumn("admit_time", to_timestamp(col("admit_time")))
        .withColumn("discharge_time", to_timestamp(col("discharge_time")))
        .dropDuplicates(["encounter_id"])
        .filter(col("encounter_id").isNotNull())
        .filter(col("patient_id").isNotNull())
    )

    df_clean.write.mode("overwrite").parquet(
        f"s3a://{bucket}/silver/encounters/"
    )

    print("Encounters Bronze -> Silver completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()