from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim


def get_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("bronze_to_silver_observations")
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
        f"s3a://{bucket}/bronze/observations.csv",
        header=True,
        inferSchema=True,
    )

    df_clean = (
        df
        .withColumn("observation_id", trim(col("observation_id")))
        .withColumn("patient_id", trim(col("patient_id")))
        .withColumn("encounter_id", trim(col("encounter_id")))
        .withColumn("observation_code", trim(col("observation_code")))
        .withColumn("unit", trim(col("unit")))
        .withColumn("effective_time", to_timestamp(col("effective_time")))
        .dropDuplicates(["observation_id"])
        .filter(col("observation_id").isNotNull())
        .filter(col("patient_id").isNotNull())
    )

    df_clean.write.mode("overwrite").parquet(
        f"s3a://{bucket}/silver/observations/"
    )

    print("Observations Bronze -> Silver completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()