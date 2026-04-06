from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, trim

# Initialize Spark
spark = SparkSession.builder \
    .appName("bronze_to_silver_patients") \
    .getOrCreate()

# MinIO config
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

bucket = "healthcare-lakehouse"

# Read Bronze
df = spark.read.csv(
    f"s3a://{bucket}/bronze/patients.csv",
    header=True,
    inferSchema=True
)

# Transform → Silver
df_clean = (
    df
    .withColumn("given_name", trim(col("given_name")))
    .withColumn("family_name", trim(col("family_name")))
    .withColumn("birth_date", to_date(col("birth_date")))
    .dropDuplicates(["patient_id"])
)

# Write Silver
df_clean.write.mode("overwrite").parquet(
    f"s3a://{bucket}/silver/patients/"
)

print("✅ Patients Bronze → Silver complete")