import logging
import os
import boto3
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.pip_utils import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────
BUCKET_NAME     = "kartik-nyc-taxi-pipeline"
LOCAL_PROCESSED = "data/processed/nyc_taxi_clean"
DELTA_LOCAL     = "data/delta/nyc_taxi_delta"
SAMPLE_SIZE     = 100000

# ─────────────────────────────────────────
# SPARK SESSION WITH DELTA LAKE
# ─────────────────────────────────────────
logger.info("Starting Spark Session with Delta Lake 3.0.0...")

builder = SparkSession.builder \
    .appName("NYC_Taxi_DeltaLake_Day15") \
    .master("local[4]") \
    .config("spark.driver.memory", "3g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session with Delta Lake started successfully")

# ─────────────────────────────────────────
# STEP 1 — READ SAMPLE OF PROCESSED DATA
# ─────────────────────────────────────────
logger.info(f"Reading {SAMPLE_SIZE:,} rows from processed parquet...")

df = spark.read.parquet(LOCAL_PROCESSED) \
    .limit(SAMPLE_SIZE)

df.cache()
count = df.count()

logger.info(f"Sample loaded: {count:,} rows")
logger.info(f"Columns: {len(df.columns)}")

# ─────────────────────────────────────────
# STEP 2 — WRITE AS DELTA FORMAT LOCALLY
# ─────────────────────────────────────────
logger.info(f"Writing Delta table to: {DELTA_LOCAL}")

# Remove existing delta folder if exists
import shutil
if os.path.exists(DELTA_LOCAL):
    shutil.rmtree(DELTA_LOCAL)
    logger.info("Removed existing Delta folder")

df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("time_of_day") \
    .save(DELTA_LOCAL)

logger.info("Delta table written successfully")

# ─────────────────────────────────────────
# STEP 3 — VERIFY DELTA LOG CREATED
# ─────────────────────────────────────────
delta_log_path = f"{DELTA_LOCAL}/_delta_log"

print("\n" + "="*55)
print("DELTA LOG FILES CREATED")
print("="*55)
if os.path.exists(delta_log_path):
    log_files = os.listdir(delta_log_path)
    for f in sorted(log_files):
        size = os.path.getsize(f"{delta_log_path}/{f}")
        print(f"✅ {f} ({size} bytes)")
    print(f"\nTotal log files: {len(log_files)}")
else:
    print("Delta log not found")
print("="*55)

# ─────────────────────────────────────────
# STEP 4 — DELTA TABLE HISTORY
# ─────────────────────────────────────────
logger.info("Reading Delta table history...")

delta_table = DeltaTable.forPath(spark, DELTA_LOCAL)

print("\n" + "="*55)
print("DELTA TABLE HISTORY")
print("="*55)
delta_table.history().select(
    "version",
    "timestamp",
    "operation"
).show(truncate=False)

# ─────────────────────────────────────────
# STEP 5 — TIME TRAVEL DEMO
# ─────────────────────────────────────────
logger.info("Running Time Travel query on Version 0...")

df_v0 = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load(DELTA_LOCAL)

v0_count = df_v0.count()

print("="*55)
print("TIME TRAVEL — VERSION 0 (Initial Load)")
print("="*55)
print(f"Version 0 row count : {v0_count:,}")
print(f"Columns             : {len(df_v0.columns)}")
print(f"Time travel query   : SUCCESS")
print("="*55)

# ─────────────────────────────────────────
# STEP 6 — QUICK ANALYTICS ON DELTA TABLE
# ─────────────────────────────────────────
logger.info("Running analytics on Delta table...")

df_v0.createOrReplaceTempView("delta_nyc_taxi")

print("\n" + "="*55)
print("ANALYTICS ON DELTA TABLE")
print("="*55)
spark.sql("""
    SELECT
        time_of_day,
        COUNT(*) as trips,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(SUM(fare_amount), 2) as total_revenue
    FROM delta_nyc_taxi
    GROUP BY time_of_day
    ORDER BY total_revenue DESC
""").show()

# ─────────────────────────────────────────
# STEP 7 — UPLOAD DELTA FILES TO S3
# ─────────────────────────────────────────
logger.info("Uploading Delta table to S3...")

s3_client = boto3.client("s3", region_name="ap-south-1")
local_delta = Path(DELTA_LOCAL)
uploaded = 0
total_size = 0

for file_path in local_delta.rglob("*"):
    if file_path.is_file():
        relative_path = file_path.relative_to(local_delta)
        s3_key = f"delta/nyc_taxi_delta/{relative_path}"
        file_size = os.path.getsize(file_path)
        total_size += file_size
        s3_client.upload_file(
            str(file_path),
            BUCKET_NAME,
            s3_key
        )
        uploaded += 1
        logger.info(f"Uploaded ({uploaded}): {s3_key}")

size_mb = total_size / (1024 * 1024)

# ─────────────────────────────────────────
# FINAL SUMMARY
# ─────────────────────────────────────────
print("\n" + "="*55)
print("DELTA LAKE DAY 15 — COMPLETE SUMMARY")
print("="*55)
print(f"Sample rows used     : {count:,}")
print(f"Delta files created  : {uploaded}")
print(f"Total size uploaded  : {size_mb:.2f} MB")
print(f"S3 location          : s3://{BUCKET_NAME}/delta/")
print(f"Delta Lake version   : 3.0.0")
print(f"PySpark version      : 3.5.0")
print(f"Partitioned by       : time_of_day")
print(f"ACID transactions    : Enabled")
print(f"Time travel          : Enabled")
print(f"Audit log            : Enabled")
print("="*55)
print("Delta Lake on S3 — DAY 15 COMPLETE")
print("="*55)

spark.stop()