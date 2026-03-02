import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# ─────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
    
)
logger = logging.getLogger(__name__)
   
# ─────────────────────────────────────────
# STEP 1 — SPARK SESSION
# ─────────────────────────────────────────
logger.info("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("NYC_Taxi_Pipeline_Day12") \
    .master("local[6]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
     
     
spark.sparkContext.setLogLevel("WARN")
logger.info("Spark Session initialized successfully.")  


# ─────────────────────────────────────────
# STEP 2 — LOAD RAW DATA
# ─────────────────────────────────────────
RAW_DATA_PATH = "data/raw/yellow_tripdata_2024-01.parquet"
PROCESSED_DATA_PATH = "data/processed/nyc_taxi_clean"

logger.info(f"Loading raw data from {RAW_DATA_PATH}...")
start_time = time.time()

df_raw = spark.read.parquet(RAW_DATA_PATH)
raw_count = df_raw.count()

logger.info(f"Raw data loaded successfully with {raw_count} records in {time.time() - start_time:.2f} seconds.")
logger.info(f"  columns: {len(df_raw.columns)}")


# ─────────────────────────────────────────
# STEP 3 — DATA QUALITY FILTER
# ─────────────────────────────────────────

logger.info("Applying data quality filters...")

df_clean = df_raw.filter(
    (F.col("passenger_count") <= 6)&
    (F.col("passenger_count") >= 1)&
    (F.col("trip_distance") > 0)&
    (F.col("fare_amount") > 0)&
    (F.col("fare_amount") <= 500)&
    (F.col("tpep_pickup_datetime").isNotNull()) &
    (F.col("payment_type").isin([1, 2, 3, 4, 5, 6]))
)
        
clean_count = df_clean.count()
removed_count = raw_count - clean_count
removal_percentage = (removed_count / raw_count) * 100

logger.info(f"✅ Data quality filter complete!")
logger.info(f"   Raw rows     : {raw_count:,}")
logger.info(f"   Clean rows   : {clean_count:,}")
logger.info(f"   Removed rows : {removed_count:,} ({removal_percentage:.2f}%)")


# ─────────────────────────────────────────
# STEP 4 — TRANSFORMATIONS
# ─────────────────────────────────────────

df_transformed = df_clean \
    .withColumn(
        "trip_duration_minutes",
        F.round(
            (F.unix_timestamp("tpep_dropoff_datetime") -
             F.unix_timestamp("tpep_pickup_datetime")) / 60, 2
        )
    ) \
    .withColumn(
        "fare_per_mile",
        F.round(F.col("fare_amount") / F.col("trip_distance"), 2)
    ) \
    .withColumn(
        "pickup_hour",
        F.hour("tpep_pickup_datetime")
    ) \
    .withColumn(
        "time_of_day",
        F.when(F.col("pickup_hour").between(6, 11), "Morning")
         .when(F.col("pickup_hour").between(12, 16), "Afternoon")
         .when(F.col("pickup_hour").between(17, 21), "Evening")
         .otherwise("Night")
    ) \
    .withColumn(
        "pickup_date",
        F.to_date("tpep_pickup_datetime")
    )

logger.info("✅ Transformations applied:")
logger.info("   + trip_duration_minutes")
logger.info("   + fare_per_mile")
logger.info("   + pickup_hour")
logger.info("   + time_of_day (Morning/Afternoon/Evening/Night)")
logger.info("   + pickup_date")

# ─────────────────────────────────────────
# STEP 5 — SPARK SQL ANALYTICS
# ─────────────────────────────────────────

logger.info("Running Spark SQL analytics...")
df_transformed.createOrReplaceTempView("nyc_taxi")

#Query 1: revenue by time of day
print("\n" + "="*55)
print("ANALYTICS 1: Total Revenue by Time of Day")
print("="*55)

spark.sql("""
    SELECT 
        time_of_day,
        COUNT(*) as total_trips,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(SUM(fare_amount), 2) as total_revenue,
        ROUND(AVG(trip_distance), 2) as avg_distance
    FROM nyc_taxi
    GROUP BY time_of_day
    ORDER BY total_revenue DESC
""").show()

# Query 2 — Top payment methods
print("="*55)
print("ANALYTICS 2 — Payment Method Breakdown")
print("="*55)
spark.sql("""
    SELECT 
        payment_type,
        COUNT(*) as trip_count,
        ROUND(AVG(fare_amount), 2) as avg_fare,
        ROUND(AVG(tip_amount), 2) as avg_tip
    FROM nyc_taxi
    GROUP BY payment_type
    ORDER BY trip_count DESC
""").show()

# Query 3 — Daily trip volume trend
print("="*55)
print("ANALYTICS 3 — Daily Trip Volume (Top 10 Days)")
print("="*55)
spark.sql("""
    SELECT 
        pickup_date,
        COUNT(*) as daily_trips,
        ROUND(SUM(fare_amount), 2) as daily_revenue,
        ROUND(AVG(trip_duration_minutes), 2) as avg_duration
    FROM nyc_taxi
    GROUP BY pickup_date
    ORDER BY daily_trips DESC
    LIMIT 10
""").show()


# Query 4 — Fare per mile by time of day (Window Function)
print("="*55)
print("ANALYTICS 4 — Fare Efficiency by Time of Day")
print("="*55)
spark.sql("""
    SELECT DISTINCT
        time_of_day,
        ROUND(AVG(fare_per_mile) OVER (PARTITION BY time_of_day), 2) as avg_fare_per_mile,
        ROUND(AVG(trip_duration_minutes) OVER (PARTITION BY time_of_day), 2) as avg_duration
    FROM nyc_taxi
    WHERE fare_per_mile IS NOT NULL 
    AND fare_per_mile < 100
    ORDER BY avg_fare_per_mile DESC
""").show()

logger.info("✅ All 4 analytics queries complete!")

# ─────────────────────────────────────────
# STEP 6 — WRITE PROCESSED DATA
# ─────────────────────────────────────────

logger.info(f"Writing clean data to: {PROCESSED_DATA_PATH}")

df_transformed.write \
    .mode("overwrite") \
    .partitionBy("time_of_day") \
    .parquet(PROCESSED_DATA_PATH)

logger.info("✅ Clean data written to processed folder!")
logger.info("   Format: Parquet with Snappy compression")
logger.info("   Partitioned by: time_of_day")


# ─────────────────────────────────────────
# STEP 7 — PIPELINE SUMMARY
# ─────────────────────────────────────────
end_time = time.time()
duration = round(end_time - start_time, 2)

print("\n" + "="*55)
print("PIPELINE SUMMARY — DAY 12")
print("="*55)
print(f"Raw Rows Loaded    : {raw_count:,}")
print(f"Clean Rows Output  : {clean_count:,}")
print(f"Dirty Rows Removed : {removed_count:,} ({removal_percentage:.2f}%)")
print(f"New Columns Added  : 5")
print(f"Analytics Queries  : 4")
print(f"Output Format      : Parquet (partitioned)")
print(f"Total Runtime      : {duration}s")
print("="*55)
print("✅ NYC TAXI PIPELINE — DAY 12 COMPLETE!")
print("="*55)

spark.stop()
