from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, max, round

spark = SparkSession.builder \
    .appName("NYC_Taxi_Explorer") \
    .master("local[6]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("NYC YELLOW TAXI 2024 - DATA EXPLORATION")
print("=" * 60)

df = spark.read.parquet("data/raw/yellow_tripdata_2024-01.parquet")

print(f"\nTotal Rows: {df.count():,}")
print(f"Total Columns: {len(df.columns)}")

print("\nSchema:")
df.printSchema()

print("\nFirst 5 rows:")
df.show(5, truncate=False)

print("\nKey Statistics:")
df.select(
    round(avg("fare_amount"), 2).alias("avg_fare"),
    round(avg("trip_distance"), 2).alias("avg_distance"),
    round(avg("passenger_count"), 1).alias("avg_passengers"),
    round(avg("tip_amount"), 2).alias("avg_tip"),
    min("tpep_pickup_datetime").alias("earliest_trip"),
    max("tpep_pickup_datetime").alias("latest_trip")
).show(truncate=False)

print("\nNull Counts:")
for c in df.columns:
    null_count = df.filter(col(c).isNull()).count()
    if null_count > 0:
        print(f"  {c}: {null_count:,} nulls")

print("\nExploration Complete!")
spark.stop()
