import snowflake.connector
import pandas as pd
import logging
import glob
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s — %(levelname)s — %(message)s'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# CONNECTION
# ─────────────────────────────────────────
conn = snowflake.connector.connect(
    account   = "eulwggz-fvb65417",
    user      = "Kartik9495",
    password  = os.environ.get("SNOWFLAKE_PASSWORD"),
    warehouse = "NYC_TAXI_WH",
    database  = "NYC_TAXI_DB",
    schema    = "NYC_TAXI_SCHEMA"
)
cursor = conn.cursor()
logger.info("Connected to Snowflake successfully")

# ─────────────────────────────────────────
# DROP AND RECREATE TABLE
# ─────────────────────────────────────────
cursor.execute("DROP TABLE IF EXISTS NYC_TAXI_TRIPS")
cursor.execute("""
    CREATE TABLE NYC_TAXI_TRIPS (
        VENDORID              INTEGER,
        TPEP_PICKUP_DATETIME  TIMESTAMP_NTZ,
        TPEP_DROPOFF_DATETIME TIMESTAMP_NTZ,
        PASSENGER_COUNT       FLOAT,
        TRIP_DISTANCE         FLOAT,
        RATECODEID            FLOAT,
        STORE_AND_FWD_FLAG    VARCHAR(5),
        PULOCATIONID          INTEGER,
        DOLOCATIONID          INTEGER,
        PAYMENT_TYPE          FLOAT,
        FARE_AMOUNT           FLOAT,
        EXTRA                 FLOAT,
        MTA_TAX               FLOAT,
        TIP_AMOUNT            FLOAT,
        TOLLS_AMOUNT          FLOAT,
        IMPROVEMENT_SURCHARGE FLOAT,
        TOTAL_AMOUNT          FLOAT,
        CONGESTION_SURCHARGE  FLOAT,
        AIRPORT_FEE           FLOAT,
        TRIP_DURATION_MINUTES FLOAT,
        FARE_PER_MILE         FLOAT,
        PICKUP_HOUR           INTEGER,
        PICKUP_DATE           DATE,
        TIME_OF_DAY           VARCHAR(20)
    )
""")
logger.info("Table created successfully")

# ─────────────────────────────────────────
# LOAD PARQUET
# ─────────────────────────────────────────
# Read each partition separately — 12,500 from each
df_morning   = pd.read_parquet("data/processed/nyc_taxi_clean/time_of_day=Morning").head(12500)
df_afternoon = pd.read_parquet("data/processed/nyc_taxi_clean/time_of_day=Afternoon").head(12500)
df_evening   = pd.read_parquet("data/processed/nyc_taxi_clean/time_of_day=Evening").head(12500)
df_night     = pd.read_parquet("data/processed/nyc_taxi_clean/time_of_day=Night").head(12500)

# Add time_of_day column to each partition
df_morning['time_of_day']   = 'Morning'
df_afternoon['time_of_day'] = 'Afternoon'
df_evening['time_of_day']   = 'Evening'
df_night['time_of_day']     = 'Night'

# Combine all partitions
df = pd.concat([df_morning, df_afternoon, df_evening, df_night], ignore_index=True)
logger.info(f"Loaded {len(df):,} rows from all 4 partitions")
df.columns = [col.upper() for col in df.columns]

# Convert timestamps to strings for safe insertion
df['TPEP_PICKUP_DATETIME'] = pd.to_datetime(
    df['TPEP_PICKUP_DATETIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['TPEP_DROPOFF_DATETIME'] = pd.to_datetime(
    df['TPEP_DROPOFF_DATETIME']).dt.strftime('%Y-%m-%d %H:%M:%S')
df['PICKUP_DATE'] = pd.to_datetime(
    df['PICKUP_DATE']).dt.strftime('%Y-%m-%d')

logger.info(f"Loaded {len(df):,} rows from parquet")

# ─────────────────────────────────────────
# INSERT IN BATCHES
# ─────────────────────────────────────────
logger.info("Inserting data in batches...")

batch_size = 5000
total_inserted = 0

for i in range(0, len(df), batch_size):
    batch = df.iloc[i:i+batch_size]
    
    # Build values list
    values = []
    for _, row in batch.iterrows():
        values.append((
            None if pd.isna(row['VENDORID']) else int(row['VENDORID']),
            str(row['TPEP_PICKUP_DATETIME']),
            str(row['TPEP_DROPOFF_DATETIME']),
            None if pd.isna(row['PASSENGER_COUNT']) else float(row['PASSENGER_COUNT']),
            float(row['TRIP_DISTANCE']),
            None if pd.isna(row['RATECODEID']) else float(row['RATECODEID']),
            str(row['STORE_AND_FWD_FLAG']) if row['STORE_AND_FWD_FLAG'] else None,
            int(row['PULOCATIONID']),
            int(row['DOLOCATIONID']),
            float(row['PAYMENT_TYPE']),
            float(row['FARE_AMOUNT']),
            float(row['EXTRA']),
            float(row['MTA_TAX']),
            float(row['TIP_AMOUNT']),
            float(row['TOLLS_AMOUNT']),
            float(row['IMPROVEMENT_SURCHARGE']),
            float(row['TOTAL_AMOUNT']),
            None if pd.isna(row['CONGESTION_SURCHARGE']) else float(row['CONGESTION_SURCHARGE']),
            None if pd.isna(row['AIRPORT_FEE']) else float(row['AIRPORT_FEE']),
            float(row['TRIP_DURATION_MINUTES']),
            float(row['FARE_PER_MILE']),
            int(row['PICKUP_HOUR']),
            str(row['PICKUP_DATE']),
            str(row['TIME_OF_DAY'])
        ))
    
    cursor.executemany("""
        INSERT INTO NYC_TAXI_TRIPS VALUES (
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
        )
    """, values)
    
    total_inserted += len(batch)
    logger.info(f"Inserted batch: {total_inserted:,}/{len(df):,} rows")

# ─────────────────────────────────────────
# VERIFY
# ─────────────────────────────────────────
cursor.execute("SELECT COUNT(*) FROM NYC_TAXI_TRIPS")
count = cursor.fetchone()[0]

cursor.execute("""
    SELECT
        TIME_OF_DAY,
        COUNT(*) as TRIPS,
        ROUND(AVG(FARE_AMOUNT), 2) as AVG_FARE,
        ROUND(SUM(FARE_AMOUNT), 2) as TOTAL_REVENUE
    FROM NYC_TAXI_TRIPS
    GROUP BY TIME_OF_DAY
    ORDER BY TOTAL_REVENUE DESC
""")
results = cursor.fetchall()

print("\n" + "="*55)
print("SNOWFLAKE VERIFICATION")
print("="*55)
print(f"Total rows in Snowflake: {count:,}")
print(f"\n{'Time':<12} {'Trips':>8} {'Avg Fare':>10} {'Revenue':>15}")
print("-"*50)
for row in results:
    print(f"{row[0]:<12} {row[1]:>8,} {row[2]:>10.2f} {row[3]:>15.2f}")
print("="*55)
print("NYC TAXI DATA LOADED INTO SNOWFLAKE")
print("="*55)

cursor.close()
conn.close()
logger.info("Connection closed")