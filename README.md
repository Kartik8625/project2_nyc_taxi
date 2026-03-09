# 🚕 NYC Taxi Analytics Pipeline — Production-Grade Batch Processing

> End-to-end data engineering pipeline processing **2.96 million** NYC Yellow Taxi trips through validation, transformation, cloud storage, and a data warehouse — built with industry-standard tools.

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange)](https://spark.apache.org)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-green)](https://airflow.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-lightblue)](https://snowflake.com)
[![dbt](https://img.shields.io/badge/dbt-Cloud-red)](https://getdbt.com)
[![AWS S3](https://img.shields.io/badge/AWS-S3-yellow)](https://aws.amazon.com/s3)

---

## 📌 Project Overview

This project simulates a **real-world production data pipeline** used by data engineering teams at companies like Uber, Lyft, and NYC transit authorities. It ingests raw government taxi data, validates it, processes it with distributed computing, stores it in a cloud data lake, and serves it through a cloud warehouse with business-ready models.

**Dataset:** NYC Yellow Taxi Trip Records — January 2024  
**Source:** [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) — Official government data  
**Size:** 2,964,624 rows · 19 columns · 48MB Parquet

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PIPELINE ARCHITECTURE                         │
└─────────────────────────────────────────────────────────────────────┘

  NYC TLC Open Data (48MB Parquet)
           │
           ▼
  ┌─────────────────┐
  │ Great           │  ← 5 expectation rules
  │ Expectations    │  ← Found: 240,892 dirty rows (8.13%)
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ Apache Airflow  │  ← Orchestration (Docker)
  │ DAG Scheduler   │  ← Daily at 6:00 AM
  └────────┬────────┘
           │
           ▼
  ┌─────────────────┐
  │ PySpark 3.5.0   │  ← 2.72M clean rows
  │ Processing      │  ← 5 new columns added
  └────────┬────────┘
           │
     ┌─────┴─────┐
     ▼           ▼
┌─────────┐  ┌──────────────┐
│  AWS S3  │  │  Delta Lake  │  ← ACID + Time Travel
│ (62 MB)  │  │  on S3       │
└─────────┘  └──────┬───────┘
                     │
                     ▼
             ┌──────────────┐
             │  Snowflake   │  ← Cloud Data Warehouse
             │  NYC_TAXI_DB │  ← 50,000 rows loaded
             └──────┬───────┘
                    │
                    ▼
             ┌──────────────┐
             │     dbt      │  ← Business Models
             │  Cloud       │  ← Staging + Mart layers
             └──────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Tool | Version | Purpose |
|-------|------|---------|---------|
| Processing | PySpark | 3.5.0 | Distributed data transformation |
| Validation | Great Expectations | 0.18.19 | Data quality checks |
| Orchestration | Apache Airflow | 2.8.1 | Pipeline scheduling & monitoring |
| Containerization | Docker | Latest | Isolated Airflow environment |
| Data Lake | AWS S3 | — | Cloud object storage (Mumbai region) |
| ACID Storage | Delta Lake | 3.0.0 | Transactions + time travel on S3 |
| Warehouse | Snowflake | — | Cloud data warehouse |
| Transformation | dbt Cloud | Latest | Staging + business mart models |
| Language | Python | 3.12 | Pipeline code |
| Version Control | Git + GitHub | — | Source control |

---

## 📊 Pipeline Results

### Data Quality (Great Expectations)

| Expectation | Column | Issue | Rows Affected |
|-------------|--------|-------|---------------|
| ✅ PASSED | tpep_pickup_datetime | No nulls | 0 |
| ❌ FAILED | fare_amount | Negative fares | 37,494 |
| ❌ FAILED | passenger_count | Zero passengers | 31,525 |
| ❌ FAILED | trip_distance | Zero distance | 60,371 |
| ❌ FAILED | payment_type | Invalid type (0) | 140,162 |

> **Key insight:** All 140,162 invalid payment rows share the same null pattern across 5 columns — indicating payment system failures, not random data entry errors.

### PySpark Processing

| Metric | Value |
|--------|-------|
| Raw rows loaded | 2,964,624 |
| Clean rows output | 2,723,732 |
| Dirty rows removed | 240,892 (8.13%) |
| New columns added | 5 |
| Runtime | ~37 seconds |
| Output format | Parquet (partitioned by time_of_day) |

### Snowflake Analytics

| Time of Day | Trips | Avg Fare | Total Revenue |
|-------------|-------|----------|---------------|
| Morning | 12,500 | $24.69 | $308,571 |
| Evening | 12,500 | $22.98 | $287,208 |
| Afternoon | 12,500 | $21.26 | $265,785 |
| Night | 12,500 | $18.36 | $229,493 |

> **Business insight:** Morning trips earn 34% more revenue per trip than Night trips — likely due to airport runs during commute hours.

---

## 📁 Project Structure

```
project2_nyc_taxi/
├── src/
│   ├── explore_nyc_taxi.py        # Day 10: Initial data exploration
│   ├── setup_ge.py                # Day 11: GE datasource setup
│   ├── create_expectations.py     # Day 11: Expectation suite creation
│   ├── run_validation.py          # Day 11: GE validation runner
│   ├── pyspark_pipeline.py        # Day 12: PySpark ETL pipeline
│   ├── upload_to_s3.py            # Day 14: S3 upload script
│   ├── delta_lake_s3.py           # Day 15: Delta Lake on S3
│   └── snowflake_load.py          # Day 16: Snowflake loader
├── models/                        # dbt models
│   ├── schema.yml                 # Source definitions
│   ├── stg_nyc_taxi_trips.sql     # Staging model
│   └── mart_revenue_by_time.sql   # Business mart model
├── gx/
│   ├── checkpoints/
│   └── expectations/
│       └── nyc_taxi_quality_suite.json
├── data/
│   ├── raw/                       # Source parquet (48MB)
│   ├── processed/                 # Partitioned clean parquet
│   └── delta/                     # Delta Lake local copy
├── airflow/
│   └── dags/
│       └── nyc_taxi_dag.py        # Airflow DAG definition
├── config/
├── logs/
├── tests/
└── README.md
```

---

## ⚙️ How to Run

### Prerequisites
```bash
# Python 3.12, Java 17, Docker installed
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SNOWFLAKE_PASSWORD="your_password"
```

### Setup
```bash
git clone https://github.com/Kartik8625/project2_nyc_taxi.git
cd project2_nyc_taxi
python -m venv de_env
source de_env/bin/activate
pip install -r requirements.txt
```

### Run Pipeline Steps
```bash
# Step 1: Data validation
python src/run_validation.py

# Step 2: PySpark processing
python src/pyspark_pipeline.py

# Step 3: Upload to S3
python src/upload_to_s3.py

# Step 4: Delta Lake
python src/delta_lake_s3.py

# Step 5: Load to Snowflake
python src/snowflake_load.py
```

### Start Airflow (Docker)
```bash
cd ~/airflow
docker compose start
# UI: http://localhost:8080
```

---

## 🔑 Key Technical Lessons

1. **GE datasource** requires absolute paths (`os.path.abspath`) — relative paths silently fail
2. **PySpark + Delta Lake**: Use `local[6]`, `driver.memory=4g` on 8GB RAM machines
3. **Snowflake connector** requires `datetime64[ns]` not `datetime64[us]` — silent type mismatch
4. **`write_pandas()` has timestamp bugs** — use `executemany()` for reliable inserts
5. **Partitioned Parquet** — always read the folder, not individual files
6. **dbt**: use `source()` for raw tables, `ref()` for dbt models — never mix them
7. **Always define sources in `schema.yml`** before creating models
8. **Cash tips = $0 in data** — taxi meters don't capture cash, only card tips recorded
9. **git-filter-repo** to remove large files committed by mistake (repo went 456MB → 7KB)
10. **Docker isolation** means packages installed outside container are invisible inside it

---

## 🏆 Skills Demonstrated

- ✅ Distributed data processing (PySpark)
- ✅ Data quality validation (Great Expectations)
- ✅ Pipeline orchestration (Apache Airflow)
- ✅ Containerization (Docker)
- ✅ Cloud object storage (AWS S3)
- ✅ ACID transactions + time travel (Delta Lake)
- ✅ Cloud data warehousing (Snowflake)
- ✅ Data modeling (dbt — staging + mart layers)
- ✅ Production-grade Python (logging, error handling, modular code)
- ✅ Git version control

---

## 👤 Author

**Kartik Inamdar**  
Data Engineering Student — 3rd Year E&TC Engineering, Solapur  
📍 90-Day Data Engineering Portfolio Challenge — Project 2 of 5

🔗 [GitHub: Kartik8625](https://github.com/Kartik8625)  
💼 Open to Data Engineering Internships