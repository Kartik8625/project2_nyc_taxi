# NYC Taxi Analytics Pipeline — Project 2

## Overview
Production-grade batch data pipeline processing 3 million NYC Yellow Taxi trips (January 2024).

## Tech Stack
| Tool | Purpose |
|------|---------|
| PySpark 3.5.0 | Data processing |
| Great Expectations 0.18.19 | Data validation |
| Apache Airflow 2.8.1 | Orchestration |
| Docker | Containerization |
| Python 3.12 | Pipeline code |

## Dataset
- Source: NYC Open Data (government)
- Size: ~3 million rows, 48MB parquet
- File: yellow_tripdata_2024-01.parquet

## Pipeline Architecture
```
Raw Parquet (3M rows)
      ↓
Great Expectations (data validation)
      ↓
PySpark Processing (clean + transform)
      ↓
Partitioned Parquet Output (2.7M clean rows)
```

## Data Quality Results
| Column | Issue Found | Rows Affected |
|--------|-------------|---------------|
| fare_amount | Negative fares | 37,494 |
| passenger_count | Zero passengers | 31,525 |
| trip_distance | Zero distance | 60,371 |
| payment_type | Invalid payment | 140,162 |
| Total removed | 8.13% dirty data | 240,892 |

## Transformations Added
- trip_duration_minutes
- fare_per_mile
- pickup_hour
- time_of_day (Morning/Afternoon/Evening/Night)
- pickup_date

## Analytics Queries
1. Revenue by time of day
2. Payment method breakdown
3. Daily trip volume trend
4. Fare efficiency by time of day

## Airflow DAG — Pipeline Orchestration
**DAG:** nyc_taxi_pipeline
**Schedule:** Daily at 6:00 AM
**Orchestration:** Apache Airflow 2.8.1 (Docker)

### Pipeline Tasks
| Task | Type | Description |
|------|------|-------------|
| validate_data | BashOperator | Great Expectations validation on 3M rows |
| run_pipeline | BashOperator | PySpark processing — 2.7M clean rows output |
| verify_output | BashOperator | Confirms partitioned parquet output |

### Task Dependencies
validate_data >> run_pipeline >> verify_output

## Pipeline Metrics
- Raw rows processed: 2,964,624
- Clean rows output: 2,723,732
- Dirty rows removed: 240,892 (8.13%)
- New columns added: 5
- Analytics queries: 4
- Total runtime: ~42 seconds
- Output format: Parquet (partitioned by time_of_day)

## Author
Kartik Inamd — Data Engineering Student
GitHub: github.com/Kartik8625
