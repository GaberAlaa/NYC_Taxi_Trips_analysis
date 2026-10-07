# NYC Taxi Trips Big Data Pipeline

<img width="1536" height="1024" alt="Project_Overview" src="https://github.com/user-attachments/assets/69153f94-0389-4cd6-967b-03e0c76ba137" />

## Data Source

WebPage : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

API: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

## Services

| Service      | Purpose                                   |
| ------------ | ----------------------------------------- |
| SeaweedFS    | Local S3-compatible data lake             |
| PostgreSQL   | Serving database for dashboards/streaming |
| Kafka        | Real-time traffic ingestion               |
| Spark master | Spark coordination                        |
| Spark worker | Spark execution                           |
| Airflow      | Batch pipeline orchestration              |
| Superset     | Dashboard                                 |
| Streamlit    | ML prediction UI                          |

## Start

Windows PowerShell:

```powershell
Copy-Item .env.example .env
docker compose pull
docker pull apache/spark:4.1.3-python3
docker pull apache/airflow:3.3.2-python3.12
docker pull apache/superset:6.1.0
docker pull python:3.12-slim
docker compose up -d --build
docker compose ps
```

Linux/macOS:

```bash
cp .env.example .env
docker compose pull
docker pull apache/spark:4.1.3-python3
docker pull apache/airflow:3.3.2-python3.12
docker pull apache/superset:6.1.0
docker pull python:3.12-slim
docker compose up -d --build
docker compose ps
```

## URLs

- SeaweedFS S3: http://localhost:8333
- SeaweedFS Master: http://localhost:9333
- SeaweedFS Filer: http://localhost:8888
- Spark Master UI: http://localhost:8080
- Airflow: http://localhost:8081
- Superset: http://localhost:8088
- Streamlit: http://localhost:8501

## Data lake

Use SeaweedFS for shared project storage:

```text
s3a://traffic-raw/
s3a://traffic-clean/
s3a://traffic-models/
```

This keeps the pipeline independent from individual team members' host paths.

## Pipeline

```text
Raw files
   |
   v
Airflow
   |
   v
Spark batch
   |
   +--> SeaweedFS raw
   |
   +--> SeaweedFS clean
   |
   +--> train/test split
   |
   +--> SparkML/XGBoost model
   |
   +--> SeaweedFS models
   |
   v
PostgreSQL
   |
   +--> Superset

Kafka
   |
   v
Spark Structured Streaming
   |
   v
PostgreSQL
   |
   v
Superset

SeaweedFS models
   |
   v
Streamlit
```

## Reset

WARNING: deletes all Docker volumes and stored project data.

```bash
docker compose down -v
docker compose up -d --build
```
