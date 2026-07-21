# Project Setup Guide

## Overview

This guide explains how to configure and run the Real-Time Stock Market Data Pipeline locally.

The full technology stack includes:

- Python producer
- Apache Kafka
- Apache Spark
- PostgreSQL
- pgAdmin
- Apache Airflow
- Grafana
- Power BI
- GitHub Actions

Because the complete stack requires significant memory, the project supports running services in smaller groups.

---

## Prerequisites

Install the following software:

- Git
- Python 3.11
- Java 17
- Docker Desktop
- Power BI Desktop
- Visual Studio Code
- PostgreSQL client or pgAdmin

Recommended local environment:

```text
Operating System: Windows 11
Python: 3.11
Java: 17
Docker Desktop: Latest stable version
```

---

## Clone the Repository

```powershell
git clone <repository-url>
cd stock-market-data-pipeline
```

Replace `<repository-url>` with the GitHub repository URL.

---

## Create the Python Virtual Environment

From the project root:

```powershell
python -m venv .venv
```

Activate it:

```powershell
.\.venv\Scripts\Activate.ps1
```

Upgrade pip:

```powershell
python -m pip install --upgrade pip
```

Install required Python packages:

```powershell
python -m pip install kafka-python
python -m pip install python-dotenv
python -m pip install pytest
python -m pip install psycopg2-binary
```

---

## Environment Configuration

Create the local environment file:

```text
.env
```

Use `.env.example` as the template.

Example:

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=stock_market
POSTGRES_USER=stock_user
POSTGRES_PASSWORD=replace-with-local-password

CRITICAL_EMAIL_NOTIFICATIONS_ENABLED=false
```

Do not commit the real `.env` file.

---

## Docker Environment Configuration

Create:

```text
docker/.env
```

Example:

```env
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin123

AIRFLOW_UID=50000
AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5

AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_FIRSTNAME=Pipeline
AIRFLOW_ADMIN_LASTNAME=Admin
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

These credentials are suitable only for local development.

---

## Validate Docker Compose

From the Docker folder:

```powershell
cd docker
docker compose config --quiet
```

No output means the configuration is valid.

Return to the project root:

```powershell
cd ..
```

---

## Start the Core Streaming Stack

Start only the services required for Kafka and Spark processing:

```powershell
cd docker

docker compose up -d `
  postgres `
  stock-kafka `
  spark-master `
  spark-worker
```

Check status:

```powershell
docker compose ps
```

Required containers:

```text
stock-postgres
stock-kafka
stock-spark-master
stock-spark-worker
```

---

## Initialize the Database

Open pgAdmin:

```text
http://localhost:8080
```

Local credentials:

```text
Email: admin@stock.com
Password: admin123
```

Connect using:

```text
Host: postgres
Port: 5432
Database: stock_market
Username: stock_user
Password: stock_password
```

Run the SQL scripts in numerical order from the `sql/` directory.

Example order:

```text
001_bronze_silver_gold.sql
002_gold_processed_batches.sql
003_silver_merge.sql
004_gold_merge.sql
005_pipeline_batch_audit.sql
006_data_quality.sql
007_operational_alerts.sql
```

Use the actual filenames available in the repository.

---

## Start the Spark Streaming Consumer

Enter the Spark master container:

```powershell
docker exec -it stock-spark-master bash
```

Run:

```bash
/opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.postgresql:postgresql:42.7.3 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.driver.host=spark-master \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=37001 \
  --conf spark.blockManager.port=37002 \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=768m \
  /opt/spark-apps/streaming/kafka_consumer.py
```

The command remains active because Structured Streaming is a long-running process.

Stop it using:

```text
Ctrl + C
```

---

## Start the Python Producer

Open a second PowerShell terminal.

From the project root:

```powershell
.\.venv\Scripts\Activate.ps1
python -m producer.producer
```

Expected output includes:

```text
Message delivered
Topic=stock_prices
Partition=0
Offset=<number>
```

Stop the producer using:

```text
Ctrl + C
```

---

## Verify Pipeline Data

Example PostgreSQL checks:

```sql
SELECT COUNT(*) FROM bronze.stock_events;

SELECT COUNT(*) FROM silver.stock_events;

SELECT COUNT(*) FROM silver.stock_events_rejected;

SELECT COUNT(*) FROM gold.stock_metrics_1min;
```

Check recent pipeline batches:

```sql
SELECT
    spark_batch_id,
    status,
    source_record_count,
    bronze_inserted_count,
    silver_inserted_count,
    rejected_record_count,
    gold_affected_count,
    started_at,
    completed_at
FROM monitoring.pipeline_batch_audit
ORDER BY started_at DESC
LIMIT 10;
```

---

## Start Airflow

Initialize Airflow once:

```powershell
cd docker
docker compose up airflow-init
```

A successful initialization ends with:

```text
stock-airflow-init exited with code 0
```

Start Airflow:

```powershell
docker compose up -d `
  postgres `
  airflow-postgres `
  airflow-webserver `
  airflow-scheduler
```

Open:

```text
http://localhost:8083
```

Sign in:

```text
Username: admin
Password: admin
```

---

## Start Grafana

Start PostgreSQL and Grafana:

```powershell
cd docker
docker compose up -d postgres stock-grafana
```

Open:

```text
http://localhost:3000
```

Use the Grafana credentials from `docker/.env`.

---

## Open Power BI

Start only PostgreSQL:

```powershell
cd docker
docker compose up -d postgres
```

Open:

```text
powerbi/stock_market_analytics.pbix
```

Power BI connects to the PostgreSQL Gold and monitoring tables.

---

## Run Unit Tests

Activate the virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Run:

```powershell
python -m pytest tests -v
```

Expected result:

```text
7 passed
```

---

## Compile Python Files

```powershell
python -m compileall producer config spark airflow\dags
```

A successful command completes without a traceback.

---

## Stop Containers

From the Docker folder:

```powershell
docker compose down
```

This removes containers and the Docker network but preserves named volumes.

Never use:

```powershell
docker compose down -v
```

The `-v` option deletes persisted project data.

---

## Resource-Efficient Demonstration

Run only the services needed for the current demonstration.

### Core pipeline

```powershell
docker compose up -d postgres stock-kafka spark-master spark-worker
```

### Power BI

```powershell
docker compose up -d postgres
```

### Grafana

```powershell
docker compose up -d postgres stock-grafana
```

### Airflow

```powershell
docker compose up -d postgres airflow-postgres airflow-webserver airflow-scheduler
```

This approach prevents unnecessary laptop memory and CPU usage.