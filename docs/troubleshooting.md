# Troubleshooting Guide

## Overview

This guide documents common issues encountered while running the local stock-market data pipeline.

The stack includes Docker, Kafka, Spark, PostgreSQL, Airflow, Grafana and Power BI.

---

## Docker Desktop Is Unresponsive

### Symptoms

Commands return:

```text
500 Internal Server Error
```

or Docker API errors.

### Resolution

Restart Docker Desktop:

```powershell
docker desktop restart
```

If unavailable, restart it from the Docker Desktop interface.

If required, shut down WSL:

```powershell
wsl --shutdown
```

Then reopen Docker Desktop.

Verify:

```powershell
docker info
```

Do not use Docker Desktop options such as:

```text
Clean / Purge data
Reset to factory defaults
```

unless data loss is acceptable.

---

## Containers Consume Too Much Memory

### Symptoms

- Laptop becomes slow
- CPU temperature increases
- Docker becomes unresponsive
- Spark tasks remain active for a long time
- Containers restart unexpectedly

### Resolution

Do not run the complete stack simultaneously.

Use staged startup.

Core pipeline:

```powershell
docker compose up -d postgres stock-kafka spark-master spark-worker
```

Power BI:

```powershell
docker compose up -d postgres
```

Grafana:

```powershell
docker compose up -d postgres stock-grafana
```

Airflow:

```powershell
docker compose up -d postgres airflow-postgres airflow-webserver airflow-scheduler
```

Check resource usage:

```powershell
docker stats
```

Stop unnecessary services:

```powershell
docker compose down
```

---

## Producer Cannot Connect to Kafka

### Symptoms

```text
No broker metadata found in MetadataResponse
```

or repeated connection warnings.

### Checks

Verify Kafka:

```powershell
cd docker
docker compose ps stock-kafka
```

Start Kafka:

```powershell
docker compose up -d stock-kafka
```

Verify port `9092`:

```powershell
Test-NetConnection localhost -Port 9092
```

Expected:

```text
TcpTestSucceeded : True
```

The host producer must connect using:

```text
localhost:9092
```

Spark containers must connect using:

```text
stock-kafka:29092
```

---

## Stop a Running Producer

Press:

```text
Ctrl + C
```

If the VS Code terminal does not respond, use the terminal trash icon to terminate the session.

---

## Spark Worker Cannot Connect to Driver

### Symptoms

```text
Failed to connect to spark-master:<dynamic-port>
Lost executor
Unable to create executor
```

### Resolution

Run Spark with fixed driver ports:

```bash
--conf spark.driver.host=spark-master
--conf spark.driver.bindAddress=0.0.0.0
--conf spark.driver.port=37001
--conf spark.blockManager.port=37002
```

Example:

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

---

## Spark Cannot Resolve PostgreSQL

### Symptoms

```text
java.net.UnknownHostException: postgres
```

### Checks

From Spark Master:

```powershell
docker exec stock-spark-master getent hosts postgres
```

From Spark Worker:

```powershell
docker exec stock-spark-worker getent hosts postgres
```

Expected output:

```text
172.x.x.x postgres
```

Test port connectivity:

```powershell
docker exec stock-spark-worker bash -c "timeout 5 bash -c '</dev/tcp/postgres/5432' && echo 'Postgres reachable'"
```

If DNS is unavailable, recreate the Spark containers:

```powershell
docker compose up -d --force-recreate spark-master spark-worker
```

---

## Spark Job Appears Stuck

### Important

Spark Structured Streaming does not exit automatically because it uses:

```python
query.awaitTermination()
```

The terminal remaining active is normal.

### Potential problem

If processing remains at the same message for several minutes, check:

```powershell
docker stats --no-stream
```

Worker logs:

```powershell
docker logs --tail 150 stock-spark-worker
```

Spark UI:

```text
http://localhost:8082
```

Check:

- Jobs
- Stages
- Executors
- Failed tasks
- Active stages

Stop the job using:

```text
Ctrl + C
```

If necessary:

```powershell
docker exec stock-spark-master pkill -f kafka_consumer.py
```

---

## Batch Retries Insert Zero Bronze Rows

### Example

```text
New rows inserted: 0
```

This can be correct.

The pipeline uses unique event IDs and Bronze merge logic.

When Spark retries a previously inserted batch, duplicate events are ignored.

This confirms idempotency.

---

## Pipeline Batch Remains STARTED

### Cause

The Spark job may have stopped after audit initialization but before completion.

### Query

```sql
SELECT
    spark_batch_id,
    status,
    started_at,
    completed_at,
    error_message
FROM monitoring.pipeline_batch_audit
ORDER BY started_at DESC
LIMIT 20;
```

### Resolution

Review Spark logs and determine whether the batch:

- Failed before `fail_batch()` could execute
- Lost PostgreSQL connectivity
- Was terminated manually
- Exhausted local resources

A production system could include automatic timeout handling for stale `STARTED` records.

---

## Airflow DAG Does Not Appear

Check import errors:

```powershell
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Expected:

```text
No data found
```

Check whether the DAG is detected:

```powershell
docker compose exec airflow-scheduler airflow dags list
```

Check scheduler logs:

```powershell
docker compose logs --tail=100 airflow-scheduler
```

Confirm the DAG exists under:

```text
airflow/dags/
```

---

## Airflow Initialization Fails

Run:

```powershell
docker compose up airflow-init
```

Expected final status:

```text
Exited (0)
```

Verify the metadata database:

```powershell
docker compose ps -a
```

Expected:

```text
stock-airflow-postgres Up (healthy)
stock-airflow-init Exited (0)
```

---

## Airflow Freshness Check Fails

### Example

```text
Pipeline data is stale.
```

This is expected when the latest completed Spark batch is older than the configured threshold.

Check:

```sql
SELECT
    spark_batch_id,
    status,
    completed_at
FROM monitoring.pipeline_batch_audit
ORDER BY started_at DESC
LIMIT 5;
```

The DAG will pass after a recent successful batch is created.

For portfolio demonstration, a stale-data failure is useful evidence that monitoring works correctly.

---

## Docker Compose Variables Are Not Set

### Symptoms

```text
The "AIRFLOW_IMAGE_NAME" variable is not set
```

or:

```text
service has neither an image nor a build context
```

### Resolution

Create:

```text
docker/.env
```

Confirm it is not accidentally saved as:

```text
.env.txt
```

Check:

```powershell
Get-ChildItem -Force docker
```

Validate:

```powershell
cd docker
docker compose config
```

---

## Docker Compose YAML Error

### Example

```text
mapping key "image" already defined
```

This generally indicates incorrect YAML indentation.

Service definitions must align at two spaces under:

```yaml
services:
```

Example:

```yaml
services:

  postgres:
    image: postgres:16

  airflow-postgres:
    image: postgres:16
```

Validate after changes:

```powershell
docker compose config --quiet
```

---

## Git Warning for Airflow Logs

### Symptom

```text
could not open directory 'airflow/logs/scheduler/latest/'
```

### Resolution

Ignore Airflow runtime files in `.gitignore`:

```gitignore
airflow/logs/
airflow/plugins/__pycache__/
airflow/config/__pycache__/
airflow/dags/__pycache__/
```

Do not ignore:

```text
airflow/dags/
```

The DAG source files must be committed.

---

## GitHub Actions Cannot Import Producer

### Symptom

```text
ModuleNotFoundError: No module named 'producer'
```

### Resolution

Set `PYTHONPATH` in the workflow:

```yaml
- name: Run tests when available
  env:
    PYTHONPATH: ${{ github.workspace }}
  run: |
    python -m pytest -v
```

---

## Local Pytest Is Not Found

Activate the virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

Run:

```powershell
python -m pytest tests -v
```

If missing:

```powershell
python -m pip install pytest
```

Pytest is installed inside `.venv`, not globally.

---

## Power BI Cannot Connect

Verify PostgreSQL is running:

```powershell
cd docker
docker compose up -d postgres
```

Test:

```powershell
Test-NetConnection localhost -Port 5432
```

Power BI connection:

```text
Server: localhost
Database: stock_market
```

Use the project PostgreSQL username and password.

---

## Grafana Does Not Load

Verify:

```powershell
docker compose ps stock-grafana postgres
```

Start:

```powershell
docker compose up -d postgres stock-grafana
```

Open:

```text
http://localhost:3000
```

Check logs:

```powershell
docker compose logs --tail=100 stock-grafana
```

---

## pgAdmin Does Not Load

Start:

```powershell
docker compose up -d postgres pgadmin
```

Open:

```text
http://localhost:8080
```

Check logs:

```powershell
docker compose logs --tail=100 pgadmin
```

---

## Preserve Docker Data

Safe:

```powershell
docker compose down
```

Unsafe when data must be preserved:

```powershell
docker compose down -v
```

The `-v` option deletes named volumes containing:

- PostgreSQL data
- Kafka data
- Spark checkpoints
- Grafana data
- Airflow metadata

---

## Diagnostic Command Summary

```powershell
docker compose ps -a
docker stats --no-stream
docker info
docker compose config --quiet
docker compose logs --tail=100 <service>
docker exec <container> getent hosts <hostname>
Test-NetConnection localhost -Port <port>
git status
python -m pytest tests -v
python -m compileall producer config spark airflow\dags
```