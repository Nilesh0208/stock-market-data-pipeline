# Real-Time Stock Market Analytics Platform

An end-to-end real-time data engineering project that ingests simulated stock-market events through Apache Kafka, processes them using Spark Structured Streaming, stores curated data in PostgreSQL using a Bronze-Silver-Gold architecture, and exposes analytics through Power BI and Grafana.

The project also includes data-quality validation, rejected-record quarantine, idempotent processing, operational alerts, pipeline auditing, Apache Airflow monitoring, unit tests, and GitHub Actions CI.

---

## Project Objectives

The project demonstrates how to design and build a production-style streaming data platform with:

- Real-time event ingestion
- Distributed stream processing
- Medallion architecture
- Data-quality enforcement
- Idempotent database writes
- Operational monitoring and alerting
- Workflow orchestration
- Business intelligence reporting
- Automated CI validation

---

## Project Screenshots

### Power BI — Market Overview

![Power BI Market Overview](docs/screenshots/powerbi-market-overview.png)

### Power BI — Stock Performance Analysis

![Power BI Stock Performance](docs/screenshots/powerbi-stock-performance.png)

### Power BI — Pipeline Operations

![Power BI Pipeline Operations](docs/screenshots/powerbi-pipeline-operations.png)

### Grafana — Operational Monitoring

![Grafana Monitoring Overview](docs/screenshots/grafana-monitoring-overview.png)

![Grafana Monitoring Details](docs/screenshots/grafana-monitoring-details.png)

### Airflow — Successful Service Health Check

![Airflow Service Health Success](docs/screenshots/airflow-service-health-success.png)

### Airflow — Stale Pipeline Detection

![Airflow Stale Data Detection](docs/screenshots/airflow-stale-data-detection.png)

### GitHub Actions — CI Success

![GitHub Actions CI Success](docs/screenshots/github-actions-ci-success.png)

### Kafka Producer

![Kafka Producer Sending Events](docs/screenshots/kafka-producer-sending.png)

### Spark Structured Streaming

![Spark Streaming Processing](docs/screenshots/spark-job-running.png)

### PostgreSQL — Medallion Schemas

![PostgreSQL Medallion Schemas](docs/screenshots/postgres-medallion-schemas.png)

### PostgreSQL — Bronze Data

![PostgreSQL Bronze Data](docs/screenshots/postgres-bronze-data.png)

---

## Architecture

```text
Python Stock Producer
        |
        v
Apache Kafka
        |
        v
Spark Structured Streaming
        |
        +------------------------------+
        |                              |
        v                              v
Bronze Layer                    Rejected Records
Raw Event Storage               Data Quarantine
        |
        v
Silver Layer
Cleaned and Validated Events
        |
        v
Gold Layer
One-Minute Stock Metrics
        |
        v
PostgreSQL
        |
        +-------------------+-------------------+
        |                   |                   |
        v                   v                   v
Power BI                Grafana             Airflow
Analytics Dashboard     Monitoring          Health Checks
```

---