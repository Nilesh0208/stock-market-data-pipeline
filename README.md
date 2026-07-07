# Real-Time Stock Market Analytics Platform

## Overview

This project demonstrates an end-to-end real-time data engineering pipeline built using modern open-source technologies.

## Tech Stack

- Apache Kafka
- Apache Spark Structured Streaming
- Apache Airflow
- dbt Core
- PostgreSQL
- Python
- Docker
- Power BI

## Architecture

Historical Stock Data
        ↓
Kafka Producer
        ↓
Apache Kafka
        ↓
Spark Streaming
        ↓
PostgreSQL (Bronze)
        ↓
dbt (Silver → Gold)
        ↓
Power BI