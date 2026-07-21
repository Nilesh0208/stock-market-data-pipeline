# Pipeline Flow

## Overview

This project processes simulated stock-market events through a real-time streaming pipeline.

The complete flow is:

```text
Python Producer
      |
      v
Apache Kafka
      |
      v
Spark Structured Streaming
      |
      +--------------------------+
      |                          |
      v                          v
Bronze Layer              Rejected Records
      |
      v
Silver Layer
      |
      v
Gold Layer
      |
      v
PostgreSQL
      |
      +----------------+----------------+----------------+
      |                |                |
      v                v                v
Power BI           Grafana          Airflow