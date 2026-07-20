# Real-Time Stock Market Data Pipeline Architecture

## Overview

This project implements a production-style real-time stock market data pipeline using the Medallion Architecture (Bronze, Silver, Gold).

The pipeline continuously ingests stock market events from Apache Kafka, processes them using Apache Spark Structured Streaming, stores curated data in PostgreSQL, performs data quality validation, generates operational alerts, and prepares analytical datasets for visualization.

---

## High-Level Architecture

```
                +----------------------+
                |   Python Producer    |
                +----------+-----------+
                           |
                           |
                           v
                  Apache Kafka Topic
                           |
                           |
                           v
           Spark Structured Streaming
                           |
      +--------------------+--------------------+
      |                    |                    |
      v                    v                    v
   Bronze Layer       Silver Layer        Gold Layer
      |                    |                    |
      +--------------------+--------------------+
                           |
                           v
                 PostgreSQL Database
                           |
        +------------------+------------------+
        |                  |                  |
        v                  v                  v
  Batch Audit      Data Quality       Alert Engine
                           |
                           v
                Notification Framework
                           |
                           v
                Grafana / Power BI
```

---

## Technologies

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Docker
- pgAdmin
- Grafana (Upcoming)
- Power BI (Upcoming)

---

## Architecture Pattern

The project follows the Medallion Architecture:

- Bronze Layer
- Silver Layer
- Gold Layer

This architecture separates raw ingestion, cleaned data, and business-ready analytics.

---

## Key Features

- Real-time streaming
- Schema validation
- Data quality monitoring
- Idempotent processing
- Batch auditing
- Operational alerting
- Email notification framework
- Production-ready folder structure