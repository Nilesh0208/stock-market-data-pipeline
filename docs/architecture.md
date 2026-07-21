# Architecture

## Overview

The project implements a real-time stock market data pipeline using an event-driven and medallion-style architecture.

The main processing flow is:

```text
Python Producer
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
Bronze Layer                  Rejected Records
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