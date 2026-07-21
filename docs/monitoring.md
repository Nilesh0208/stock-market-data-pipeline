# Monitoring and Alerting

## Overview

The project includes monitoring at multiple levels:

- Spark batch auditing
- Data-quality evaluation
- Operational alerts
- Grafana dashboards
- Airflow service checks
- Airflow stale-data detection
- GitHub Actions CI monitoring

These layers help distinguish between infrastructure availability, processing failures, data-quality problems and stale data.

---

## Pipeline Batch Audit

Every Spark micro-batch is recorded in:

```text
monitoring.pipeline_batch_audit
```

The audit lifecycle begins when processing starts:

```text
STARTED
```

It then ends as:

```text
SUCCESS
```

or:

```text
FAILED
```

Important columns include:

```text
pipeline_name
spark_batch_id
status
source_record_count
bronze_inserted_count
silver_inserted_count
rejected_record_count
gold_affected_count
started_at
completed_at
error_message
```

Example query:

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
    completed_at,
    error_message
FROM monitoring.pipeline_batch_audit
ORDER BY started_at DESC
LIMIT 20;
```

---

## Batch Status Interpretation

### STARTED

The batch began processing but has not yet completed.

A batch that remains `STARTED` for too long can indicate:

- Spark job interruption
- Executor failure
- Database connectivity failure
- Resource exhaustion
- Application termination during processing

### SUCCESS

The batch completed all required processing stages.

A successful batch can include rejected records if the valid records were processed and invalid records were correctly quarantined.

### FAILED

The batch encountered an exception.

The `error_message` column should be checked to identify the root cause.

---

## Data-Quality Monitoring

Each micro-batch is evaluated using configurable thresholds.

The framework measures:

```text
source_record_count
valid_record_count
rejected_record_count
rejection_rate_percent
average_latency_ms
maximum_latency_ms
quality_status
severity
```

Possible statuses:

```text
HEALTHY
WARNING
CRITICAL
```

---

## Health Classification

### HEALTHY

A batch is healthy when rejection and latency values remain within acceptable thresholds.

### WARNING

A warning indicates degraded processing without complete failure.

Examples:

- Elevated average latency
- Moderate rejection rate
- Temporary performance degradation

### CRITICAL

A critical batch requires attention.

Examples:

- High rejection rate
- Very high processing latency
- Excessive maximum latency
- Fully invalid input batch

---

## Rejected Records

Invalid records are stored in:

```text
silver.stock_events_rejected
```

Important columns include:

```text
event_id
symbol
price
volume
currency
rejection_reason
spark_batch_id
```

Example query:

```sql
SELECT
    spark_batch_id,
    event_id,
    symbol,
    price,
    volume,
    currency,
    rejection_reason
FROM silver.stock_events_rejected
ORDER BY spark_batch_id DESC
LIMIT 50;
```

Rejected records are retained for:

- Root-cause analysis
- Producer-quality monitoring
- Reprocessing
- Compliance and audit evidence

---

## Operational Alerts

Operational alerts are stored in:

```text
monitoring.pipeline_alerts
```

Alert fields include:

```text
batch_id
severity
alert_type
alert_message
rejection_rate_pct
average_latency_ms
maximum_latency_ms
alert_status
created_at
```

Supported severity values:

```text
WARNING
CRITICAL
```

Example query:

```sql
SELECT
    batch_id,
    severity,
    alert_type,
    alert_message,
    rejection_rate_pct,
    average_latency_ms,
    maximum_latency_ms,
    alert_status,
    created_at
FROM monitoring.pipeline_alerts
ORDER BY created_at DESC
LIMIT 20;
```

---

## Alert Status

Alerts initially use:

```text
OPEN
```

A production system could extend this with:

```text
ACKNOWLEDGED
RESOLVED
SUPPRESSED
```

This project demonstrates the generation and persistence of operational alerts.

---

## Email Notifications

The project includes configurable email-notification settings.

Example:

```env
CRITICAL_EMAIL_NOTIFICATIONS_ENABLED=false

SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USE_TLS=true

SMTP_USERNAME=your-smtp-username
SMTP_PASSWORD=your-smtp-app-password

ALERT_EMAIL_FROM=pipeline-alerts@example.com
ALERT_EMAIL_TO=operations@example.com
```

Email notifications remain disabled by default for safe local development.

Credentials must never be committed to Git.

---

## Grafana Monitoring

Grafana reads PostgreSQL monitoring tables.

The dashboard displays:

- Total successful batches
- Failed batches
- Rejected records
- Total processed records
- Pipeline processing trend
- Batch health status
- Rejection-rate trend
- Average processing latency
- Maximum processing latency
- Recent pipeline batches
- Operational alerts

Grafana URL:

```text
http://localhost:3000
```

Recommended local startup:

```powershell
cd docker
docker compose up -d postgres stock-grafana
```

---

## Airflow Monitoring

The project includes:

```text
stock_pipeline_health_check
stock_pipeline_service_health
```

The basic DAG confirms Airflow task execution.

The service-health DAG checks:

- PostgreSQL TCP connectivity
- Kafka TCP connectivity
- Spark Master TCP connectivity
- Latest pipeline audit status
- Latest completed batch freshness

---

## Airflow Dependency Flow

```text
                         check_postgres
                        /
start_health_check ---- check_kafka
                        \
                         check_spark_master
                                  |
                                  v
                   check_latest_pipeline_audit
                                  |
                                  v
                       finish_health_check
```

Infrastructure checks run in parallel.

The audit check runs only after all infrastructure checks pass.

---

## Stale-Data Detection

The Airflow DAG compares the latest `completed_at` timestamp with the current UTC time.

Example threshold:

```text
MAX_PIPELINE_STALENESS_MINUTES = 30
```

The DAG fails when the latest completed batch is older than the threshold.

Example error:

```text
Pipeline data is stale.
Latest completed batch is older than the maximum allowed age.
```

This detects cases where:

- Containers are running
- Ports are reachable
- No new data is being processed

This is an important distinction between service availability and pipeline health.

---

## Scheduled Airflow Monitoring

The service-health DAG runs every 15 minutes:

```text
*/15 * * * *
```

The DAG also uses:

```text
catchup=False
```

This prevents Airflow from generating historical runs for missed intervals.

---

## Power BI Operational Monitoring

The Power BI Pipeline Operations Dashboard displays:

- Successful batches
- Failed batches
- Total rejected records
- Total processed records
- Pipeline processing trend
- Operational alerts
- Recent data-quality results

Power BI is intended for business and analytical users.

Grafana is intended for operational monitoring.

Airflow is intended for workflow health and automated detection.

---

## CI Monitoring

GitHub Actions runs on:

```text
push to main
pull request to main
manual workflow dispatch
```

The workflow validates:

- Python syntax
- Unit tests
- Required project structure
- Docker Compose configuration

A green workflow confirms that the repository passes automated validation.

---

## Monitoring Strategy Summary

```text
Spark Audit
    |
    +--> Processing state
    +--> Record counts
    +--> Error information

Data Quality
    |
    +--> Rejection rate
    +--> Latency
    +--> Health status

Operational Alerts
    |
    +--> Warning
    +--> Critical

Grafana
    |
    +--> Operational dashboard

Power BI
    |
    +--> Business and pipeline reporting

Airflow
    |
    +--> Service checks
    +--> Audit checks
    +--> Freshness checks

GitHub Actions
    |
    +--> Source and configuration validation
```

---

## Production Recommendations

For production monitoring, consider:

- Prometheus metrics
- Centralized log aggregation
- PagerDuty or Opsgenie integration
- Slack or Microsoft Teams alerts
- Alert acknowledgement workflow
- Service-level objectives
- Data freshness service-level agreements
- Dashboard access controls
- Automated incident creation
- Metric retention policies
- Distributed tracing