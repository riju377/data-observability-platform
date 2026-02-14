# Data Observability Platform -- User Guide

## 1. Getting Started

The Data Observability Platform automatically captures lineage, schema, metrics, and anomalies from your Apache Spark jobs -- without requiring any code changes. A Spark listener attaches transparently via `spark-submit` configuration flags and publishes metadata asynchronously over HTTP to a FastAPI backend backed by PostgreSQL. You get table lineage, column-level lineage, schema versioning, row count metrics, anomaly detection, and multi-channel alerting out of the box.

### Prerequisites

- **Apache Spark 3.5+** installed and available on your PATH
- **The assembly JAR** built from this project (`data-observability-platform-assembly-1.1.0.jar`)
- **A running API server** (FastAPI backend + PostgreSQL)

Build the assembly JAR if you have not already:

```bash
sbt assembly
```

The JAR will be written to `target/scala-2.12/data-observability-platform-assembly-1.1.0.jar`.

Start the API server:

```bash
cd api
python main.py
```

The API server runs on `http://localhost:8000` by default. Swagger docs are available at `http://localhost:8000/docs`. The React frontend runs on `http://localhost:3000`.

---

## 2. Authentication

All API requests require authentication via an API key passed as a Bearer token.

### API Key Format

Keys follow the format `obs_live_` followed by random characters. Example:

```
obs_live_DEMO_KEY_FOR_TESTING_ONLY
```

Keys are SHA-256 hashed on the server side, with prefix-based lookup for efficient validation.

### Getting an API Key

**Option A: Use the demo key (local development only)**

A demo key is seeded automatically by `init-db.sql`:

```
obs_live_DEMO_KEY_FOR_TESTING_ONLY
```

**Option B: Register and generate a key via the API**

1. Register a new account:

```bash
curl -X POST http://localhost:8000/api/v1/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "you@company.com",
    "password": "your-secure-password",
    "name": "Your Name"
  }'
```

This returns a JWT token in the response.

2. Create an API key using the JWT:

```bash
curl -X POST http://localhost:8000/api/v1/auth/api-keys \
  -H "Authorization: Bearer <your-jwt-token>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production ETL",
    "scopes": ["ingest"],
    "expires_in_days": 365
  }'
```

The response includes the full API key. Save it immediately -- it cannot be retrieved again.

### Using the API Key

**For spark-submit:**

```
--conf "spark.observability.api.key=YOUR_API_KEY"
```

**For curl:**

```bash
curl -H "Authorization: Bearer YOUR_API_KEY" http://localhost:8000/datasets
```

---

## 3. Enabling the Listener on Your Spark Jobs

### spark-submit (Local / Standalone / YARN)

```bash
spark-submit \
  --class your.MainClass \
  --master "local[*]" \
  --jars path/to/data-observability-platform-assembly-1.1.0.jar \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf "spark.observability.api.url=http://localhost:8000" \
  --conf "spark.observability.api.key=YOUR_API_KEY" \
  your-application.jar
```

No code changes are required. The listener registers transparently via `spark.extraListeners` and captures all read/write operations automatically.

### Databricks

Add the following Spark configuration to your cluster or job:

```
spark.extraListeners  com.observability.listener.ObservabilityListener
spark.observability.api.url  https://your-api-server.com
spark.observability.api.key  YOUR_API_KEY
```

Upload the assembly JAR to DBFS or Unity Catalog Volumes and reference it in the cluster's Libraries tab.

### AWS EMR

Add `--conf` flags in the EMR step configuration:

```json
{
  "Name": "Your Spark Job",
  "ActionOnFailure": "CONTINUE",
  "HadoopJarStep": {
    "Jar": "command-runner.jar",
    "Args": [
      "spark-submit",
      "--class", "your.MainClass",
      "--jars", "s3://your-bucket/jars/data-observability-platform-assembly-1.1.0.jar",
      "--conf", "spark.extraListeners=com.observability.listener.ObservabilityListener",
      "--conf", "spark.observability.api.url=https://your-api-server.com",
      "--conf", "spark.observability.api.key=YOUR_API_KEY",
      "s3://your-bucket/jars/your-application.jar"
    ]
  }
}
```

Alternatively, use a bootstrap action to place the JAR on each node and set Spark defaults.

### Local Development

Same as the standard spark-submit command above. Use the demo key and `http://localhost:8000` as the API URL:

```bash
spark-submit \
  --class your.MainClass \
  --master "local[*]" \
  --jars target/scala-2.12/data-observability-platform-assembly-1.1.0.jar \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf "spark.observability.api.url=http://localhost:8000" \
  --conf "spark.observability.api.key=obs_live_DEMO_KEY_FOR_TESTING_ONLY" \
  your-application.jar
```

### What Gets Captured Automatically

Once the listener is enabled, the following metadata is captured for every Spark job with no additional configuration:

- **Table lineage** -- Input-to-output dataset dependencies. If your job reads from `bronze_taxi_trips` and writes to `silver_enriched_trips`, that edge is recorded. Path-based datasets are named using a `bucket:logical_name` convention (e.g., `mw-device-profile:high_value_brand_propensity`). Self-reads (where Spark reads back data it just wrote as part of its commit protocol) and checkpoint/staging paths are automatically filtered out.
- **Column lineage** -- Field-to-field mappings with transform type classification:
  - `DIRECT` -- Column passes through unchanged
  - `EXPRESSION` -- Column is derived from a computation (e.g., `UPPER(name)`, `value * 2`)
  - `AGGREGATE` -- Column is the result of an aggregation (e.g., `SUM`, `COUNT`, `AVG`)
  - `JOIN` -- Column originates from a join condition
- **Schema capture** -- Column names, data types, and nullability for every dataset written
- **Metrics** -- Row count, byte size, and execution time per write operation
- **Job execution details** -- Start/end timestamps, stage breakdown, shuffle read/write bytes, task counts

---

## 4. Querying the API

All endpoints require the `Authorization: Bearer YOUR_API_KEY` header. The examples below use `API_URL=http://localhost:8000` and `API_KEY` as placeholders.

### Datasets

**List all datasets:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets"
```

Optional query parameters: `dataset_type`, `limit`, `offset`.

**Get a single dataset:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/bronze_taxi_trips"
```

### Lineage

**Full lineage graph (nodes + edges):**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/silver_enriched_trips/lineage"
```

Returns the dataset, all upstream datasets, all downstream datasets, and the edges connecting them.

**Upstream dependencies (parents):**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/silver_enriched_trips/upstream?max_depth=5"
```

**Downstream dependencies (impact analysis):**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/bronze_taxi_trips/downstream?max_depth=5"
```

Use this to answer: "If I change this dataset, what downstream tables are affected?"

**Column lineage for a dataset:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/gold_borough_metrics/column-lineage"
```

**Column upstream/downstream (single column):**

```bash
# Where does this column's data come from?
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/gold_daily_metrics/columns/total_revenue/upstream"

# What will break if I change this column?
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/bronze_taxi_trips/columns/fare_amount/downstream"
```

**Column impact analysis:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/bronze_taxi_trips/columns/fare_amount/impact"
```

Returns all affected datasets and columns with criticality rating (LOW, MEDIUM, HIGH, CRITICAL).

### Schema

**Current schema:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/demo_customers/schema"
```

**Schema version history:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/demo_customers/schema/history?limit=10"
```

**Diff between two schema versions:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/demo_customers/schema/diff?from_version=<uuid1>&to_version=<uuid2>"
```

Returns added, removed, and modified fields, plus a `is_breaking` flag.

### Metrics

**Time-series metrics:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/bronze_taxi_trips/metrics?hours=24"
```

Returns row counts, byte sizes, and execution times over time.

**Statistical summary:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/datasets/bronze_taxi_trips/metrics/stats?days=30"
```

Returns mean, stddev, min, max, p5, and p95 for row counts and byte sizes.

### Anomalies

**List anomalies (with filters):**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/anomalies?severity=CRITICAL&status=OPEN&hours=24&limit=50"
```

All filter parameters are optional: `dataset_name`, `severity` (WARNING, CRITICAL), `status` (OPEN, ACKNOWLEDGED, RESOLVED), `hours`, `limit`.

**Get anomaly details:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/anomalies/<anomaly-uuid>"
```

**Anomaly summary (counts by severity and status):**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/anomalies/stats/summary?hours=24"
```

### Alert Rules

**Create an alert rule:**

```bash
curl -X POST "$API_URL/alert-rules" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production Spike Alert",
    "dataset_name": "bronze_*",
    "severity": "WARNING",
    "channel_type": "EMAIL",
    "channel_config": {"email_to": ["team@company.com"]},
    "enabled": true,
    "deduplication_minutes": 30
  }'
```

**List alert rules:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/alert-rules"
```

Optional filter: `?enabled=true`.

**Update an alert rule:**

```bash
curl -X PUT "$API_URL/alert-rules/<rule-uuid>" \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Updated Alert",
    "dataset_name": "bronze_*",
    "severity": "CRITICAL",
    "channel_type": "EMAIL",
    "channel_config": {"email_to": ["oncall@company.com"]},
    "enabled": true,
    "deduplication_minutes": 60
  }'
```

**Delete an alert rule:**

```bash
curl -X DELETE "$API_URL/alert-rules/<rule-uuid>" \
  -H "Authorization: Bearer $API_KEY"
```

### Alert History

**Recent alerts sent:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/alert-history?hours=24"
```

Optional filters: `status` (SENT, FAILED, DEDUPLICATED), `limit`.

### Jobs

**List job executions:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/jobs?status=Success&hours=24&limit=20"
```

**Get stage metrics for a job:**

```bash
curl -H "Authorization: Bearer $API_KEY" \
  "$API_URL/jobs/<job-uuid>/stages"
```

---

## 5. Setting Up Alerts

Alert rules define which anomalies trigger notifications and where those notifications are sent. Each rule matches anomalies by dataset name and severity, then dispatches to a configured channel.

### Email (Gmail)

**Step 1: Configure the API server.**

Set the following in `api/.env`:

```
EMAIL_BACKEND=gmail
GMAIL_ADDRESS=your-app@gmail.com
GMAIL_APP_PASSWORD=your-16-char-app-password
```

Use a Gmail App Password (not your regular password). Generate one at https://myaccount.google.com/apppasswords.

**Step 2: Create an alert rule.**

```bash
curl -X POST http://localhost:8000/alert-rules \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Production Spike Alert",
    "dataset_name": "bronze_*",
    "severity": "WARNING",
    "channel_type": "EMAIL",
    "channel_config": {"email_to": ["team@company.com"]},
    "enabled": true,
    "deduplication_minutes": 30
  }'
```

### Slack

**Step 1:** Create an Incoming Webhook in your Slack workspace (Apps > Incoming Webhooks > Add New Webhook to Workspace).

**Step 2: Create an alert rule.**

```bash
curl -X POST http://localhost:8000/alert-rules \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Slack Pipeline Alerts",
    "dataset_name": "silver_*",
    "severity": "CRITICAL",
    "channel_type": "SLACK",
    "channel_config": {"webhook_url": "https://hooks.slack.com/services/YOUR_WORKSPACE/YOUR_CHANNEL/YOUR_TOKEN"},
    "enabled": true,
    "deduplication_minutes": 15
  }'
```

### Microsoft Teams

**Step 1:** Create an Incoming Webhook in your Teams channel (Channel Settings > Connectors > Incoming Webhook).

**Step 2: Create an alert rule.**

```bash
curl -X POST http://localhost:8000/alert-rules \
  -H "Authorization: Bearer $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Teams Data Quality Alerts",
    "dataset_name": "*",
    "severity": "CRITICAL",
    "channel_type": "TEAMS",
    "channel_config": {"webhook_url": "https://outlook.office.com/webhook/..."},
    "enabled": true,
    "deduplication_minutes": 30
  }'
```

### Dataset Name Pattern Matching

The `dataset_name` field in alert rules supports pattern matching:

| Pattern | Behavior |
|---------|----------|
| `"bronze_taxi_trips"` | Exact match -- only that specific table |
| `"bronze_*"` | Wildcard prefix -- matches all tables starting with `bronze_` |
| `"*"` | Catch-all -- matches every dataset |
| `null` | Same as `"*"` -- matches everything |

### Deduplication

The `deduplication_minutes` field prevents alert fatigue. If an alert was already sent for the same rule within the deduplication window, subsequent matching anomalies are recorded as `DEDUPLICATED` in alert history instead of sending another notification.

---

## 6. Understanding Anomaly Detection

### How It Works

The platform uses **3-sigma statistical process control** to detect anomalies in row counts:

1. When a new metric is ingested, the system looks at the last **30 days** of historical data for that dataset.
2. It computes the **mean** and **standard deviation** of row counts.
3. If the current value falls more than **3 standard deviations** from the mean, it is flagged as an anomaly.

### Requirements

- **Minimum data points:** Detection requires at least **5 historical data points**. No anomalies will be detected before this threshold is reached.
- The current data point is included in the mean/stddev calculation, which means a single outlier inflates both values. For reliable detection, have at least 15 normal data points before expecting a spike to be caught.

### Anomaly Types

| Type | Severity | Meaning |
|------|----------|---------|
| `RowCountSpike` | WARNING | Row count is more than 3 sigma above the mean |
| `RowCountDrop` | CRITICAL | Row count is more than 3 sigma below the mean |

### Interpreting Results

An anomaly record contains:

- `expected_value` -- The historical mean (what the system expected)
- `actual_value` -- The observed value
- `deviation_score` -- How many standard deviations the value is from the mean
- `description` -- Human-readable explanation

Example: "Row count of 10000 is 5.2 standard deviations above the 30-day mean of 1000 (threshold: 3.0 sigma)"

### Anomaly Status Lifecycle

- `OPEN` -- Newly detected, not yet reviewed
- `ACKNOWLEDGED` -- Someone is looking into it
- `RESOLVED` -- Issue has been addressed

---

## 7. Using the Frontend

The React frontend is available at `http://localhost:3000` and provides a visual interface for all platform features.

### Dashboard

The main dashboard provides an overview of:
- Total tracked datasets
- Recent anomalies with severity indicators
- Quick links to drill into specific datasets

### Lineage Page

An interactive directed acyclic graph (DAG) rendered with ReactFlow and automatically laid out using the dagre library. Edges use bezier curves for readability. Click any node to see its details, upstream/downstream dependencies, and column-level lineage. Use this to visually trace data flow through your pipeline.

**Composite dataset names:** Path-based datasets (e.g., S3 paths) are displayed with a `bucket:name` naming convention. In the UI, the short logical name is shown prominently on each node, with a small bucket badge indicating the storage container. The dataset dropdown shows entries in `displayName (bucket)` format so you can distinguish datasets that share a logical name but reside in different buckets.

### Schema Page

View the current schema for any dataset. The schema tree containers handle wide schemas gracefully with horizontal scrolling to prevent overflow. The history view shows all schema versions with change descriptions, highlighting:
- Added columns (non-breaking)
- Removed columns (breaking)
- Type changes (breaking)
- Nullability changes (breaking)

### Metrics Page

Time-series charts showing row count and byte size trends over time. Useful for spotting gradual drift or sudden changes in data volumes.

### Anomalies Page

Lists all detected anomalies with filtering by dataset, severity, and status. You can acknowledge and resolve anomalies directly from this page.

---

## 8. Running the Demo Scripts

Three demo scripts are included to exercise the platform end-to-end. Each uses environment variables for configuration.

### Environment Setup

```bash
export API_URL="http://localhost:8000"
export API_KEY="obs_live_DEMO_KEY_FOR_TESTING_ONLY"
export JAR_PATH="target/scala-2.12/data-observability-platform-assembly-1.1.0.jar"
```

Make sure the API server is running and the JAR is built (`sbt assembly`) before running any demo.

### run-quick-test.sh -- Smoke Test

```bash
./run-quick-test.sh
```

Creates 3 datasets (`test_raw`, `test_bronze`, `test_silver`) with 2 lineage edges and column lineage. This is a fast validation that the listener and API are working correctly.

What it does:
1. Creates `test_raw` with 5 rows
2. Transforms to `test_bronze` with `UPPER` and `value * 2` expressions
3. Aggregates to `test_silver` with `COUNT`, `SUM`, `AVG`
4. Captures all lineage, column lineage, and schemas

Verify results:

```bash
curl "$API_URL/datasets"
curl "$API_URL/datasets/test_silver/lineage"
curl "$API_URL/datasets/test_silver/column-lineage"
```

### run-demo.sh -- Full Pipeline with Anomaly Detection

```bash
./run-demo.sh
```

Runs a comprehensive demo showcasing the full medallion architecture (Raw -> Bronze -> Silver -> Gold), diamond lineage patterns, multi-source ingestion, and anomaly detection.

What it does:
1. Creates an alert rule for `bronze_taxi_trips`
2. Writes ~15 normal metric data points for `bronze_taxi_trips`
3. Injects a 10x row count spike to trigger anomaly detection
4. Demonstrates multi-source ingestion (table, parquet, CSV)
5. Captures column lineage with all transform types (DIRECT, EXPRESSION, AGGREGATE, JOIN)

Verify results:

```bash
curl "$API_URL/anomalies?dataset_name=bronze_taxi_trips&hours=1"
curl "$API_URL/datasets/bronze_taxi_trips/metrics?hours=1"
curl "$API_URL/alert-history?hours=1"
curl "$API_URL/datasets/silver_trip_weather/lineage"
curl "$API_URL/datasets/gold_borough_metrics/column-lineage"
```

### run-schema-demo.sh -- Schema Evolution

```bash
./run-schema-demo.sh
```

Writes 5 progressive schema versions to `demo_customers` to demonstrate schema tracking:

| Version | Change | Breaking? |
|---------|--------|-----------|
| V1 | `id`, `name`, `age` (initial) | -- |
| V2 | Added `email`, `city` | No |
| V3 | `age` changed from INT to STRING | Yes |
| V4 | Removed `email` | Yes |
| V5 | `id` and `name` set to NOT NULL | Yes |

Verify results:

```bash
curl "$API_URL/datasets/demo_customers/schema/history"
curl "$API_URL/datasets/demo_customers/schema"
```

---

## 9. Troubleshooting

### "No anomalies detected"

Anomaly detection requires at least 5 historical data points for a dataset before it can compute meaningful statistics. Additionally, the spike must exceed 3 standard deviations from the mean. With small sample sizes, a single outlier inflates both the mean and standard deviation, making detection harder. For testing, write at least 15 normal data points before injecting a spike of 10x the normal value.

### "Alert not sent"

1. Verify an alert rule exists: `curl "$API_URL/alert-rules"`
2. Check that the rule's `dataset_name` pattern matches the anomaly's dataset
3. Check that the rule's `severity` matches the anomaly's severity
4. Check deduplication: if an alert was sent recently within the `deduplication_minutes` window, subsequent alerts are suppressed
5. Check alert history for errors: `curl "$API_URL/alert-history?hours=1"`

### "Listener not capturing data"

1. Verify the assembly JAR is on the classpath (`--jars` flag points to the correct file)
2. Verify the API URL is reachable from the Spark driver: `curl http://localhost:8000/health`
3. Check that `spark.extraListeners` is set to `com.observability.listener.ObservabilityListener`
4. Check Spark driver logs for `ObservabilityListener` or `MetadataPublisher` log messages
5. The publisher is asynchronous with a 2-thread pool. If the job exits immediately after a write, add a short sleep to allow the publisher to flush.

### "401 Unauthorized"

1. Verify the API key is correct and includes the `obs_live_` prefix
2. Check that the key has not been revoked
3. Check that the key has not expired (if `expires_in_days` was set)
4. Ensure the header format is exactly `Authorization: Bearer YOUR_KEY` (note the space after "Bearer")

### "Database disconnected"

1. Verify PostgreSQL is running: `docker ps` (if using Docker) or `pg_isready`
2. Check the health endpoint: `curl http://localhost:8000/health`
3. Verify database connection settings in `api/.env` (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)

### "Connection refused" from Spark to API

1. If running Spark locally and the API in Docker, use `host.docker.internal` instead of `localhost`
2. If running Spark on a cluster, ensure the API server is network-accessible from the driver node
3. Check firewall rules if the API is behind a VPN or security group
