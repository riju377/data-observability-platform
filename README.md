# Data Observability Platform

A production-grade data observability platform that automatically captures lineage, tracks schema evolution, detects anomalies, and alerts -- all without changing a single line of your Spark code.

## What It Does

| Capability | Description |
|---|---|
| **Lineage Tracking** | Table-level and column-level dependency graphs, built automatically from Spark logical plans |
| **Schema Evolution** | SCD Type 2 versioning of every schema change with full audit history |
| **Anomaly Detection** | Statistical process control (3-sigma) catches row count spikes/drops in real time |
| **Multi-Channel Alerting** | Email (Gmail/SMTP), Slack, and Microsoft Teams notifications on anomalies |
| **Impact Analysis** | Recursive CTEs answer "if I change this table, what breaks?" in <100ms |
| **Zero Configuration** | Just add `--conf spark.extraListeners=...` to your spark-submit -- done |

## Architecture

```
                        spark-submit --conf spark.extraListeners=...
                                         |
                     +-------------------v--------------------+
                     |        Apache Spark Job                |
                     |  ObservabilityListener (transparent)   |
                     |   - Parses logical plans for lineage   |
                     |   - Extracts schemas from StructType   |
                     |   - Collects row counts & metrics      |
                     +-------------------+--------------------+
                                         |
                           MetadataPublisher (async, fire-and-forget)
                                         |
                            POST /api/v1/ingest/metadata
                            (Bearer token auth)
                                         |
                     +-------------------v--------------------+
                     |         FastAPI Backend (Python)       |
                     |   - Upserts datasets & lineage edges   |
                     |   - Stores schemas (SCD Type 2)        |
                     |   - Records metrics time-series        |
                     |   - Runs anomaly detection (3-sigma)   |
                     |   - Dispatches alerts (Email/Slack/Teams)|
                     +-------------------+--------------------+
                                         |
                              PostgreSQL (single DB)
                              14 tables, recursive CTEs,
                              JSONB, optimized indexes
                                         |
                     +-------------------v--------------------+
                     |         React Frontend (Vite)          |
                     |   - ReactFlow lineage visualization    |
                     |   - Recharts metrics dashboards        |
                     |   - Schema history browser             |
                     |   - Anomaly & alert management         |
                     +----------------------------------------+
```

## Quick Start

### Prerequisites

- JDK 11+, Scala 2.12, SBT 1.9+
- Python 3.9+, Node.js 18+
- Docker (for PostgreSQL)
- Apache Spark 3.5 (for running demos)

### 1. Start the database

```bash
docker compose up -d postgres
```

### 2. Build the Spark listener JAR

```bash
sbt assembly
# Output: target/scala-2.12/data-observability-platform-assembly-1.1.0.jar
```

### 3. Start the API server

```bash
cd api
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python main.py
# API at http://localhost:8000, docs at http://localhost:8000/docs
```

### 4. Start the frontend

```bash
cd client
npm install && npm run dev
# UI at http://localhost:3000
```

### 5. Run a demo

```bash
export API_KEY="obs_live_DEMO_KEY_FOR_TESTING_ONLY"

# Quick smoke test (lineage + schema)
./run-quick-test.sh

# Full pipeline with anomaly detection + alerting
./run-demo.sh

# Schema evolution (5 versions)
./run-schema-demo.sh
```

## Project Structure

```
data-observability-platform/
|-- src/main/scala/com/observability/
|   |-- listener/        ObservabilityListener.scala  (SparkListener + QueryExecutionListener)
|   |-- lineage/         QueryPlanParser, ColumnLineageExtractor
|   |-- schema/          SchemaTracker, SchemaComparator
|   |-- metrics/         MetricsCollector
|   |-- publisher/       MetadataPublisher (async HTTP to API)
|   |-- client/          ObservabilityApiClient (HTTP client)
|   +-- examples/        QuickLineageTest, FullPipelineDemo, SchemaEvolutionDemo
|
|-- api/                 FastAPI backend
|   |-- main.py          App entry + all REST endpoints
|   |-- auth.py          JWT + API key authentication
|   |-- alerting.py      Email/Slack/Teams providers (strategy pattern)
|   |-- services/        anomaly_service.py, alert_service.py
|   +-- routers/         ingest.py (metadata ingestion), auth.py
|
|-- client/              React + Vite frontend
|   +-- src/             Components, pages, services
|
|-- scripts/             SQL scripts (init-db.sql, schema_master.sql, migrations)
|-- docs/                Consolidated documentation (4 files)
|-- docker-compose.yml   PostgreSQL, Jupyter, MinIO
+-- build.sbt            Scala build with Maven Central publishing
```

## Documentation

| Document | Audience | Description |
|---|---|---|
| [Architecture Deep-Dive](docs/ARCHITECTURE.md) | Everyone | Full system internals, data flow, database schema, all architectural decisions |
| [User Guide](docs/USER-GUIDE.md) | End Users | Authentication, spark-submit configuration, API usage, alerting setup |
| [Developer Guide](docs/DEVELOPER-GUIDE.md) | Contributors | Local setup, database credentials, build process, deployment, contributing |
| [Interview Guide](docs/INTERVIEW-GUIDE.md) | Author | Technical decisions, interview Q&A, comparisons with alternatives |
| [API Reference](api/README.md) | API Users | All REST endpoints with request/response examples |

## Technology Stack

| Layer | Technology | Version |
|---|---|---|
| Spark Listener | Scala, Apache Spark | 2.12, 3.5.0 |
| Backend API | Python, FastAPI, Pydantic | 3.14, latest |
| Database | PostgreSQL | 16 |
| Frontend | React, ReactFlow, Recharts, Vite | 19, 11, 3, 7 |
| Auth | JWT (PyJWT), bcrypt, API keys | - |
| Alerting | SMTP (Gmail), Slack webhooks, Teams webhooks | - |
| Build | SBT (assembly JAR), npm, pip | 1.9+ |
| Infrastructure | Docker Compose | - |

## Why This Project?

Modern data teams face three critical problems:

1. **No lineage visibility** -- "If I change this table, what breaks?" requires tribal knowledge
2. **Late issue detection** -- Data quality problems found days later when users complain
3. **No schema audit trail** -- "What was the schema last Tuesday?" has no answer

Existing solutions either cost $50K+/year (Monte Carlo, Datadog) or require manual instrumentation (Great Expectations). This platform provides **automated, zero-config observability** by hooking into Spark's execution engine directly.

See [Interview Guide](docs/INTERVIEW-GUIDE.md) for detailed comparisons with alternatives.

## License

Apache 2.0
