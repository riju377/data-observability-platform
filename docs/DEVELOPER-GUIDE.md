# Data Observability Platform -- Developer Guide

This guide covers local setup, project structure, contribution workflow, and deployment for the Data Observability Platform. It is written for developers who want to build, run, extend, or deploy the system.

The platform has three main components:

- **Scala Spark Listener** -- attaches to Apache Spark and captures lineage, schema, and metrics automatically.
- **Python FastAPI Backend** -- REST API that ingests metadata, stores it in PostgreSQL, runs anomaly detection, and dispatches alerts.
- **React Frontend** -- dashboard for exploring lineage graphs, schema history, metrics, and anomalies.

---

## 1. Prerequisites

Install the following before proceeding:

| Tool | Version | Notes |
|---|---|---|
| JDK | 11+ (recommend 11 or 17) | Required for Scala/SBT compilation |
| SBT | 1.9+ | Install via SDKMAN: `sdk install sbt` |
| Python | 3.9+ (developed on 3.14) | For the FastAPI backend |
| Node.js | 18+ | For the React frontend |
| Docker & Docker Compose | Latest stable | For PostgreSQL and optional services |
| Apache Spark | 3.5 | For running demos; `brew install apache-spark` or download from https://spark.apache.org |
| Git | Any recent version | Source control |

---

## 2. Initial Setup

Follow these steps in order to get the full stack running locally.

### Clone the repository

```bash
git clone https://github.com/riju377/data-observability-platform.git
cd data-observability-platform
```

### Start PostgreSQL

```bash
docker compose up -d postgres
```

This starts PostgreSQL 16 on port 5432 and automatically runs `scripts/init-db.sql` to create the schema. A demo API key is seeded: `obs_live_DEMO_KEY_FOR_TESTING_ONLY` under the default organization (`Default Organization`, UUID `00000000-0000-0000-0000-000000000001`).

### Build the Spark listener

```bash
sbt assembly
```

This compiles the Scala code and produces a fat JAR at:

```
target/scala-2.12/data-observability-platform-assembly-1.1.0.jar
```

Spark itself is marked as `provided` and is not bundled into the JAR.

### Set up the Python API

```bash
cd api
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Configure the database connection. Either edit `api/database.py` directly or set environment variables:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_NAME=lineage_db
export DB_USER=observability
export DB_PASSWORD=observability123
```

Start the API server:

```bash
python main.py
```

The API runs on **http://localhost:8000**. Interactive API docs are available at **http://localhost:8000/docs** (Swagger UI).

### Set up the frontend

```bash
cd ../client
npm install
npm run dev
```

The frontend runs on **http://localhost:3000**.

---

## 3. Using Your Own Database (Not Docker)

You do not need Docker for PostgreSQL. Any PostgreSQL 16 instance works, including managed services:

- **Aiven for PostgreSQL**
- **AWS RDS**
- **Google Cloud SQL**
- **Azure Database for PostgreSQL**

Setup steps:

1. Create a database (e.g., `lineage_db`).
2. Run `scripts/schema_master.sql` against it to create all 14 tables.
3. Update the connection settings in `api/database.py` or set the `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD` environment variables.

No special PostgreSQL extensions are required beyond `uuid-ossp` and `pgcrypto`, both of which are standard and available on all managed PostgreSQL providers. There is no dependency on pgvector or any other exotic extension.

---

## 4. Project Structure

### Scala Spark Listener (`src/main/scala/com/observability/`)

| Directory / File | Purpose |
|---|---|
| `listener/ObservabilityListener.scala` | Main Spark listener. Hooks into `onJobEnd` and `onSuccess` to capture execution metadata. Filters out flush/self-reads (inputs matching output paths) and checkpoint/staging paths. |
| `lineage/QueryPlanParser.scala` | Parses Spark logical plans to extract dataset-level lineage (sources and sinks). Normalizes partition paths (YYYYMMDD, YYYYMM, date ranges, country codes). Uses `bucket:logical_name` composite naming for path-based datasets via `extractBucketFromPath()`. Filters flush/self-reads and checkpoint paths. |
| `lineage/ColumnLineageExtractor.scala` | Extracts column-level lineage from query plans. |
| `schema/SchemaTracker.scala` | Records schema snapshots for datasets. |
| `schema/SchemaComparator.scala` | Detects schema changes between snapshots. |
| `metrics/MetricsCollector.scala` | Collects row counts, byte sizes, duration, and other job metrics. |
| `publisher/MetadataPublisher.scala` | Async HTTP publisher. Runs on a 2-thread pool, batches metadata, and sends to the API. |
| `client/ObservabilityApiClient.scala` | HTTP client that POSTs metadata to `POST /api/v1/ingest/metadata`. |
| `examples/` | Three demo Spark applications. These have **zero** `com.observability.*` imports -- the listener is registered transparently via `spark.extraListeners` in spark-submit. |

### Python FastAPI Backend (`api/`)

| File | Purpose |
|---|---|
| `main.py` | FastAPI application entry point. Defines all REST endpoints and CORS configuration. |
| `auth.py` | JWT token authentication and API key validation. |
| `database.py` | PostgreSQL connection pooling using psycopg. |
| `models.py` | Pydantic request/response models. |
| `alerting.py` | Alert dispatch with strategy pattern. Includes email, Slack, and Microsoft Teams providers. |
| `services/anomaly_service.py` | 3-sigma anomaly detection. Requires at least 5 data points. |
| `services/alert_service.py` | Matches alert rules to detected anomalies and dispatches notifications. |
| `routers/ingest.py` | Metadata ingestion endpoint (`POST /api/v1/ingest/metadata`). Processes incoming data in the background: upserts datasets, lineage edges, and metrics, then triggers anomaly detection. `infer_dataset_type()` checks location patterns (S3, HDFS, etc.) before table type, so path-based datasets get accurate storage types. |
| `routers/auth.py` | Authentication endpoints: register, login, API key management. |
| `.env` | Email alerting configuration (see Configuration Reference below). |

### React Frontend (`client/`)

| Path | Purpose |
|---|---|
| `client/src/` | React source code. |
| Key dependencies | React 19.2, React Router 7.13, ReactFlow 11.11 (lineage graphs), dagre (automatic graph layout), Recharts 3.7 (charts), Axios 1.13, Lucide React (icons), Vite 7.2. |

### Scripts and Configuration

| File | Purpose |
|---|---|
| `scripts/init-db.sql` | Initial schema, auto-run by Docker on first start (mounted to `/docker-entrypoint-initdb.d/`). |
| `scripts/schema_master.sql` | Master reference containing all 14 tables. Use this when setting up a non-Docker database. |
| `api/migrations/` | Incremental migration files for schema changes. |
| `docker-compose.yml` | Defines postgres, jupyter, and minio services. |
| `build.sbt` | SBT build definition. Organization: `io.github.riju377`, version `1.1.0`, Scala 2.12.18, Spark 3.5.0. |
| `run-demo.sh` | Shell script to run the demo Spark application. |

---

## 5. Build Commands

### Scala

```bash
sbt compile          # Compile source code
sbt assembly         # Build fat JAR for spark-submit
sbt test             # Run ScalaTest + Mockito test suite
sbt clean            # Remove all build artifacts
```

### Python API

```bash
cd api
source venv/bin/activate
python main.py       # Start the API server on port 8000
```

There is no automated test suite for the API yet.

### Frontend

```bash
cd client
npm run dev          # Start Vite dev server on port 3000
npm run build        # Production build (outputs to dist/)
npm run lint         # Run ESLint
```

---

## 6. Configuration Reference

### Spark Listener (spark-submit --conf)

| Config Key | Description | Default |
|---|---|---|
| `spark.observability.api.url` | Backend API URL | `http://localhost:8000` |
| `spark.observability.api.key` | API key for authentication | *(required)* |
| `spark.extraListeners` | Must include `com.observability.listener.ObservabilityListener` | *(required)* |

Example spark-submit invocation:

```bash
spark-submit \
  --class your.MainClass \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf spark.observability.api.url=http://localhost:8000 \
  --conf spark.observability.api.key=obs_live_DEMO_KEY_FOR_TESTING_ONLY \
  --jars target/scala-2.12/data-observability-platform-assembly-1.1.0.jar \
  your-application.jar
```

### API Environment Variables

| Variable | Description | Default |
|---|---|---|
| `DB_HOST` | PostgreSQL host | `localhost` |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `lineage_db` |
| `DB_USER` | Database user | `observability` |
| `DB_PASSWORD` | Database password | `observability123` |

### Email Alerting (.env in api/)

| Variable | Description | Options / Example |
|---|---|---|
| `EMAIL_BACKEND` | Email delivery method | `console`, `gmail`, `smtp` |
| `EMAIL_HOST` | SMTP server | `smtp.gmail.com` |
| `EMAIL_PORT` | SMTP port | `587` |
| `EMAIL_USE_TLS` | Use TLS encryption | `true` |
| `EMAIL_USERNAME` | SMTP username | `your-email@gmail.com` |
| `EMAIL_PASSWORD` | SMTP password or app password | 16-character app password for Gmail |
| `EMAIL_FROM` | Sender display name and address | `Data Observability Platform <your-email@gmail.com>` |

For Gmail, you must generate an App Password (Google Account > Security > 2-Step Verification > App passwords). Regular account passwords will not work.

---

## 7. Adding New Features

### Adding a new API endpoint

1. Define Pydantic request/response models in `api/models.py`.
2. Add the endpoint function in `api/main.py` (or in a new router under `api/routers/`).
3. Write SQL queries using `execute_query` or `execute_single` from `api/database.py`.
4. Test with curl or the Swagger UI at `http://localhost:8000/docs`.

### Adding a new alert channel

1. Create a new provider class in `api/alerting.py` that extends `BaseAlertProvider`.
2. Implement the `send_alert(anomaly, config)` method.
3. Register the new provider in `AlertFactory.PROVIDERS` dict.
4. Create an alert rule with the new `channel_type` to use it.

### Frontend utilities for composite dataset names

Datasets that originate from file paths use a `bucket:logical_name` naming convention. The frontend provides a `parseDatasetName(name)` helper that splits this into `{ bucket, displayName }`. When adding new pages or components that display dataset names, use this helper so the UI shows the short display name with a bucket badge. It is already used in the Lineage, ColumnLineage, and Schema pages.

### Adding new metadata capture in the Spark listener

1. Extract the data you need in `ObservabilityListener`'s `onSuccess` or `onJobEnd` method.
2. Add the data to the metadata JSON payload in `MetadataPublisher`.
3. Handle the new fields in `api/routers/ingest.py` inside the `process_metadata()` function.
4. Add database tables or columns if needed (update `scripts/schema_master.sql` and create a migration).

---

## 8. Database Migrations

The project uses manual SQL scripts for schema management. There is no automated migration tool (e.g., Alembic or Flyway) at this time.

### Key files

- `scripts/init-db.sql` -- initial schema, auto-run by Docker on first start.
- `scripts/schema_master.sql` -- complete current schema with all 14 tables. This is the single source of truth.
- `api/migrations/` -- incremental migration files for changes applied after initial setup.

### Adding a new table

1. Add the `CREATE TABLE` statement to `scripts/schema_master.sql`.
2. Create a new migration file in `api/migrations/` with the incremental SQL.
3. Run the migration manually against your database:

```bash
psql -h localhost -U observability -d lineage_db -f api/migrations/your_migration.sql
```

---

## 9. Deployment

### Option A: Docker Compose (development)

```bash
docker compose up -d
```

This starts three services:

| Service | Port | Purpose |
|---|---|---|
| `postgres` | 5432 | PostgreSQL 16 (Alpine), user `observability`, password `observability123`, database `lineage_db` |
| `jupyter` | 8888 | PySpark notebook for interactive testing |
| `minio` | 9000, 9001 | S3-compatible object storage, user `minioadmin`, password `minioadmin123` |

The API and frontend are started manually (see Initial Setup above).

### Option B: Cloud deployment

- **PostgreSQL**: Use a managed service (Aiven, RDS, Cloud SQL, Azure Database).
- **API**: Deploy as a Docker container or on any Python-capable platform (Railway, Render, EC2, ECS).
- **Frontend**: Run `npm run build` in the `client/` directory, then deploy the `dist/` folder to Vercel, Netlify, or an S3 bucket with CloudFront.
- **Spark Listener**: Distribute the assembly JAR to your Spark clusters. Configure via `spark-submit --conf` flags.

### Production checklist

- [ ] Set up managed PostgreSQL with SSL enabled.
- [ ] Run `scripts/schema_master.sql` on the production database.
- [ ] Configure the API with production database credentials (not the Docker defaults).
- [ ] Set `EMAIL_BACKEND=gmail` or `EMAIL_BACKEND=smtp` with real credentials.
- [ ] Deploy the API behind a reverse proxy (nginx) with HTTPS.
- [ ] Build the frontend with the correct `API_BASE_URL` pointing to the production API.
- [ ] Distribute the assembly JAR to all Spark clusters.
- [ ] Create production API keys (do not use the demo key `obs_live_DEMO_KEY_FOR_TESTING_ONLY`).
- [ ] Set CORS origins in `api/main.py` to specific domains instead of `"*"`.

### Option C: Maven Central (distribute JAR only)

The assembly JAR is published to Maven Central under:

```
io.github.riju377:data-observability-platform_2.12:1.1.0
```

Users can add it as a dependency or download the JAR directly. They configure the listener entirely through `spark-submit --conf` flags and need their own API server running to receive metadata.

---

## 10. Troubleshooting

**sbt compile fails**
Check your JDK version (`java -version`). JDK 11 or 17 is required. Scala 2.12.18 does not support JDK 21+. Also ensure SBT 1.9+ is installed (`sbt --version`).

**Import errors in IDE (IntelliJ, VS Code)**
Reload the SBT project. In IntelliJ: right-click `build.sbt` > Reload. Ensure Spark 3.5 libraries are resolved on the classpath.

**Database connection refused**
Verify Docker is running and the postgres container is up:

```bash
docker ps
```

Check that port 5432 is not in use by another process:

```bash
lsof -i :5432
```

**API cannot connect to the database**
Double-check the values of `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD`. If running PostgreSQL in Docker, `DB_HOST` should be `localhost` (not the container name, unless the API is also in Docker).

**Frontend cannot reach the API**
Check the CORS configuration in `api/main.py`. Ensure the API is running on port 8000 and the frontend is configured to make requests to `http://localhost:8000`.

**Email alerts not sending**
Check `EMAIL_BACKEND` in `api/.env`. If set to `console`, alerts are printed to stdout instead of sent. For Gmail, verify you are using a 16-character App Password, not your regular account password.

**"from alerting import" error in IDE**
This is a false positive. `alerting.py` lives in the same directory as the files that import it. The import works correctly at runtime; IDEs sometimes fail to resolve sibling imports in the absence of an `__init__.py` or proper project root configuration.

**Assembly JAR is large (~30MB)**
This is expected. Spark itself is marked as `provided` and is not included. The size comes from Circe (JSON), the PostgreSQL driver (legacy), scalaj-http, and other runtime dependencies.

**Thread.sleep in example code**
The `MetadataPublisher` runs asynchronously on a background thread pool. Without a sleep before `spark.stop()`, the Spark session shuts down before the publisher finishes sending metadata to the API. This is only relevant in short-lived batch jobs and examples.

---

## Quick Reference

```bash
# Full stack startup (from project root)
docker compose up -d postgres
sbt assembly
cd api && source venv/bin/activate && python main.py &
cd client && npm run dev &

# Run a demo Spark job
export API_URL=http://localhost:8000
export API_KEY=obs_live_DEMO_KEY_FOR_TESTING_ONLY
export JAR_PATH=target/scala-2.12/data-observability-platform-assembly-1.1.0.jar
./run-demo.sh

# Check API health
curl http://localhost:8000/docs

# Connect to local database
psql -h localhost -U observability -d lineage_db
# Password: observability123
```
