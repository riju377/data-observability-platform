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

## Usage

### Option 1: Try the Live Demo

The platform is deployed and ready to use:

🌐 **Dashboard**: [https://data-observability-platform.vercel.app](https://data-observability-platform.vercel.app)

**Quick start with your Spark jobs:**

1. **Create an API key** via the dashboard (login required - free account)
2. **Add the package** to your spark-submit command:
   ```bash
   spark-submit \
     --packages io.github.riju377:data-observability-platform_2.12:1.4.0 \
     --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
     --conf spark.sql.queryExecutionListeners=com.observability.listener.ObservabilityListener \
     --conf spark.observability.api.key=<YOUR_API_KEY> \
     your-application.jar
   ```
   
   **Optional Configuration:**
   
   | Property | Description | Default |
   |---|---|---|
   | `spark.observability.job.name` | Custom name for your job (overrides Spark app name) | (Spark App Name) |
   | `spark.observability.api.connectTimeoutMs` | Connection timeout in milliseconds | 5000 |
   | `spark.observability.api.readTimeoutMs` | Read timeout in milliseconds | 30000 |
   
   **Optional Configuration:**
   
   | Property | Description | Default |
   |---|---|---|
   | `spark.observability.job.name` | Custom name for your job (overrides Spark app name) | (Spark App Name) |
   | `spark.observability.api.connectTimeoutMs` | Connection timeout in milliseconds | 5000 |
   | `spark.observability.api.readTimeoutMs` | Read timeout in milliseconds | 30000 |
3. **View results** - Lineage, schemas, and anomalies appear automatically in the dashboard

**Note:** The API URL is pre-configured for the hosted version. Just add your API key and you're ready to go!

---

### Option 2: Self-Hosted Deployment

Deploy the full platform on your own infrastructure.

**Prerequisites:**
- JDK 11+, Scala 2.12, SBT 1.9+
- Python 3.9+, Node.js 18+
- Docker & Docker Compose
- Apache Spark 3.5+ (for your data pipelines)
- PostgreSQL 16 (via Docker or managed service)

**Step 1: Clone the Repository**

```bash
git clone https://github.com/riju377/data-observability-platform.git
cd data-observability-platform
```

**Step 2: Start the Database**

```bash
# Using Docker Compose (recommended for local dev)
docker compose up -d postgres

# Or use your own PostgreSQL instance and update api/database.py
```

**Step 3: Initialize the Database**

```bash
# Connect to PostgreSQL and run the schema
docker exec -i <postgres-container-id> psql -U postgres -d observability < scripts/init-db.sql
```

**Step 4: Configure Environment Variables**

```bash
# Create .env file in api/ directory
cat > api/.env << EOF
DATABASE_HOST=localhost
DATABASE_PORT=5432
DATABASE_NAME=observability
DATABASE_USER=postgres
DATABASE_PASSWORD=postgres
JWT_SECRET_KEY=$(openssl rand -hex 32)
API_PORT=8000
EOF
```

**Step 5: Start the API Backend**

```bash
cd api
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
python main.py
# API running at http://localhost:8000
# Swagger docs at http://localhost:8000/docs
```

**Step 6: Build the Spark Listener JAR**

```bash
sbt assembly
# Output: target/scala-2.12/data-observability-platform-assembly-1.4.0.jar
```

**Step 7: Start the Frontend (Optional)**

```bash
cd client
npm install
npm run dev
# Dashboard at http://localhost:5173
```

**Step 8: Create API Key for Spark Jobs**

```bash
# Register a user and organization via API
curl -X POST http://localhost:8000/auth/register \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@yourcompany.com",
    "password": "secure_password",
    "full_name": "Admin User",
    "organization_name": "Your Company"
  }'

# Login to get JWT token
curl -X POST http://localhost:8000/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "admin@yourcompany.com",
    "password": "secure_password"
  }'

# Create API key (use JWT token from login)
curl -X POST http://localhost:8000/api-keys \
  -H "Authorization: Bearer <JWT_TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Spark Production",
    "scopes": ["ingest"]
  }'
```

**Step 9: Configure Your Spark Jobs**

Add the listener to your Spark applications:

```bash
spark-submit \
  --jars /path/to/data-observability-platform-assembly-1.4.0.jar \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf spark.sql.queryExecutionListeners=com.observability.listener.ObservabilityListener \
  --conf spark.observability.api.url=http://your-api-host:8000 \
  --conf spark.observability.api.key=obs_live_YOUR_API_KEY \
  your-application.jar
```

**Step 10: Verify Data Flow**

1. Run a Spark job with the listener configured
2. Check API logs for incoming metadata: `POST /api/v1/ingest/metadata`
3. View lineage in dashboard: `http://localhost:5173`
4. Query via API: `http://localhost:8000/docs`

---

## Production Deployment

For production environments:

**Backend (FastAPI):**
- Deploy to AWS ECS, Google Cloud Run, or Kubernetes
- Use managed PostgreSQL (AWS RDS, Google Cloud SQL, Azure Database)
- Set environment variables via secrets manager
- Enable HTTPS with SSL certificates
- Configure CORS with specific origins (remove wildcard)

**Frontend (React):**
- Deploy to Vercel, Netlify, or S3 + CloudFront
- Update API base URL in environment config

**Database:**
- Use connection pooling (PgBouncer recommended)
- Enable automated backups
- Monitor query performance
- Consider read replicas for query endpoints

**Spark Listener JAR:**
- Upload to S3/GCS bucket for easy distribution
- Reference in spark-submit via `--jars s3://bucket/path/to/jar`
- Publish to Maven Central for dependency management

---

## Demo Examples

Run example Spark jobs to see the platform in action:

```bash
export API_KEY="obs_live_YOUR_API_KEY"
export API_URL="http://localhost:8000"

# Quick lineage test (creates bronze -> silver -> gold pipeline)
./run-quick-test.sh

# Full pipeline with anomaly detection
./run-demo.sh

# Schema evolution demo (5 version changes)
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

## License

Apache 2.0
