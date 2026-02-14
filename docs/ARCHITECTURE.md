# Data Observability Platform -- Architecture Document

## Table of Contents

1. [System Overview](#1-system-overview)
2. [Component Deep-Dive](#2-component-deep-dive)
3. [Database Schema](#3-database-schema)
4. [Data Flow](#4-data-flow)
5. [Architectural Decisions](#5-architectural-decisions)
6. [Anomaly Detection Mathematics](#6-anomaly-detection-mathematics)
7. [Alerting Pipeline](#7-alerting-pipeline)
8. [Performance Characteristics](#8-performance-characteristics)
9. [Scaling Considerations](#9-scaling-considerations)

---

## 1. System Overview

The Data Observability Platform is a three-tier system that transparently captures metadata from Apache Spark jobs and surfaces it through a REST API and interactive dashboard. It tracks data lineage (table-level and column-level), schema evolution, dataset metrics, anomalies, and alerts -- all without requiring changes to existing Spark application code.

### High-Level Architecture

```
+---------------------------------------------------------------+
|                        SPARK CLUSTER                          |
|                                                               |
|  +------------------+   +------------------+   +-----------+  |
|  | Your Spark Job   |   | ObservabilityListener           |  |
|  | (unchanged code) |-->| (SparkListener + QueryExecution) |  |
|  +------------------+   +------------------+               |  |
|                              |                              |  |
|                              v                              |  |
|                     +-------------------+                   |  |
|                     | MetadataPublisher |                   |  |
|                     | (async, 2 threads)|                   |  |
|                     +-------------------+                   |  |
|                              |                              |  |
|                              | HTTP POST (fire-and-forget)  |  |
+------------------------------|--------------------------------+
                               |
                               | Bearer token auth
                               v
+---------------------------------------------------------------+
|                      FASTAPI BACKEND                          |
|                                                               |
|  +-------------------+   +------------------+                 |
|  | POST /api/v1/     |-->| process_metadata |                 |
|  |   ingest/metadata  |   | (background)     |                 |
|  +-------------------+   +--------+---------+                 |
|                                   |                           |
|                    +--------------+--------------+            |
|                    |              |              |             |
|                    v              v              v             |
|              +---------+  +----------+  +------------+        |
|              | Upsert  |  | Anomaly  |  |  Alert     |        |
|              | datasets|  | Service  |  |  Service   |        |
|              | lineage |  | (3-sigma)|  | (dispatch) |        |
|              | metrics |  +----------+  +------------+        |
|              | schema  |         |              |              |
|              +---------+         v              v              |
|                    |       +----------+  +-----------+        |
|                    +------>|PostgreSQL|<--| Email     |        |
|                            |   16    |   | Slack     |        |
|                            +----------+  | Teams     |        |
|                                          +-----------+        |
+---------------------------------------------------------------+
                               |
                               | REST API (JWT / API key)
                               v
+---------------------------------------------------------------+
|                     REACT FRONTEND                            |
|                                                               |
|  +----------+ +-----------+ +--------+ +---------+ +-------+ |
|  | Dashboard| | Lineage   | | Schema | | Anomaly | | Alerts| |
|  | (stats)  | | (ReactFlow)| | (diff) | | (list)  | | (CRUD)| |
|  +----------+ +-----------+ +--------+ +---------+ +-------+ |
|                                                               |
|  React 19 + Vite 7 + ReactFlow 11 + Recharts 3               |
+---------------------------------------------------------------+
```

### Data Flow Summary

1. A Spark job executes. The `ObservabilityListener` intercepts events via `spark.extraListeners` configuration -- no code changes required in the Spark application.
2. The listener extracts lineage (input/output tables), column-level lineage, schema snapshots, and execution metrics from Spark's logical plans and stage accumulators.
3. `MetadataPublisher` sends all captured data asynchronously to the backend API via HTTP POST with Bearer token authentication.
4. The FastAPI backend processes the payload in a background task: upserting datasets, lineage edges, column lineage, schema versions (SCD Type 2), and metrics, then running anomaly detection and alert dispatch.
5. The React frontend queries the API and renders interactive lineage DAGs, metrics charts, schema history, anomaly lists, and alert management.

### Technology Stack

| Layer | Technology | Version |
|-------|-----------|---------|
| Listener | Scala, Apache Spark | 2.12.18, 3.5.0 |
| Backend API | Python, FastAPI, Pydantic, psycopg3, Jinja2 | 3.14 |
| Frontend | React, ReactFlow, dagre, Recharts, Vite, Axios, Lucide | 19, 11, -, 3, 7 |
| Database | PostgreSQL | 16 |
| Infrastructure | Docker Compose (PostgreSQL, Jupyter, MinIO) | -- |
| Build | sbt (Scala), npm (frontend), pip (API) | -- |

---

## 2. Component Deep-Dive

### 2.1 Spark Listener Layer

**Source:** `src/main/scala/com/observability/`

The listener layer is designed for zero-impact deployment. It hooks into Spark transparently via configuration:

```bash
spark-submit \
  --conf spark.extraListeners=com.observability.listener.ObservabilityListener \
  --conf spark.observability.api.url=https://api.example.com \
  --conf spark.observability.api.key=obs_live_xxx \
  your-spark-job.jar
```

No `com.observability.*` imports are needed in application code.

#### ObservabilityListener

**File:** `src/main/scala/com/observability/listener/ObservabilityListener.scala`

Extends both `SparkListener` and `QueryExecutionListener` to capture metadata at two levels:

- **SparkListener hooks** (low-level job/stage events):
  - `onApplicationStart` -- triggers self-registration as `QueryExecutionListener`
  - `onJobStart` -- creates `JobMetadata`, maps stage IDs to job IDs, initializes lineage tracking
  - `onStageCompleted` -- extracts metrics from stage accumulators (I/O bytes, shuffle, CPU, memory, spill), accumulates per-job
  - `onJobEnd` -- finalizes metrics, publishes complete job metadata
  - `onApplicationEnd` -- drains the async publisher (30-second timeout)

- **QueryExecutionListener hooks** (high-level SQL events):
  - `onSuccess` -- receives the `QueryExecution` object containing the logical plan; extracts table lineage, column lineage, and schema snapshots; publishes each immediately
  - `onFailure` -- logs query failures

Self-registration as `QueryExecutionListener` is handled via reflection in `ensureQueryListenerRegistered()`, attempting `getActiveSession`, then `getDefaultSession`, because `spark.sql.queryExecutionListeners` config does not work reliably for programmatic registration.

**Flush/self-read filtering:** The listener filters out inputs that match the output paths of the same query (flush reads), as well as checkpoint and staging paths (e.g., `_spark_metadata`, `.spark-staging`). This prevents spurious lineage edges where Spark reads back data it just wrote as part of its internal commit protocol.

**Key tracking maps:**

```scala
private val activeJobs    = mutable.Map[Int, JobMetadata]()       // jobId -> metadata
private val stageToJob    = mutable.Map[Int, Int]()               // stageId -> jobId
private val jobMetrics    = mutable.Map[Int, JobMetrics]()        // jobId -> aggregated
private val jobLineage    = mutable.Map[Int, (Seq[...], Seq[...])]() // inputs, outputs
```

#### QueryPlanParser

**File:** `src/main/scala/com/observability/lineage/QueryPlanParser.scala`

Performs recursive depth-first traversal of Spark's `LogicalPlan` tree to extract input and output tables.

**Input table extraction** (what the job reads):
- `SubqueryAlias` nodes -- most common for table reads; extracts table name via reflection on `identifier.name`
- `LogicalRelation` -- `DataSource` tables; checks `catalogTable` for catalog tables, falls back to `HadoopFsRelation` path extraction for file-based reads
- `DataSourceV2Relation` -- Iceberg, Delta Lake, etc.; accesses `table.name()` via reflection
- Special handling for JDBC, Snowflake, BigQuery, and Redshift relations via regex matching on the relation's string representation

**Output table extraction** (what the job writes):
- `CreateDataSourceTableAsSelectCommand` -- `CREATE TABLE AS SELECT`; parses table name using regex on backtick-quoted identifiers
- `InsertIntoHadoopFsRelationCommand` -- file-based writes (Parquet, Avro, ORC, CSV, JSON); extracts file path using URI pattern matching
- `SaveIntoDataSourceCommand` -- `DataFrame.save()` operations

**Partition path normalization:** Partitioned paths like `s3://bucket/data/date=2024-01-01/country=US/` are normalized to the logical dataset name `data`, stripping Hive-style partition segments and the following recognized patterns:

| Pattern | Example | Regex |
|---------|---------|-------|
| ISO date | `2024-01-01` | `\d{4}-\d{2}-\d{2}` |
| Compact date (YYYYMMDD) | `20240101` | `\d{8}` |
| Year-month (YYYYMM) | `202401` | `\d{6}` |
| Date range | `20240101-20240131` | `\d{8}-\d{8}` |
| 2-letter uppercase country | `US`, `IN` | `[A-Z]{2}` |
| 2-letter lowercase country | `us`, `in` | `[a-z]{2}` |
| Environment markers | `prod`, `staging` | Literal match |

**Composite dataset naming:** Path-based datasets use a `bucket:logical_name` format (e.g., `mw-device-profile:high_value_brand_propensity`) to preserve the S3 bucket or root container as context. The helper `extractBucketFromPath()` extracts the bucket component from the URI. Catalog tables (those with a `catalogTable` entry) retain their simple names without a bucket prefix.

**Path generalization:** Physical paths are generalized for location storage, replacing partition values with wildcards (e.g., `date=2024-01-01` becomes `date=*`). This is now handled in the backend (`ingest.py`) to ensure consistent `partition_key` generation for anomaly scoping.

#### ColumnLineageExtractor

**File:** `src/main/scala/com/observability/lineage/ColumnLineageExtractor.scala`

Extracts column-level lineage by analyzing Spark's expression trees within the logical plan.

**Process:**
1. Identify output tables (targets) and input tables (sources)
2. Build an `attributeOrigins` map: expression ID to (source table, column name) by traversing leaf nodes
3. Traverse `Project` and `Aggregate` nodes, analyzing each `NamedExpression` to find source column references

**Transformation types detected:**

| Type | Description | Example |
|------|-------------|---------|
| `DIRECT` | Column passes through unchanged | `SELECT col_a FROM ...` |
| `EXPRESSION` | Calculated from source columns | `col_a + col_b AS total` |
| `AGGREGATE` | Aggregation function | `SUM(amount) AS total_amount` |
| `JOIN` | Used as join key | `ON a.id = b.id` |
| `FILTER` | Used in WHERE/HAVING | `WHERE status = 'active'` |
| `CASE` | CASE WHEN expression | `CASE WHEN x > 0 THEN ...` |

Each edge produces a `ColumnLineageEdge(sourceDatasetName, sourceColumn, targetDatasetName, targetColumn, transformType, expression)`.

#### SchemaTracker

**File:** `src/main/scala/com/observability/schema/SchemaTracker.scala`

Captures schema snapshots from Spark's `StructType` and converts them to a simplified representation for storage.

**Schema hashing:** SHA-256 hash computed from canonical string `"name:type:nullable|name:type:nullable|..."`. Includes column names (order-sensitive), data types, and nullability. Excludes comments and metadata (those can change without breaking compatibility).

**Schema representation:**

```scala
case class SchemaField(name: String, dataType: String, nullable: Boolean, comment: Option[String])
case class SchemaSnapshot(fields: Seq[SchemaField], schemaHash: String, fieldCount: Int)
```

Schema is serialized to JSON for storage in PostgreSQL's `JSONB` column.

#### MetricsCollector

**File:** `src/main/scala/com/observability/metrics/MetricsCollector.scala`

Extracts dataset-level metrics from completed jobs:
- Row counts (`rowsWritten` from job metrics)
- Byte sizes (`bytesWritten`)
- Execution timestamps

The listener also captures comprehensive stage-level metrics from Spark's accumulators including shuffle read/write details, CPU time, GC time, memory/disk spill, and peak execution memory.

#### MetadataPublisher

**File:** `src/main/scala/com/observability/publisher/MetadataPublisher.scala`

Singleton object providing asynchronous, fire-and-forget publishing to the backend API.

**Thread pool:** 2-thread `ExecutorService` -- sufficient for HTTP fire-and-forget since the API returns immediately.

**Published data types:**
- `publishLineageOnly()` -- called immediately from `onSuccess()` for table-level lineage
- `publish()` -- called from `onJobEnd()` for complete job metadata with metrics
- `publishSchemaVersion()` -- called from `onSuccess()` for schema snapshots
- `publishColumnLineage()` -- called from `onSuccess()` for column-level lineage edges

**Shutdown:** `shutdown()` is called from `onApplicationEnd()`. It initiates orderly shutdown of the executor service and waits up to 30 seconds for pending calls to drain.

**Client initialization:** Lazy, thread-safe initialization from SparkConf. The API client is created on first publish call and cached.

#### ObservabilityApiClient

**File:** `src/main/scala/com/observability/client/ObservabilityApiClient.scala`

HTTP client for the observability API using `java.net.HttpURLConnection`.

**Configuration (from SparkConf):**

| Key | Default | Description |
|-----|---------|-------------|
| `spark.observability.api.url` | (required) | API base URL |
| `spark.observability.api.key` | (required) | Bearer token (API key) |
| `spark.observability.api.enabled` | `true` | Enable/disable |
| `spark.observability.api.connectTimeoutMs` | `5000` | Connection timeout |
| `spark.observability.api.readTimeoutMs` | `10000` | Read timeout |

All API calls go to `POST /api/v1/ingest/metadata` or `POST /api/v1/ingest/schema` with JSON payloads and `Authorization: Bearer <key>` headers.

#### Data Models

**File:** `src/main/scala/com/observability/models/JobMetadata.scala`

Core model hierarchy:

```
JobMetadata
  +-- jobId, jobName, applicationId
  +-- startTime, endTime, status
  +-- inputTables: Seq[TableReference]
  +-- outputTables: Seq[TableReference]
  +-- metrics: JobMetrics
  +-- errorMessage

TableReference
  +-- name, tableType (Table|View|TempView|File|Stream)
  +-- location, schema: Option[SchemaInfo]

SchemaInfo
  +-- fields: Seq[FieldInfo(name, dataType, nullable, metadata)]
  +-- schemaHash (SHA-256)

JobMetrics
  +-- rowsRead/Written, bytesRead/Written
  +-- executionTimeMs, totalTasks, failedTasks
  +-- shuffleReadBytes/WriteBytes (basic + detailed)
  +-- executorCpuTimeNs, jvmGcTimeMs
  +-- memoryBytesSpilled, diskBytesSpilled, peakExecutionMemory

ColumnLineageEdge
  +-- sourceDatasetName, sourceColumn
  +-- targetDatasetName, targetColumn
  +-- transformType (DIRECT|EXPRESSION|AGGREGATE|JOIN|FILTER|CASE)
  +-- expression
```

### 2.2 Backend API Layer

**Source:** `api/`

#### FastAPI Application

**File:** `api/main.py`

The FastAPI app serves as the centralized API for both data ingestion (from Spark) and data querying (from the frontend).

**Middleware:**
- CORS middleware with wildcard origins (to be restricted in production)

**Routers:**
- `ingest_router` -- mounted at `/api/v1/ingest/` for Spark listener data ingestion
- `auth_router` -- mounted for user registration, login, and API key management

**API Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check with DB connectivity |
| `GET` | `/datasets` | List datasets (paginated, filtered by org) |
| `GET` | `/datasets/{name}` | Get dataset details |
| `GET` | `/datasets/{name}/upstream` | Recursive upstream dependencies |
| `GET` | `/datasets/{name}/downstream` | Recursive downstream dependencies |
| `GET` | `/datasets/{name}/lineage` | Complete lineage graph (upstream + downstream + edges) |
| `GET` | `/datasets/{name}/columns/{col}/upstream` | Column upstream lineage |
| `GET` | `/datasets/{name}/columns/{col}/downstream` | Column downstream lineage |
| `GET` | `/datasets/{name}/columns/{col}/lineage` | Complete column lineage graph |
| `GET` | `/datasets/{name}/columns/{col}/impact` | Impact analysis for column changes |
| `GET` | `/datasets/{name}/column-lineage` | All column edges for a dataset |
| `GET` | `/datasets/{name}/schema` | Current schema (valid_to IS NULL) |
| `GET` | `/datasets/{name}/schema/history` | Schema version history |
| `GET` | `/datasets/{name}/schema/diff` | Diff between two schema versions |
| `GET` | `/datasets/{name}/metrics` | Time-series metrics |
| `GET` | `/datasets/{name}/metrics/stats` | Statistical summary (mean, stddev, percentiles) |
| `GET` | `/anomalies` | List anomalies (filtered by dataset, severity, status) |
| `GET` | `/anomalies/{id}` | Get anomaly details |
| `GET` | `/anomalies/stats/summary` | Anomaly counts by severity, status, type |
| `POST` | `/alert-rules` | Create alert rule |
| `GET` | `/alert-rules` | List alert rules |
| `PUT` | `/alert-rules/{id}` | Update alert rule |
| `DELETE` | `/alert-rules/{id}` | Delete alert rule |
| `GET` | `/alert-history` | List alert dispatch history |
| `POST` | `/test-alert` | Send a test alert |
| `GET` | `/jobs` | List job executions |
| `GET` | `/jobs/{id}` | Get job execution details |
| `GET` | `/jobs/{id}/stages` | Get stage-level metrics for a job |
| `GET` | `/jobs/{id}/stages/{stage_id}` | Get specific stage metrics |
| `GET` | `/jobs/stats/summary` | Job execution summary statistics |
| `POST` | `/api/v1/ingest/metadata` | Ingest lineage + metrics from Spark |
| `POST` | `/api/v1/ingest/schema` | Ingest schema snapshots from Spark |

#### Authentication

**File:** `api/auth.py`

Dual authentication model:

1. **API keys** (for Spark listeners): Prefixed with `obs_live_`, hashed with SHA-256, stored with prefix-based lookup. The `key_prefix` (first 12 characters) enables quick identification. Keys are validated against the `api_keys` table checking `revoked = FALSE` and `expires_at`.

2. **JWT tokens** (for frontend users): HS256-signed tokens containing `user_id`, `org_id`, `email`, and `role`. Configurable expiry (default 24 hours).

**Auth flow:**

```python
def get_current_org(credentials):
    token = credentials.credentials
    if token.startswith("obs_live_"):
        # API key path: hash -> lookup in api_keys table
        return validate_api_key_sync(token)
    else:
        # JWT path: decode -> extract org context
        return decode_jwt_token(token)
```

**Scope-based authorization:** API keys carry scopes (e.g., `["ingest"]`), JWT tokens derive scopes from user role (`admin` -> `["read", "write", "admin"]`, `editor` -> `["read", "write"]`, `viewer` -> `["read"]`).

**Multi-tenancy:** All queries are scoped by `organization_id` extracted from the auth context.

#### Ingest Router

**File:** `api/routers/ingest.py`

The ingest endpoint accepts metadata from Spark and processes it asynchronously:

```python
@router.post("/metadata")
async def ingest_metadata(payload, background_tasks, org):
    background_tasks.add_task(process_metadata, payload, org.org_id)
    return IngestResponse(status="accepted", ...)
```

**`process_metadata()` background processing flow:**

```
1. Upsert all datasets (inputs + outputs + column lineage tables)
   - INSERT ... ON CONFLICT (name) DO UPDATE
   - Infers dataset_type via `infer_dataset_type()`: checks location patterns first (S3 paths -> "s3", HDFS -> "hdfs", etc.) before falling back to the reported table type, so path-based datasets show their storage type rather than a generic "file"

2. Insert lineage edges
   - INSERT ... ON CONFLICT (source, target, job_id) DO NOTHING

3. Insert metrics
   - INSERT into dataset_metrics (row_count, byte_size, ...)

4. Insert anomalies (if sent from Spark side)

5. Insert column lineage edges
   - INSERT ... ON CONFLICT DO UPDATE (transform_type, expression)

6. COMMIT transaction

7. Trigger backend anomaly detection
   - AnomalyService.detect_anomalies() for each dataset with metrics
   - AlertService.check_and_send_alerts() for any detected anomalies
```

**Schema processing** (`process_schema()`):
1. Normalize field format (`data_type` -> `type`)
2. Check if schema hash already exists (skip if unchanged)
3. Close previous version: `UPDATE schema_versions SET valid_to = NOW() WHERE valid_to IS NULL`
4. Insert new version with `valid_from = NOW()`

This implements SCD Type 2 (Slowly Changing Dimension Type 2) for complete schema audit trail.

#### AnomalyService

**File:** `api/services/anomaly_service.py`

Statistical anomaly detection using 3-sigma process control on row counts.

**Algorithm:**
1. Determine Scope: Generalize the output path (e.g., `.../country=US/date=...` -> `.../country=US/date=*`) to create a `partition_key`.
2. Fetch historical stats: `AVG(row_count)` and `STDDEV(row_count)` from `dataset_metrics` **scoped by this partition_key**. This ensures that "Small Partition" runs (e.g., India sales) are not compared against "Large Partition" runs (e.g., US sales).
3. Require at minimum 5 data points (cold start protection)
4. Compute bounds: `upper = mean + 3 * stddev`, `lower = max(0, mean - 3 * stddev)`
5. If `current > upper` -> `RowCountSpike` or `VolumeSpike` (severity: WARNING)
6. If `current < lower` -> `RowCountDrop` or `VolumeDrop` (severity: CRITICAL)
7. Additional check: if row count drops more than 50% from the mean, flag as CRITICAL even if within sigma bounds

**Important gotcha:** The SQL query `AVG(row_count)` **excludes the current job's data** (fixed in `anomaly_service.py`) to prevent the outlier from inflating the standard deviation and masking the anomaly. See [Section 6](#6-anomaly-detection-mathematics) for the full mathematical analysis.

#### AlertService

**File:** `api/services/alert_service.py`

Matches detected anomalies against user-defined alert rules and dispatches notifications.

**Flow:**
1. Fetch active alert rules for the organization
2. For each anomaly:
   a. Persist to `anomalies` table (with `status = 'OPEN'`)
   b. Find matching rules via pattern matching
   c. Dispatch alert via the matched channel provider
   d. Log to `alert_history` table
   e. Update anomaly status to `'ALERTED'` on successful send

**Rule matching** (`_matches_rule()`):
- Dataset name: supports exact match (`sales_daily`), wildcard prefix (`sales_*`), or catch-all (`*` / NULL)
- Anomaly type: exact match or NULL (matches all)
- Severity: exact match or NULL (matches all)

#### AlertFactory and Providers

**File:** `api/alerting.py`

Strategy pattern for alert dispatch:

```python
class AlertFactory:
    @staticmethod
    def get_provider(channel_type: str) -> AlertProvider:
        if channel_type == "EMAIL":   return EmailProvider()
        elif channel_type == "SLACK": return SlackProvider()
        elif channel_type == "TEAMS": return TeamsProvider()
```

**EmailProvider:**
- Supports `console` backend (prints to terminal, for development) and `smtp`/`gmail` backend (real SMTP delivery)
- HTML email body rendered from Jinja2 template (`templates/anomaly_alert.html`)
- Subject generated from template or default: `[SEVERITY] Data Anomaly: dataset_name`
- Configurable SMTP host, port, TLS, username, password via environment variables
- Enriches email with downstream impact count from database

**SlackProvider:**
- Posts to a webhook URL with Block Kit formatted messages
- Color-coded by severity (CRITICAL: red, WARNING: orange, INFO: blue)
- 5-second timeout on HTTP POST

**TeamsProvider:**
- Posts to a webhook URL with MessageCard format
- Includes severity, dataset name, anomaly type, description, and detection timestamp

### 2.3 Frontend Layer

**Source:** `client/`

Single-page React application built with Vite.

**Technology:**
- React 19 with React Router 7 for routing
- ReactFlow 11 for interactive lineage DAG visualization
- dagre for automatic graph layout (replaces manual node positioning)
- Recharts 3 for metrics time-series charts
- Axios for API communication
- Lucide React for icons

**Composite dataset name display:** The `parseDatasetName(name)` utility splits `bucket:logical_name` into `{ bucket, displayName }`. All pages that display dataset names (Lineage, ColumnLineage, Schema) use this helper to show the short display name with a bucket badge where applicable. The lineage dropdown shows entries in `displayName (bucket)` format for clarity.

**Pages:**

| Route | Component | Description |
|-------|-----------|-------------|
| `/` | `Dashboard` | Overview statistics and summary |
| `/lineage` | `Lineage` | Interactive table-level lineage graph (ReactFlow + dagre auto-layout, bezier edges, bucket badges) |
| `/column-lineage` | `ColumnLineage` | Column-level lineage visualization |
| `/schema` | `Schema` | Schema history, diff viewer, field tree (with overflow-safe containers) |
| `/anomalies` | `Anomalies` | Detected anomalies list with filtering |
| `/alerts` | `Alerts` | Alert rule CRUD management |
| `/api-keys` | `ApiKeys` | API key management |
| `/login` | `Login` | User login (public) |
| `/register` | `Register` | User registration (public) |

**Context providers:**
- `AuthContext` -- manages JWT token, login/logout, protected route enforcement
- `ThemeContext` -- light/dark theme toggle
- `ToastContext` -- notification toasts

**Protected routing:** The `ProtectedRoute` component wraps all authenticated routes, redirecting to `/login` if no valid JWT is present.

---

## 3. Database Schema

PostgreSQL 16 with `uuid-ossp` and `pgcrypto` extensions.

### 3.1 Authentication and Multi-Tenancy

#### organizations

```sql
CREATE TABLE organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    slug VARCHAR(100) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

Root entity for multi-tenancy. All data is scoped by `organization_id`.

#### users

```sql
CREATE TABLE users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255),          -- bcrypt
    name VARCHAR(255),
    role VARCHAR(50) NOT NULL DEFAULT 'viewer',  -- admin, editor, viewer
    email_verified BOOLEAN DEFAULT FALSE,
    invite_token VARCHAR(255),
    invite_expires_at TIMESTAMP WITH TIME ZONE,
    last_login_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- Indexes: idx_users_org(organization_id), idx_users_email(email)
```

#### api_keys

```sql
CREATE TABLE api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    key_hash VARCHAR(64) NOT NULL,       -- SHA-256 of the raw key
    key_prefix VARCHAR(12) NOT NULL,     -- First 12 chars for identification
    name VARCHAR(255) NOT NULL,
    scopes JSONB DEFAULT '["ingest"]',
    created_by UUID REFERENCES users(id),
    last_used_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    revoked BOOLEAN DEFAULT FALSE,
    revoked_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
-- Indexes: idx_api_keys_org, idx_api_keys_hash, idx_api_keys_prefix
```

API keys follow the format `obs_live_<random>`. Only the SHA-256 hash is stored; the raw key is shown once at creation time.

### 3.2 Core Metadata

#### datasets

```sql
CREATE TABLE datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    name VARCHAR(500) NOT NULL,
    dataset_type VARCHAR(50),            -- table, file, s3, iceberg, delta, etc.
    location VARCHAR(1000),              -- Physical path or URI
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);
-- Indexes: idx_datasets_name(name), idx_datasets_org(organization_id)
```

Central node in the lineage graph. Names are globally unique (not scoped by org for simplicity in lineage traversal).

#### lineage_edges

```sql
CREATE TABLE lineage_edges (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    source_dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    target_dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    job_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_dataset_id, target_dataset_id, job_id)
);
-- Indexes: idx_lineage_source, idx_lineage_target, idx_lineage_job, idx_lineage_edges_org
```

Directed edges in the lineage DAG. The unique constraint on `(source, target, job_id)` prevents duplicate edges per job while allowing the same edge to be recorded by different jobs.

#### column_lineage_edges

```sql
CREATE TABLE column_lineage_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    source_dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    source_column VARCHAR(255) NOT NULL,
    target_dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    target_column VARCHAR(255) NOT NULL,
    transform_type VARCHAR(50) NOT NULL, -- DIRECT, EXPRESSION, AGGREGATE, JOIN, FILTER, CASE
    expression TEXT,                     -- e.g., "SUM(fare_amount)"
    job_id VARCHAR(100),
    job_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_dataset_id, source_column, target_dataset_id, target_column, job_id)
);
-- Indexes: idx_col_lineage_source(source_dataset_id, source_column),
--          idx_col_lineage_target(target_dataset_id, target_column)
```

#### schema_versions (SCD Type 2)

```sql
CREATE TABLE schema_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    schema_json JSONB NOT NULL,          -- {"fields": [{"name":"...", "type":"...", "nullable":...}]}
    schema_hash VARCHAR(64),             -- SHA-256 for O(1) change detection
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,                  -- NULL = current version
    change_type VARCHAR(50),             -- INITIAL, ADDED_COLUMN, DROPPED_COLUMN, TYPE_CHANGE
    change_description TEXT
);
-- Indexes: idx_schema_dataset, idx_schema_valid(dataset_id, valid_to), idx_schema_hash
```

SCD Type 2 implementation: `valid_to IS NULL` identifies the current schema. When a new schema is detected (different hash), the previous version's `valid_to` is set to `NOW()` and a new row is inserted with `valid_from = NOW()`.

#### dataset_metrics

```sql
CREATE TABLE dataset_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_count BIGINT,
    byte_size BIGINT,
    file_count INTEGER,
    null_count JSONB,                    -- {"column_name": count, ...}
    distinct_count JSONB,                -- {"column_name": count, ...}
    partition_key VARCHAR(1000),         -- Generalized output path signature
    job_name VARCHAR(500),               -- Job name for scoping
    execution_time_ms BIGINT,
    records_processed BIGINT
);
-- Indexes: idx_metrics_dataset_time(dataset_id, timestamp DESC), idx_metrics_job
```

Time-series metrics table. The composite index on `(dataset_id, timestamp DESC)` enables efficient time-range queries for anomaly detection and charting.

#### anomalies

```sql
CREATE TABLE anomalies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    anomaly_type VARCHAR(100) NOT NULL,  -- RowCountSpike, RowCountDrop, SCHEMA_CHANGE, etc.
    severity VARCHAR(20),                -- INFO, WARNING, CRITICAL
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expected_value JSONB,                -- {"value": 1000}
    actual_value JSONB,                  -- {"value": 5000}
    deviation_score NUMERIC(10, 4),      -- How many stddev from normal
    job_id VARCHAR(200),
    description TEXT,
    metadata JSONB,
    status VARCHAR(50) DEFAULT 'OPEN',   -- OPEN, ACKNOWLEDGED, ALERTED, RESOLVED, FALSE_POSITIVE
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(200),
    resolution_notes TEXT
);
-- Indexes: idx_anomalies_dataset, idx_anomalies_type, idx_anomalies_status,
--          idx_anomalies_detected(detected_at DESC), idx_anomalies_org
```

### 3.3 Alerting

#### alert_rules

```sql
CREATE TABLE alert_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    name VARCHAR(200) NOT NULL,
    description TEXT,
    dataset_name VARCHAR(500),           -- NULL = all datasets, "sales_*" = wildcard
    anomaly_type VARCHAR(100),           -- NULL = all types
    severity VARCHAR(20),                -- NULL = all severities
    channel_type VARCHAR(50) NOT NULL,   -- EMAIL, SLACK, TEAMS, WEBHOOK
    channel_config JSONB NOT NULL,       -- Provider-specific config
    enabled BOOLEAN DEFAULT true,
    deduplication_minutes INT DEFAULT 60,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Indexes: idx_alert_rules_enabled, idx_alert_rules_org
```

#### alert_history

```sql
CREATE TABLE alert_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_rule_id UUID REFERENCES alert_rules(id) ON DELETE CASCADE,
    anomaly_id UUID REFERENCES anomalies(id) ON DELETE CASCADE,
    channel_type VARCHAR(50) NOT NULL,
    channel_context JSONB,               -- Channel config snapshot at send time
    response_payload JSONB,              -- Provider response
    status VARCHAR(20) NOT NULL,         -- SENT, FAILED, DEDUPLICATED
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);
-- Indexes: idx_alert_history_anomaly, idx_alert_history_sent(sent_at DESC)
```

### 3.4 Spark Job Tracking

#### job_executions

```sql
CREATE TABLE job_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id VARCHAR(200) NOT NULL,
    job_name VARCHAR(500),
    application_id VARCHAR(200),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status VARCHAR(50),                  -- Running, Success, Failed
    error_message TEXT,
    total_tasks INTEGER,
    failed_tasks INTEGER,
    total_stages INTEGER,
    shuffle_read_bytes BIGINT,
    shuffle_write_bytes BIGINT,
    metadata JSONB
);
-- Indexes: idx_jobs_id, idx_jobs_started(started_at DESC)
```

#### stage_metrics

```sql
CREATE TABLE stage_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_execution_id UUID REFERENCES job_executions(id) ON DELETE CASCADE,
    job_id VARCHAR(200) NOT NULL,
    application_id VARCHAR(200),
    stage_id INTEGER NOT NULL,
    stage_name VARCHAR(500),
    stage_attempt_id INTEGER DEFAULT 0,
    num_tasks INTEGER,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    duration_ms BIGINT,
    -- I/O
    input_bytes BIGINT, input_records BIGINT,
    output_bytes BIGINT, output_records BIGINT,
    -- Shuffle Read (detailed)
    shuffle_read_bytes BIGINT, shuffle_read_records BIGINT,
    shuffle_remote_bytes_read BIGINT, shuffle_local_bytes_read BIGINT,
    shuffle_remote_bytes_read_to_disk BIGINT, shuffle_fetch_wait_time_ms BIGINT,
    shuffle_remote_blocks_fetched BIGINT, shuffle_local_blocks_fetched BIGINT,
    -- Shuffle Write (detailed)
    shuffle_write_bytes BIGINT, shuffle_write_records BIGINT, shuffle_write_time_ns BIGINT,
    -- Compute
    executor_run_time_ms BIGINT, executor_cpu_time_ns BIGINT, jvm_gc_time_ms BIGINT,
    executor_deserialize_time_ms BIGINT, executor_deserialize_cpu_time_ns BIGINT,
    result_serialization_time_ms BIGINT, result_size_bytes BIGINT,
    -- Memory / Spill
    memory_bytes_spilled BIGINT, disk_bytes_spilled BIGINT, peak_execution_memory BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Unique: idx_stage_metrics_unique(job_id, COALESCE(application_id, ''), stage_id, stage_attempt_id)
-- Indexes: idx_stage_metrics_job_id, idx_stage_metrics_spill (partial WHERE > 0),
--          idx_stage_metrics_gc (partial WHERE > 0)
```

### 3.5 Other Tables

#### freshness_sla

```sql
CREATE TABLE freshness_sla (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    sla_hours INTEGER NOT NULL,
    last_updated TIMESTAMP,
    is_stale BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id)
);
-- Indexes: idx_freshness_dataset, idx_freshness_stale(is_stale, last_updated)
```

### 3.6 Views

```sql
-- Current schema for each dataset (WHERE valid_to IS NULL)
CREATE VIEW current_schemas AS
SELECT d.id as dataset_id, d.name as dataset_name,
       sv.schema_json, sv.valid_from as schema_updated_at, d.organization_id
FROM datasets d
JOIN schema_versions sv ON d.id = sv.dataset_id
WHERE sv.valid_to IS NULL;

-- Lineage graph with human-readable names
CREATE VIEW lineage_graph AS
SELECT le.id, ds.name as source_dataset, dt.name as target_dataset,
       le.job_name, le.created_at, le.organization_id
FROM lineage_edges le
JOIN datasets ds ON le.source_dataset_id = ds.id
JOIN datasets dt ON le.target_dataset_id = dt.id;

-- Anomalies from the last 7 days
CREATE VIEW recent_anomalies AS
SELECT d.name as dataset_name, a.anomaly_type, a.severity,
       a.detected_at, a.status, a.description, a.organization_id
FROM anomalies a
JOIN datasets d ON a.dataset_id = d.id
WHERE a.detected_at > NOW() - INTERVAL '7 days'
ORDER BY a.detected_at DESC;
```

### 3.7 Recursive Functions

```sql
-- Get all downstream datasets (max 10 levels deep)
CREATE FUNCTION get_downstream_datasets(dataset_name_param VARCHAR)
RETURNS TABLE(dataset_name VARCHAR, depth INTEGER) AS $$
WITH RECURSIVE downstream AS (
    SELECT dt.name as dataset_name, 1 as depth
    FROM datasets ds
    JOIN lineage_edges le ON ds.id = le.source_dataset_id
    JOIN datasets dt ON le.target_dataset_id = dt.id
    WHERE ds.name = dataset_name_param
    UNION
    SELECT dt.name as dataset_name, d.depth + 1 as depth
    FROM downstream d
    JOIN datasets ds ON d.dataset_name = ds.name
    JOIN lineage_edges le ON ds.id = le.source_dataset_id
    JOIN datasets dt ON le.target_dataset_id = dt.id
    WHERE d.depth < 10
)
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM downstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

-- Get all upstream datasets (same structure, reversed direction)
CREATE FUNCTION get_upstream_datasets(dataset_name_param VARCHAR)
RETURNS TABLE(dataset_name VARCHAR, depth INTEGER) AS $$
WITH RECURSIVE upstream AS (
    SELECT ds.name as dataset_name, 1 as depth
    FROM datasets dt
    JOIN lineage_edges le ON dt.id = le.target_dataset_id
    JOIN datasets ds ON le.source_dataset_id = ds.id
    WHERE dt.name = dataset_name_param
    UNION
    SELECT ds.name as dataset_name, u.depth + 1 as depth
    FROM upstream u
    JOIN datasets dt ON u.dataset_name = dt.name
    JOIN lineage_edges le ON dt.id = le.target_dataset_id
    JOIN datasets ds ON le.source_dataset_id = ds.id
    WHERE u.depth < 10
)
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM upstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;
```

**Key optimization:** `DISTINCT` with `MIN(depth)` ensures each dataset appears only once at its shortest path distance, avoiding duplicate traversals in graphs with multiple paths.

### 3.8 Entity Relationship Summary

```
organizations  1---*  users
organizations  1---*  api_keys
organizations  1---*  datasets
organizations  1---*  alert_rules

datasets  1---*  lineage_edges (as source)
datasets  1---*  lineage_edges (as target)
datasets  1---*  column_lineage_edges (as source)
datasets  1---*  column_lineage_edges (as target)
datasets  1---*  schema_versions
datasets  1---*  dataset_metrics
datasets  1---*  anomalies
datasets  1---1  freshness_sla

alert_rules  1---*  alert_history
anomalies    1---*  alert_history
```

---

## 4. Data Flow

### Step-by-Step: From Spark Write to Dashboard

#### Step 1: Spark Job Writes to a Table

A Spark job (unchanged application code) executes a write operation:

```scala
// User's code -- no observability imports needed
val enriched = spark.table("bronze_taxi_trips")
  .join(spark.table("dim_zones"), ...)
  .withColumn("total_amount", col("fare") + col("tip"))

enriched.write.mode("overwrite").saveAsTable("silver_enriched_trips")
```

#### Step 2: Listener Captures Metadata

The `ObservabilityListener` (registered via `spark.extraListeners`) intercepts multiple events:

**a) `onJobStart`** -- Creates `JobMetadata`, maps stage IDs to job ID, initializes empty lineage.

**b) `onSuccess`** (QueryExecutionListener) -- Receives the full `QueryExecution` including the logical plan. The listener calls:

```
QueryPlanParser.extractInputTables(qe.logical)
  -> ["bronze_taxi_trips", "dim_zones"]

QueryPlanParser.extractOutputTables(qe.logical)
  -> ["silver_enriched_trips"]

ColumnLineageExtractor.extractColumnLineage(qe.logical)
  -> [ColumnLineageEdge("bronze_taxi_trips", "fare", "silver_enriched_trips", "total_amount", EXPRESSION, "fare + tip"),
      ColumnLineageEdge("bronze_taxi_trips", "tip", "silver_enriched_trips", "total_amount", EXPRESSION, "fare + tip"),
      ...]

SchemaTracker (via extractSchemaFromNode)
  -> SchemaSnapshot(fields=[...], schemaHash="abc123...")
```

Each of these is published immediately via `MetadataPublisher`.

**c) `onStageCompleted`** -- Extracts metrics from accumulators (rows read/written, bytes, shuffle, CPU time, GC, spill), accumulates per-job.

**d) `onJobEnd`** -- Publishes the complete `JobMetadata` including aggregated metrics from all stages.

#### Step 3: Metadata Published to API

`MetadataPublisher` fires HTTP POST requests asynchronously (fire-and-forget on a 2-thread pool):

```
POST /api/v1/ingest/metadata
Authorization: Bearer obs_live_xxx...
Content-Type: application/json

{
  "api_version": "1.0",
  "job_id": "spark-job-42",
  "job_name": "Daily ETL",
  "inputs": [{"name": "bronze_taxi_trips", "type": "Table"}, {"name": "dim_zones", "type": "Table"}],
  "outputs": [{"name": "silver_enriched_trips", "type": "Table"}],
  "lineage_edges": [
    {"source": "bronze_taxi_trips", "target": "silver_enriched_trips"},
    {"source": "dim_zones", "target": "silver_enriched_trips"}
  ],
  "column_lineage": [
    {"source_table": "bronze_taxi_trips", "source_column": "fare",
     "target_table": "silver_enriched_trips", "target_column": "total_amount",
     "transformation_type": "EXPRESSION", "expression": "(fare + tip)"}
  ],
  "metrics": {"silver_enriched_trips": {"row_count": 1500000}}
}
```

And separately:

```
POST /api/v1/ingest/schema
Authorization: Bearer obs_live_xxx...

{
  "job_id": "schema-capture-abc12345",
  "schemas": [{
    "dataset_name": "silver_enriched_trips",
    "fields": [
      {"name": "trip_id", "data_type": "string", "nullable": false},
      {"name": "total_amount", "data_type": "double", "nullable": true}
    ],
    "version_hash": "abc123def456..."
  }]
}
```

#### Step 4: API Processes in Background

The API returns `202 Accepted` immediately, then `process_metadata()` runs in a FastAPI `BackgroundTask`:

```
1. Upsert datasets: bronze_taxi_trips, dim_zones, silver_enriched_trips
   -> Each gets a UUID in dataset_ids map

2. Insert lineage edges:
   bronze_taxi_trips -> silver_enriched_trips
   dim_zones -> silver_enriched_trips

3. Insert metrics for silver_enriched_trips: row_count=1,500,000

4. Insert column lineage edges:
   bronze_taxi_trips.fare -> silver_enriched_trips.total_amount (EXPRESSION)

5. COMMIT

6. Anomaly detection:
   AnomalyService.detect_anomalies("silver_enriched_trips", {row_count: 1500000}, org_id)
   -> Fetches last 30 days of metrics
   -> If mean=1,000,000 and stddev=100,000:
      upper_bound = 1,000,000 + 3*100,000 = 1,300,000
      1,500,000 > 1,300,000 -> RowCountSpike detected!

7. Alert dispatch:
   AlertService.check_and_send_alerts([{anomaly}], org_id)
   -> Finds matching rules: "All Critical Anomalies" (EMAIL)
   -> EmailProvider.send_alert() -> SMTP to data-team@example.com
   -> Logs to alert_history
```

Schema processing runs in a separate background task:
```
1. Check if hash "abc123def456..." exists for silver_enriched_trips -> No
2. Close current version: UPDATE valid_to = NOW()
3. Insert new version: valid_from = NOW(), schema_json = {...}
```

#### Step 5: Frontend Displays Data

The React frontend queries the API:

- **Dashboard:** `GET /anomalies/stats/summary?hours=24` -- shows the RowCountSpike
- **Lineage page:** `GET /datasets/silver_enriched_trips/lineage` -- renders a ReactFlow DAG showing `bronze_taxi_trips` and `dim_zones` flowing into `silver_enriched_trips`
- **Column Lineage:** `GET /datasets/silver_enriched_trips/column-lineage` -- shows `fare + tip -> total_amount`
- **Schema page:** `GET /datasets/silver_enriched_trips/schema/history` -- shows previous and current schema versions with diff
- **Anomalies page:** `GET /anomalies?dataset_name=silver_enriched_trips` -- shows the RowCountSpike with deviation score

---

## 5. Architectural Decisions

### ADR-1: Custom SparkListener over OpenLineage

**Decision:** Build a custom `SparkListener` + `QueryExecutionListener` instead of using OpenLineage.

**Context:** OpenLineage is the industry standard for cross-tool lineage. However, it introduces external dependencies and abstractions that limit access to Spark internals.

**Rationale:**
- Full control over what metadata is captured and how it is processed
- Direct access to Spark's `LogicalPlan` for column-level lineage extraction and schema capture
- Zero external dependencies on the Spark side (no OpenLineage client JAR, no Marquez server)
- Transparent registration via `spark.extraListeners` -- no user code changes needed
- Simpler deployment: single JAR on the classpath

**Trade-off:** This is a single-tool solution (Spark only). OpenLineage supports dbt, Airflow, Flink, and others out of the box.

**When to reconsider:** If the platform needs to track lineage from non-Spark tools (dbt models, Airflow task dependencies, Flink jobs), migrating to or integrating with OpenLineage becomes worthwhile.

### ADR-2: PostgreSQL over Neo4j for Lineage Storage

**Decision:** Store the lineage graph in PostgreSQL using relational tables and recursive CTEs, rather than a dedicated graph database like Neo4j.

**Context:** Lineage is inherently a graph problem. Neo4j's Cypher query language is more natural for graph traversals.

**Rationale:**
- Operational simplicity: single database for everything (lineage, metrics, schemas, auth, alerts)
- ACID transactions across all data types in a single commit
- Team familiarity with SQL and PostgreSQL tooling
- Recursive CTEs are sufficient for lineage traversal at current scale (<100K datasets)
- Rich ecosystem: JSONB for flexible storage, window functions for metrics, percentile functions for anomaly detection

**Performance:** Measured at <100ms for 10-level recursive traversal with 1,000 datasets and 5,000 edges.

**Key optimization in recursive CTEs:**

```sql
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM downstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
```

The `DISTINCT` with `MIN(depth)` ensures each dataset appears only once at its shortest path, preventing exponential blowup in graphs with multiple paths between nodes.

**Trade-off:** SQL is more verbose than Cypher for graph queries. Cypher's `MATCH (a)-[:DEPENDS_ON*1..10]->(b)` is more readable than a 20-line recursive CTE.

**When to reconsider:** At 100K+ datasets with deep traversals (>10 levels), or if pattern-matching queries become common (e.g., "find all paths between A and B"), consider migrating the lineage subset to Neo4j while keeping metrics and auth in PostgreSQL.

### ADR-3: SCD Type 2 for Schema Versioning

**Decision:** Use Slowly Changing Dimension Type 2 (valid_from/valid_to timestamps) for schema version tracking.

**Context:** Schema changes are a major source of data pipeline failures. We need a complete audit trail.

**Rationale:**
- Complete audit trail: every schema version is preserved forever
- Point-in-time queries: "What was the schema on January 15th?"
- Industry standard pattern (data warehousing, Kimball methodology)
- SHA-256 hash enables O(1) change detection: compare hash before writing a new version

**How it works:**

```
schema_versions:
| dataset_id | schema_hash | valid_from       | valid_to         |
|------------|-------------|------------------|------------------|
| ds-1       | abc123...   | 2026-01-01 10:00 | 2026-01-15 14:30 |  <- v1
| ds-1       | def456...   | 2026-01-15 14:30 | NULL             |  <- v2 (current)
```

Current schema: `WHERE valid_to IS NULL` (indexed via `idx_schema_valid`).

**Trade-off:** Linear storage growth. At 1,000 datasets changing schema once per month, this is approximately 25MB/year -- negligible.

### ADR-4: Statistical Anomaly Detection over Machine Learning

**Decision:** Use 3-sigma statistical process control instead of ML-based anomaly detection (Isolation Forest, autoencoders, etc.).

**Context:** The platform needs to detect anomalies in dataset metrics (row count spikes/drops) from day one.

**Rationale:**
- Works from day 1 with no training data (only needs 5 data points)
- Fully explainable: "Row count 5,000,000 is 3.5 standard deviations above the 30-day mean of 1,000,000"
- No ML infrastructure needed (no model training, no serving, no retraining pipeline)
- Implemented in pure SQL: `AVG()` and `STDDEV()` with a time-range filter
- Severity mapping is deterministic: spikes are WARNING, drops are CRITICAL

**Gotcha:** The current implementation includes the current data point in the statistics calculation (because it queries `dataset_metrics` after the current row has been inserted). This inflates both mean and standard deviation, making detection harder with small sample sizes. See [Section 6](#6-anomaly-detection-mathematics) for detailed analysis.

**Trade-off:** Does not handle seasonality (weekend vs. weekday patterns), correlations between metrics (multivariate), or gradual drift. Only analyzes row counts (univariate).

**Upgrade path:**
1. **Seasonal decomposition:** STL decomposition to separate trend, seasonal, and residual components
2. **Isolation Forest:** For multivariate anomaly detection across multiple metric dimensions
3. **Exclude current point:** Run anomaly detection *before* inserting the current metric for cleaner statistics

### ADR-5: FastAPI (Python) over Scala for the API

**Decision:** Build the REST API in Python with FastAPI instead of Scala with Akka HTTP or http4s.

**Context:** The listener is written in Scala. Using the same language for the API would reduce context-switching.

**Rationale:**
- Auto-generated OpenAPI documentation (Swagger UI at `/docs`, ReDoc at `/redoc`)
- Pydantic provides runtime type validation with clear error messages
- Faster development iteration (Python vs. Scala compilation times)
- Rich ecosystem for web APIs: JWT libraries, SMTP, Jinja2 templates, webhook integrations
- psycopg3 provides efficient PostgreSQL access with connection pooling

**Trade-off:** Two different languages in the stack. Acceptable for an MVP where the listener and API are independently deployable services.

### ADR-6: API-Only Architecture (No Direct DB from Spark)

**Decision:** Spark listener sends all data through the REST API. No database credentials are configured on Spark nodes.

**Context:** In many organizations, Spark clusters run on shared infrastructure (EMR, Databricks) where giving database credentials to every job is a security risk.

**Rationale:**
- Security: no database credentials on Spark clusters or in Spark configuration
- Decoupling: listener is unaware of the database schema; the API can evolve independently
- Centralized processing: anomaly detection, schema comparison, and alert dispatch happen in one place
- Multi-tenancy enforcement: the API validates the API key and scopes before processing
- Simpler Spark deployment: only needs API URL and API key, no JDBC drivers

**How it works:**

```
Spark Node:
  spark.observability.api.url = https://api.example.com
  spark.observability.api.key = obs_live_xxx

  ObservabilityListener -> MetadataPublisher (async) -> ObservabilityApiClient
       |                                                      |
       | fire-and-forget                                      | HTTP POST
       | (2-thread pool)                                      | Bearer token
       v                                                      v
  [Spark job continues]                              [API processes in background]
```

**Trade-off:** Network latency on each publish call. Mitigated by fire-and-forget async publishing -- the Spark job never waits for the API response. The 2-thread pool with `Future {}` ensures non-blocking behavior.

---

## 6. Anomaly Detection Mathematics

### The 3-Sigma Rule

The anomaly detection algorithm uses statistical process control based on the normal distribution:

```
upper_bound = mean + 3 * stddev
lower_bound = max(0, mean - 3 * stddev)
```

**Why 3 sigma:** In a normal distribution, 99.7% of values fall within 3 standard deviations of the mean. Values outside this range have only a 0.3% chance of occurring naturally, making them strong candidates for anomalies.

```
                     99.7% of data
              |<------------------------>|
              |                          |
         -3s  -2s  -1s  mean  +1s  +2s  +3s
          |    |    |    |     |    |    |
    ------+----+----+----+----+----+----+------
          ^                              ^
      lower_bound                   upper_bound
          |                              |
   Below = RowCountDrop         Above = RowCountSpike
   (CRITICAL)                   (WARNING)
```

### Severity Logic

- **RowCountSpike** (`current > upper_bound`): Severity = WARNING. Spikes often indicate duplicated data or upstream pipeline issues, but the data itself may be intact.
- **RowCountDrop** (`current < lower_bound`): Severity = CRITICAL. Drops usually mean missing data, failed extractions, or broken upstream pipelines -- more operationally urgent.
- **50% drop fallback**: If `(current - mean) / mean < -0.5`, flag as CRITICAL even if within sigma bounds. This catches large drops when standard deviation is high.

### The Current-Value-Included Gotcha

The `AnomalyService._get_historical_stats()` queries `dataset_metrics` **after** the current row has been inserted (step 3 of `process_metadata()` runs before step 6 anomaly detection). This means the current data point is included in the mean and standard deviation calculation.

**SQL query (includes current point):**

```sql
SELECT AVG(row_count) as mean,
       STDDEV(row_count) as stddev,
       COUNT(*) as count
FROM dataset_metrics dm
JOIN datasets d ON dm.dataset_id = d.id
WHERE d.name = %s
  AND dm.timestamp > NOW() - INTERVAL '30 days'
  AND dm.row_count IS NOT NULL
```

### Mathematical Proof: Why 15 Normal Points Detect a 10x Spike

**Setup:** N normal data points at approximately 100 rows each, plus 1 spike at 1,000 rows.

**With N=15 normal points + 1 spike:**

```
Total points: 16
Sum of normal: 15 * 100 = 1,500
Sum total: 1,500 + 1,000 = 2,500

Mean = 2,500 / 16 = 156.25

Variance = (15 * (100 - 156.25)^2 + 1 * (1000 - 156.25)^2) / 15
         = (15 * 3164.0625 + 1 * 711,914.0625) / 15
         = (47,460.9375 + 711,914.0625) / 15
         = 759,375 / 15
         = 50,625

StdDev = sqrt(50,625) = 225.0

Upper bound = 156.25 + 3 * 225.0 = 831.25

1,000 > 831.25  ->  DETECTED
```

**With N=5 normal points + 1 spike:**

```
Total points: 6
Sum of normal: 5 * 100 = 500
Sum total: 500 + 1,000 = 1,500

Mean = 1,500 / 6 = 250.0

Variance = (5 * (100 - 250)^2 + 1 * (1000 - 250)^2) / 5
         = (5 * 22,500 + 1 * 562,500) / 5
         = (112,500 + 562,500) / 5
         = 135,000

StdDev = sqrt(135,000) = 367.4

Upper bound = 250.0 + 3 * 367.4 = 1,352.2

1,000 < 1,352.2  ->  NOT DETECTED
```

**Conclusion:** With only 5 data points, a single outlier inflates both the mean (from 100 to 250) and the standard deviation (from ~0 to 367.4) so severely that the upper bound exceeds the outlier itself. With 15 data points, the outlier's influence is diluted enough that it remains outside the 3-sigma bound.

**Practical guidance for demos:** Insert approximately 15 normal data points before injecting a 10x spike for reliable detection.

### Percentile-Based Fallback

The API also exposes percentile statistics for non-normal distributions:

```sql
PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY row_count) as p5_row_count,
PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY row_count) as p95_row_count
```

These can be used as alternative bounds for datasets with skewed distributions where the 3-sigma rule is less appropriate.

---

## 7. Alerting Pipeline

### Alert Rule Configuration

Alert rules are stored in the `alert_rules` table and define three things:
1. **What to match:** dataset_name pattern, anomaly_type, severity
2. **Where to send:** channel_type (EMAIL, SLACK, TEAMS) + channel_config (JSON)
3. **Rate limiting:** deduplication_minutes (default 60)

### Dataset Name Pattern Matching

| Pattern | Behavior | Example |
|---------|----------|---------|
| `NULL` or `*` | Matches all datasets | Catch-all rule |
| `sales_*` | Prefix wildcard | Matches `sales_daily`, `sales_monthly` |
| `bronze_taxi_trips` | Exact match | Only that specific dataset |

### Channel Configurations

**EMAIL:**

```json
{
  "email_to": ["data-team@example.com", "oncall@example.com"],
  "email_cc": ["manager@example.com"],
  "subject_template": "[{severity}] {anomaly_type} on {dataset_name}"
}
```

Email rendering uses Jinja2 HTML templates (`api/templates/anomaly_alert.html`), enriched with downstream impact count from the database. Supports two backends:
- `console`: prints to terminal (development)
- `smtp`/`gmail`: real SMTP delivery with TLS, configured via environment variables (`EMAIL_HOST`, `EMAIL_PORT`, `EMAIL_USERNAME`, `EMAIL_PASSWORD`)

**SLACK:**

```json
{
  "webhook_url": "https://hooks.slack.com/services/T.../B.../xxx",
  "channel": "#data-alerts"
}
```

Posts Block Kit formatted messages with severity-based coloring and structured fields.

**TEAMS:**

```json
{
  "webhook_url": "https://outlook.office.com/webhook/..."
}
```

Posts MessageCard formatted payloads with activity sections and fact lists.

### Alert Dispatch Flow

```
Anomaly Detected
       |
       v
AlertService.check_and_send_alerts(anomalies, org_id)
       |
       v
_get_alert_rules(org_id)  <-- Fetch enabled rules for this org
       |
       v
For each anomaly:
  |
  +-> _persist_anomaly()  -- INSERT into anomalies table, get anomaly_id
  |
  +-> _matches_rule() for each rule:
  |     - Dataset name pattern match
  |     - Anomaly type match
  |     - Severity match
  |
  +-> For each matching rule:
        |
        +-> AlertFactory.get_provider(channel_type)
        |     - EMAIL -> EmailProvider
        |     - SLACK -> SlackProvider
        |     - TEAMS -> TeamsProvider
        |
        +-> provider.send_alert(anomaly, channel_config)
        |     - Returns {"status": "SENT"} or {"status": "FAILED", "error": "..."}
        |
        +-> _log_alert_history()  -- INSERT into alert_history
        |
        +-> If SENT: UPDATE anomalies SET status = 'ALERTED'
```

### Deduplication

Each alert rule has a `deduplication_minutes` setting (default: 60). Before sending, the `AlertEngine` checks `alert_history` for a recent `SENT` alert with the same rule, dataset, and anomaly type within the deduplication window. If found, the alert is logged as `DEDUPLICATED` and not sent.

---

## 8. Performance Characteristics

### Lineage Queries

- **Recursive traversal:** <100ms for 10-level depth with 1,000 datasets and 5,000 edges
- **Optimization:** `DISTINCT` + `MIN(depth)` in recursive CTE for shortest-path deduplication
- **Indexes:** Composite indexes on `source_dataset_id` and `target_dataset_id` in `lineage_edges`

### API Latency

- **Read endpoints:** p95 <50ms for most queries (datasets, lineage, schema, metrics)
- **Ingest endpoint:** <10ms response time (returns immediately, processes in background)
- **Background processing:** 50-200ms per ingest payload (depending on number of datasets and edges)

### Schema Comparison

- **Change detection:** O(1) via SHA-256 hash comparison
- **Hash computation:** Canonical string `"name:type:nullable|..."` -> SHA-256 in both Scala (capture) and SQL (lookup)
- **Schema diff:** O(n) where n = max(fields_old, fields_new)

### Anomaly Detection

- **Per-dataset cost:** <10ms (single SQL query for AVG + STDDEV + COUNT with index on `(dataset_id, timestamp DESC)`)
- **30-day window:** Leverages the `idx_metrics_dataset_time` index for efficient time-range scans

### Spark Listener Overhead

- **onSuccess hook:** ~5-20ms for plan parsing and schema extraction (depends on plan complexity)
- **onStageCompleted hook:** <1ms (accumulator reads are in-memory)
- **HTTP publishing:** Fire-and-forget, does not block Spark's event processing thread
- **Thread pool:** 2 dedicated threads for async HTTP calls, separate from Spark's event thread

### Database Indexes

| Index | Table | Purpose |
|-------|-------|---------|
| `idx_metrics_dataset_time(dataset_id, timestamp DESC)` | dataset_metrics | Time-series queries, anomaly detection |
| `idx_lineage_source(source_dataset_id)` | lineage_edges | Downstream traversal |
| `idx_lineage_target(target_dataset_id)` | lineage_edges | Upstream traversal |
| `idx_schema_valid(dataset_id, valid_to)` | schema_versions | Current schema lookup |
| `idx_schema_hash(schema_hash)` | schema_versions | O(1) change detection |
| `idx_anomalies_detected(detected_at DESC)` | anomalies | Recent anomalies |
| `idx_api_keys_hash(key_hash)` | api_keys | O(1) API key validation |
| `idx_stage_metrics_unique(job_id, app_id, stage_id, attempt)` | stage_metrics | Upsert dedup |
| `idx_stage_metrics_spill` (partial) | stage_metrics | WHERE disk_bytes_spilled > 0 |
| `idx_stage_metrics_gc` (partial) | stage_metrics | WHERE jvm_gc_time_ms > 0 |

---

## 9. Scaling Considerations

### Current Scale Profile

The current architecture is designed for small to medium data teams:

| Metric | Current Capacity | Notes |
|--------|-----------------|-------|
| Datasets | <10,000 | Single PostgreSQL, no partitioning |
| Lineage edges | <50,000 | Recursive CTEs remain fast |
| Metrics/day | <100,000 rows | Single `dataset_metrics` table |
| Concurrent Spark jobs | <50 | API handles via async background tasks |
| Users | <100 | JWT auth, no session storage |

**Estimated cost:** ~$50/month (managed PostgreSQL + small API server)

### What Changes at 100K+ Datasets

#### Lineage Storage: Migrate to Neo4j

At 100K+ datasets with deep, complex dependency graphs:
- Recursive CTEs hit performance walls (>500ms for 10-level traversals)
- Pattern-matching queries (all paths between A and B) become impractical in SQL
- **Migration:** Move `datasets`, `lineage_edges`, and `column_lineage_edges` to Neo4j while keeping metrics, auth, and alerts in PostgreSQL

#### Metrics Storage: Partition by Time

At millions of metrics rows:
- Partition `dataset_metrics` by month: `CREATE TABLE dataset_metrics_2026_01 PARTITION OF dataset_metrics FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')`
- Add retention policy: drop partitions older than 1 year
- Consider TimescaleDB for automatic partitioning and compression

#### Anomaly Detection: Move to Streaming

At high ingest volume:
- Replace synchronous anomaly detection with Spark Structured Streaming consuming from Kafka
- Kafka topic: `observability.metrics` fed by the API
- Streaming job computes rolling statistics with proper windowing (excluding current point)

#### API Layer: Add Caching and Read Replicas

At high query volume:
- Redis cache for lineage graphs (invalidated on new edge insertion)
- PostgreSQL read replicas for read-heavy dashboard queries
- Connection pooling with pgbouncer

#### Frontend: Virtual Scrolling

At 10K+ nodes in the lineage graph:
- ReactFlow can handle ~1,000 nodes smoothly; beyond that, implement clustering/grouping
- Virtual scrolling for long dataset/anomaly lists
- Server-side pagination (already implemented)

### Estimated Scale Costs

| Scale | Infrastructure | Estimated Monthly Cost |
|-------|---------------|----------------------|
| Current (<10K datasets) | Managed PostgreSQL + 1 API server | ~$50 |
| Medium (10K-100K datasets) | Larger PostgreSQL + 2 API servers + Redis | ~$200 |
| Large (100K+ datasets) | PostgreSQL + Neo4j + Kafka + Redis + 4 API servers | ~$500+ |

---

*This document describes the architecture as of the current implementation. It is derived from the actual source code and reflects the system as built, including known limitations and trade-offs.*
