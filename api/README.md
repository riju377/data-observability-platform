# Data Observability Platform API

REST API for querying data lineage, schema versions, metrics, and anomalies collected by the Spark observability listener.

## Quick Start

### 1. Install Dependencies

```bash
cd api
pip install -r requirements.txt
```

### 2. Start the Server

```bash
python main.py
```

The API will start on `http://localhost:8000`

### 3. View API Documentation

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## API Endpoints

### Health Check

```bash
GET /health
```

Returns system status and database connectivity.

### Datasets

#### List all datasets
```bash
GET /datasets?limit=100&offset=0&dataset_type=table
```

#### Get dataset details
```bash
GET /datasets/bronze.taxi_trips
```

### Lineage

#### Get upstream dependencies
```bash
GET /datasets/bronze.taxi_trips/upstream?max_depth=10
```

Returns all datasets that `bronze.taxi_trips` depends on.

#### Get downstream dependencies
```bash
GET /datasets/bronze.taxi_trips/downstream?max_depth=10
```

Returns all datasets that depend on `bronze.taxi_trips`.

**Use case**: "If I change this table, what will break?"

#### Get complete lineage graph
```bash
GET /datasets/bronze.taxi_trips/lineage
```

Returns the dataset, all upstream/downstream dependencies, and edges.

### Schema

#### Get current schema
```bash
GET /datasets/bronze.taxi_trips/schema
```

Returns the most recent schema version (SCD Type 2: where `valid_to IS NULL`).

#### Get schema history
```bash
GET /datasets/bronze.taxi_trips/schema/history?limit=10
```

Returns all historical schema versions, showing how the schema evolved.

### Metrics

#### Get time-series metrics
```bash
GET /datasets/bronze.taxi_trips/metrics?limit=100&hours=24
```

Returns row counts, sizes, and execution times over time.

#### Get statistical summary
```bash
GET /datasets/bronze.taxi_trips/metrics/stats?days=30
```

Returns mean, stddev, min, max, percentiles (P5, P95) for the last 30 days.

**Use case**: Understanding baseline metrics for anomaly detection.

### Anomalies

#### List anomalies
```bash
GET /anomalies?severity=CRITICAL&status=OPEN&hours=24&limit=50
```

Filter by:
- `dataset_name`: Specific dataset
- `severity`: INFO, WARNING, CRITICAL
- `status`: OPEN, ACKNOWLEDGED, RESOLVED, FALSE_POSITIVE
- `hours`: Time window

#### Get anomaly details
```bash
GET /anomalies/{anomaly_id}
```

#### Get anomaly summary
```bash
GET /anomalies/stats/summary?hours=24
```

Returns aggregated counts by severity, status, and type.

## Example Queries

### Find what breaks if I change a table
```bash
curl http://localhost:8000/datasets/bronze.taxi_trips/downstream
```

### Check recent critical anomalies
```bash
curl "http://localhost:8000/anomalies?severity=CRITICAL&status=OPEN&hours=24"
```

### Get metrics for last 7 days
```bash
curl "http://localhost:8000/datasets/bronze.taxi_trips/metrics?hours=168"
```

### See how a schema evolved
```bash
curl http://localhost:8000/datasets/bronze.taxi_trips/schema/history
```

## Response Formats

All responses are JSON. Example:

**GET /datasets**:
```json
[
  {
    "id": "123e4567-e89b-12d3-a456-426614174000",
    "name": "bronze.taxi_trips",
    "dataset_type": "table",
    "location": "s3://bucket/bronze/taxi_trips",
    "created_at": "2024-01-20T10:30:00",
    "updated_at": "2024-01-24T15:45:00"
  }
]
```

**GET /datasets/{name}/downstream**:
```json
[
  {"dataset_name": "silver.enriched_trips", "depth": 1},
  {"dataset_name": "gold.daily_metrics", "depth": 2}
]
```

**GET /anomalies**:
```json
[
  {
    "id": "anomaly-123",
    "dataset_id": "dataset-456",
    "dataset_name": "bronze.taxi_trips",
    "anomaly_type": "ROW_COUNT_DROP",
    "severity": "CRITICAL",
    "detected_at": "2024-01-24T15:45:00",
    "expected_value": {"row_count": 1000000},
    "actual_value": {"row_count": 500000},
    "deviation_score": 3.5,
    "description": "Row count dropped by 50%",
    "status": "OPEN"
  }
]
```

## Error Handling

Errors return appropriate HTTP status codes:

- `404 Not Found`: Resource doesn't exist
- `422 Unprocessable Entity`: Invalid request parameters
- `500 Internal Server Error`: Database or server error

Example error response:
```json
{
  "detail": "Dataset 'invalid.table' not found"
}
```

## CORS

CORS is enabled for all origins (development mode). In production, configure allowed origins in `main.py`:

```python
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://yourdomain.com"],  # Specify allowed origins
    ...
)
```

## Development

### Auto-reload on code changes
The server automatically reloads when you modify the code (thanks to `uvicorn --reload`).

### Adding new endpoints
1. Add Pydantic model in `models.py`
2. Add endpoint in `main.py`
3. Documentation is auto-generated from docstrings

### Database queries
Use the helper functions in `database.py`:
```python
from database import execute_query, execute_single

# Multiple rows
results = execute_query("SELECT * FROM datasets WHERE name = %s", (name,))

# Single row
result = execute_single("SELECT * FROM datasets WHERE id = %s", (id,))
```

## Production Deployment

For production:

1. **Use environment variables** for database credentials:
   ```python
   import os
   DB_CONFIG = {
       "host": os.getenv("DB_HOST", "localhost"),
       "password": os.getenv("DB_PASSWORD"),
       ...
   }
   ```

2. **Use a production WSGI server**:
   ```bash
   gunicorn -w 4 -k uvicorn.workers.UvicornWorker main:app
   ```

3. **Enable authentication** (JWT, OAuth, etc.)

4. **Add rate limiting** (e.g., slowapi)

5. **Configure logging** (structured JSON logs)

6. **Add monitoring** (Prometheus metrics endpoint)

## Testing

Test endpoints with curl:

```bash
# Health check
curl http://localhost:8000/health

# List datasets
curl http://localhost:8000/datasets

# Get lineage
curl http://localhost:8000/datasets/bronze.taxi_trips/lineage

# Get anomalies
curl "http://localhost:8000/anomalies?severity=CRITICAL"
```

Or use the interactive Swagger UI: http://localhost:8000/docs

## Architecture

```
┌─────────────┐     POST /api/v1/ingest/*     ┌─────────────┐
│   Spark     │  ─────────────────────────────→│  FastAPI     │
│  Listener   │     (Bearer token auth)        │  (Python)    │
│  (Scala)    │                                └──────┬───────┘
└─────────────┘                                       │
                                                      ▼
┌─────────────┐     GET /datasets, etc.        ┌─────────────┐
│   React     │  ←─────────────────────────────│ PostgreSQL  │
│  Frontend   │     (via API)                  │  Database   │
└─────────────┘                                └─────────────┘
```

The Spark listener sends metadata through the API (not directly to the database). The API handles ingestion, anomaly detection, schema versioning, and alert dispatch. Read endpoints serve the React frontend and external clients.