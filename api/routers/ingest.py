"""
Ingest API Router

Endpoints for Spark jobs to push observability data.
All endpoints require API key authentication with "ingest" scope.
"""
from datetime import datetime
from typing import Optional, List
import json
import logging

# Setup file logging to debug intake
logging.basicConfig(filename='api_ingest.log', level=logging.INFO)
logger = logging.getLogger(__name__)

from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, status
from pydantic import BaseModel

from auth import get_current_org, require_scope, OrgContext
from database import execute_query, execute_single, execute_insert, get_db_connection

router = APIRouter(prefix="/api/v1/ingest", tags=["Ingest"])


# ============================================
# Request/Response Models
# ============================================

class TableReference(BaseModel):
    """Reference to a dataset/table"""
    name: str
    type: Optional[str] = None  # TABLE, FILE, VIEW, STREAM
    location: Optional[str] = None  # Physical path (S3, HDFS, file, etc.)
    database: Optional[str] = None
    catalog: Optional[str] = None


class LineageEdge(BaseModel):
    """Edge in the lineage graph"""
    source: str
    target: str
    job_id: Optional[str] = None
    job_name: Optional[str] = None


class ColumnLineageEdge(BaseModel):
    """Column-level lineage edge"""
    source_table: str
    source_column: str
    target_table: str
    target_column: str
    transformation_type: Optional[str] = "DIRECT"
    expression: Optional[str] = None


class DatasetMetrics(BaseModel):
    """Metrics for a dataset"""
    row_count: Optional[int] = None
    byte_size: Optional[int] = None
    file_count: Optional[int] = None
    partition_count: Optional[int] = None


class SchemaField(BaseModel):
    """Field in a schema"""
    name: str
    data_type: str
    nullable: bool = True
    description: Optional[str] = None


class SchemaVersion(BaseModel):
    """Schema snapshot"""
    dataset_name: str
    fields: List[SchemaField]
    version_hash: Optional[str] = None


class Anomaly(BaseModel):
    """Detected anomaly"""
    dataset_name: str
    anomaly_type: str  # ROW_COUNT_SPIKE, ROW_COUNT_DROP, etc.
    severity: str = "MEDIUM"  # LOW, MEDIUM, HIGH, CRITICAL
    current_value: float
    expected_value: float
    threshold: float
    message: str


class IngestPayload(BaseModel):
    """
    Complete payload for ingesting observability data
    
    This is the main payload format for Spark jobs to send data.
    """
    api_version: str = "1.0"
    job_id: str
    job_name: Optional[str] = None
    application_id: Optional[str] = None
    timestamp: Optional[datetime] = None
    
    # Lineage
    inputs: List[TableReference] = []
    outputs: List[TableReference] = []
    lineage_edges: List[LineageEdge] = []
    column_lineage: List[ColumnLineageEdge] = []
    
    # Metrics (keyed by dataset name)
    metrics: dict = {}
    
    # Anomalies
    anomalies: List[Anomaly] = []
    
    # Job-level execution metrics (CPU, Spill, Memory)
    execution_metrics: Optional[dict] = None
    
    class Config:
        json_schema_extra = {
            "example": {
                "api_version": "1.0",
                "job_id": "spark-job-12345",
                "job_name": "Daily ETL",
                "timestamp": "2024-01-15T10:30:00Z",
                "inputs": [{"name": "raw.taxi_data", "type": "TABLE"}],
                "outputs": [{"name": "bronze.taxi_trips", "type": "TABLE"}],
                "lineage_edges": [{"source": "raw.taxi_data", "target": "bronze.taxi_trips"}],
                "metrics": {
                    "bronze.taxi_trips": {"row_count": 1000000, "byte_size": 50000000}
                },
                "anomalies": []
            }
        }


class IngestSchemaPayload(BaseModel):
    """Payload for schema ingestion"""
    job_id: str
    schemas: List[SchemaVersion]


class IngestResponse(BaseModel):
    """Response for ingest endpoints"""
    status: str
    job_id: str
    message: str
    datasets_processed: int = 0
    edges_created: int = 0


# ============================================
# Ingest Endpoints
# ============================================

@router.post("/metadata")
async def ingest_metadata(
    payload: IngestPayload,
    background_tasks: BackgroundTasks,
    org: OrgContext = Depends(require_scope("ingest"))
):
    """
    Ingest lineage, metrics, and anomalies from Spark
    
    This is the main endpoint called by the Spark ObservabilityListener.
    Processing is done asynchronously to minimize latency to the Spark job.
    
    **Authentication:** Requires API key with "ingest" scope.
    
    **Example:**
    ```bash
    curl -X POST https://api.example.com/api/v1/ingest/metadata \\
      -H "Authorization: Bearer obs_live_xxx..." \\
      -H "Content-Type: application/json" \\
      -d '{"job_id": "spark-123", "inputs": [...], "outputs": [...]}'
    ```
    """
    logger.debug(f"Received metadata for job: {payload.job_id} ({payload.job_name}). Metrics present: {payload.execution_metrics is not None}")
    if payload.execution_metrics:
        logger.debug(f"Execution metrics payload: {json.dumps(payload.execution_metrics)}")
        
    # Queue for async processing (immediate return to Spark)
    background_tasks.add_task(process_metadata, payload, org.org_id)
    
    return IngestResponse(
        status="accepted",
        job_id=payload.job_id,
        message="Metadata queued for processing",
        datasets_processed=len(payload.inputs) + len(payload.outputs),
        edges_created=len(payload.lineage_edges)
    )


@router.post("/schema", response_model=IngestResponse)
async def ingest_schema(
    payload: IngestSchemaPayload,
    background_tasks: BackgroundTasks,
    org: OrgContext = Depends(require_scope("ingest"))
):
    """
    Ingest schema snapshots from Spark
    
    Schema versions are stored for drift detection and impact analysis.
    
    **Authentication:** Requires API key with "ingest" scope.
    """
    background_tasks.add_task(process_schema, payload, org.org_id)
    
    return IngestResponse(
        status="accepted",
        job_id=payload.job_id,
        message="Schema queued for processing",
        datasets_processed=len(payload.schemas)
    )


# ============================================
# Helpers
# ============================================

import re

def infer_dataset_type(name: str, location: Optional[str] = None, declared_type: Optional[str] = None) -> str:
    """Infer dataset type from name, location, and declared type.

    For FILE type, checks location patterns FIRST to determine the actual
    storage system (S3, GCS, HDFS, etc.) instead of just returning "file".
    """
    dt = (declared_type or "").upper()

    # Trust the Scala listener for TABLE/VIEW — managed tables may have
    # local file:/ warehouse paths that should NOT make them "file" type
    if dt in ("TABLE", "VIEW"):
        return dt.lower()

    # Check location patterns FIRST — the Scala listener sends "File" for all
    # path-based reads/writes (S3, GCS, HDFS, etc.) but we want the actual storage type
    loc = (location or "").lower()
    if loc:
        if any(loc.startswith(p) for p in ("s3://", "s3a://", "s3n://")):
            return "s3"
        if loc.startswith("gs://"):
            return "gcs"
        if any(loc.startswith(p) for p in ("wasb://", "wasbs://", "abfs://", "abfss://", "adl://")):
            return "azure"
        if loc.startswith("hdfs://"):
            return "hdfs"
        if loc.startswith("dbfs:/"):
            return "dbfs"
        if loc.startswith("file:/") or loc.startswith("/"):
            return "file"

    # Check name patterns
    n = name.lower()
    if any(n.endswith(ext) for ext in (".parquet", ".csv", ".json", ".orc", ".avro")):
        return "file"
    if "iceberg" in n or "iceberg" in loc:
        return "iceberg"
    if "delta" in n or "delta" in loc:
        return "delta"

    # Check name path-like patterns (e.g., /Users/.../data)
    if name.startswith("/") or re.match(r'^[a-z]+://', name):
        return "file"

    # If declared type was FILE but no location matched a specific storage system
    if dt == "FILE":
        return "file"

    # Default: trust declared type or fall back to table
    if declared_type:
        return declared_type.lower()
    return "table"



def infer_location_from_name(name: str) -> Optional[str]:
    """Extract location from name if it looks like a path."""
    if re.match(r'^[a-z]+://', name) or name.startswith("/"):
        return name
    return None


def generalize_path(path: Optional[str]) -> str:
    """
    Generate a stable signature for a path by replacing variable parts.
    """
    if not path:
        return "UNKNOWN"
        
    s = path
    
    # 1. Hive Partitions (key=value)
    # Matches /key=value/ or /key=value at end
    s = re.sub(r'/(date|dt|year|month|day|hour|minute|timestamp|ts)=[^/]+', r'/\1=*', s, flags=re.IGNORECASE)
    
    # 2. Date Ranges (YYYYMMDD-YYYYMMDD)
    s = re.sub(r'\d{8}-\d{8}', '*', s)
    
    # 3. ISO Dates (YYYY-MM-DD)
    s = re.sub(r'\d{4}-\d{2}-\d{2}', '*', s)
    
    # 4. Compact Dates (YYYYMMDD or YYYYMM) - only if bounded
    # /20251003/ -> /*/
    s = re.sub(r'/(\d{6,8})/', r'/*/', s)
    
    # 5. Path Year/Month/Day (/2025/10/03/)
    s = re.sub(r'/\d{4}/\d{2}/\d{2}/', r'/*/*/*/', s)
    s = re.sub(r'/\d{4}/\d{2}/', r'/*/*/', s)
    
    # 6. Time (HH:MM:SS or HH-MM-SS)
    s = re.sub(r'\d{2}[:\-]\d{2}[:\-]\d{2}', '*', s)
    
    # 7. Spark/Hadoop artifacts
    s = re.sub(r'_attempt_\d+', '*', s)
    s = re.sub(r'_temporary', '*', s)
    
    return s



# ============================================
# Background Processing
# ============================================

def process_metadata(payload: IngestPayload, org_id: str):
    """
    Background task to process ingested metadata.
    This ensures Spark jobs return immediately without waiting for DB writes.
    """
    # Wrap entire function in try/except to catch ANY error
    try:
        logger.info(f"Starting process_metadata for job {payload.job_id} ({payload.job_name})")
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # 1. Upsert all datasets
            all_tables = payload.inputs + payload.outputs
            # Also include tables from column lineage (might be new ones)
            column_tables = []
            for cl in payload.column_lineage:
                column_tables.append(TableReference(name=cl.source_table))
                column_tables.append(TableReference(name=cl.target_table))
            
            # Deduplicate by name
            unique_tables = {t.name: t for t in (all_tables + column_tables)}.values()
            
            dataset_ids = {}  # name -> uuid
            
            for table in unique_tables:
                # Infer type and location
                location = table.location or infer_location_from_name(table.name)
                dataset_type = infer_dataset_type(table.name, location, table.type)

                # Upsert and fetch ID
                cursor.execute("""
                    INSERT INTO datasets (name, dataset_type, location, organization_id, updated_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (organization_id, name) DO UPDATE SET
                        dataset_type = COALESCE(EXCLUDED.dataset_type, datasets.dataset_type),
                        location = COALESCE(EXCLUDED.location, datasets.location),
                        organization_id = EXCLUDED.organization_id,
                        updated_at = NOW()
                    RETURNING id
                """, (table.name, dataset_type, location, org_id))
                ds_row = cursor.fetchone()
                
                if ds_row:
                    dataset_ids[table.name] = ds_row['id']
                else:
                    # Fallback: SELECT if RETURNING failed
                    cursor.execute(
                        "SELECT id FROM datasets WHERE organization_id = %s AND name = %s", 
                        (org_id, table.name)
                    )
                    dataset_ids[table.name] = cursor.fetchone()['id']
            
            # 2. Insert lineage edges
            for edge in payload.lineage_edges:
                source_id = dataset_ids.get(edge.source)
                target_id = dataset_ids.get(edge.target)
                
                if source_id and target_id:
                    cursor.execute("""
                        INSERT INTO lineage_edges (source_dataset_id, target_dataset_id, job_id, job_name, organization_id, created_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (source_dataset_id, target_dataset_id)
                        DO UPDATE SET job_id = EXCLUDED.job_id, job_name = EXCLUDED.job_name, created_at = NOW()
                    """, (source_id, target_id, payload.job_id, payload.job_name, org_id))
            
            # 2.5. Upsert Jobs and Insert Job Executions (Normalized)
            try:
                # Ensure job_name is not None
                final_job_name = payload.job_name or payload.job_id or "Unknown Job"

                # Prepare execution metrics
                logger.debug(f"Processing job {final_job_name} ({payload.job_id}). Execution Metrics: {payload.execution_metrics}")
                exec_metrics_json = json.dumps(payload.execution_metrics) if payload.execution_metrics else None
                status = "SUCCESS" # We assume success if we got here for now

                cursor.execute("""
                    INSERT INTO jobs (
                        organization_id, job_name, updated_at, 
                        status, started_at, ended_at, last_execution_id, execution_metrics, metadata
                    )
                    VALUES (%s, %s, NOW(), %s, NOW(), NOW(), %s, %s, %s::jsonb)
                    ON CONFLICT (organization_id, job_name) 
                    DO UPDATE SET 
                        updated_at = NOW(),
                        status = COALESCE(EXCLUDED.status, jobs.status),
                        started_at = COALESCE(EXCLUDED.started_at, jobs.started_at),
                        ended_at = COALESCE(EXCLUDED.ended_at, jobs.ended_at),
                        last_execution_id = COALESCE(EXCLUDED.last_execution_id, jobs.last_execution_id),
                        execution_metrics = COALESCE(EXCLUDED.execution_metrics, jobs.execution_metrics),
                        metadata = COALESCE(EXCLUDED.metadata, jobs.metadata)
                    RETURNING id
                """, (
                    org_id, 
                    final_job_name, 
                    status if payload.execution_metrics else None, # Only set status if metrics provided
                    payload.job_id,
                    exec_metrics_json,
                    json.dumps({"application_id": payload.application_id}) if payload.application_id else None
                ))
                job_row = cursor.fetchone()
                
                if not job_row:
                    # Fallback: SELECT if RETURNING failed
                    cursor.execute(
                        "SELECT id FROM jobs WHERE organization_id = %s AND job_name = %s", 
                        (org_id, final_job_name)
                    )
                    job_row = cursor.fetchone()
                
                job_definition_id = job_row['id'] if job_row else None
                job_definition_id = job_row['id'] if job_row else None
            except Exception as e:
                logger.error(f"Error upserting job {payload.job_id}: {str(e)}")
            
            # Removed: Steps for separate job_executions table

            
            # 3. Insert metrics
            for dataset_name, metrics_data in payload.metrics.items():
                dataset_id = dataset_ids.get(dataset_name)
                if dataset_id:
                    metrics = DatasetMetrics(**metrics_data) if isinstance(metrics_data, dict) else metrics_data
                    
                    # Generate partition key from location
                    # Try to find the dataset object to get its location
                    # Loop through unique_tables to find this dataset
                    location = None
                    for t in unique_tables:
                        if t.name == dataset_name:
                            location = t.location or infer_location_from_name(t.name)
                            break
                    
                    partition_key = generalize_path(location)
                    
                    cursor.execute("""
                        INSERT INTO dataset_metrics (dataset_id, job_id, job_name, row_count, byte_size, organization_id, timestamp, partition_key)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                    """, (dataset_id, payload.job_id, payload.job_name, metrics.row_count, metrics.byte_size, org_id, partition_key))
            
            # 4. Insert anomalies
            for anomaly in payload.anomalies:
                dataset_id = dataset_ids.get(anomaly.dataset_name)
                if dataset_id:
                    actual_json = json.dumps({"value": anomaly.current_value})
                    expected_json = json.dumps({"value": anomaly.expected_value})
                    cursor.execute("""
                        INSERT INTO anomalies (
                            dataset_id, anomaly_type, severity,
                            actual_value, expected_value, deviation_score, description,
                            organization_id, detected_at, job_id
                        )
                        VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, NOW(), %s)
                    """, (
                        dataset_id, anomaly.anomaly_type, anomaly.severity,
                        actual_json, expected_json,
                        anomaly.threshold, anomaly.message, org_id, payload.job_id
                    ))
            
            # 5. Insert column lineage
            for cl in payload.column_lineage:
                source_id = dataset_ids.get(cl.source_table)
                target_id = dataset_ids.get(cl.target_table)
                
                if source_id and target_id:
                    cursor.execute("""
                        INSERT INTO column_lineage_edges (
                            source_dataset_id, source_column,
                            target_dataset_id, target_column,
                            transform_type, expression,
                            job_id, job_name, organization_id, created_at
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (source_dataset_id, source_column, target_dataset_id, target_column)
                        DO UPDATE SET
                            job_id = EXCLUDED.job_id,
                            job_name = EXCLUDED.job_name,
                            transform_type = EXCLUDED.transform_type,
                            expression = EXCLUDED.expression,
                            created_at = NOW()
                    """, (
                        source_id, cl.source_column,
                        target_id, cl.target_column,
                        cl.transformation_type, cl.expression,
                        payload.job_id, payload.job_name, org_id
                    ))

            conn.commit()
            logger.info(f"Processed metadata for job {payload.job_id}: {len(unique_tables)} datasets, {len(payload.lineage_edges)} edges, {len(payload.column_lineage)} column edges")

            # 6. Trigger Backend Anomaly Detection
            try:
                from services.anomaly_service import AnomalyService
                from services.alert_service import AlertService

                all_anomalies = []

                for dataset_name, metrics_data in payload.metrics.items():
                    metrics = DatasetMetrics(**metrics_data) if isinstance(metrics_data, dict) else metrics_data
                    
                    # Re-derive info for anomaly detection context
                    location = None
                    for t in unique_tables:
                        if t.name == dataset_name:
                            location = t.location or infer_location_from_name(t.name)
                            break
                    partition_key = generalize_path(location)
                    # 4. Check for Anomalies (Scoped)
                    anomalies = AnomalyService.detect_anomalies(
                        dataset_name=dataset_name,
                        metrics=metrics,
                        org_id=org_id,
                        job_name=payload.job_name,
                        partition_key=partition_key,
                        job_id=payload.job_id
                    )
                    if anomalies:
                        all_anomalies.extend(anomalies)

                # Insert backend-detected anomalies
                if all_anomalies:
                    for anomaly in all_anomalies:
                         # anomaly is a dict from detect_anomalies
                         dataset_id = dataset_ids.get(anomaly['dataset_name'])
                         if dataset_id:
                             actual_json = json.dumps({"value": anomaly['current_value']})
                             expected_json = json.dumps({"value": anomaly['expected_value']})
                             try:
                                 cursor.execute("""
                                     INSERT INTO anomalies (
                                         dataset_id, anomaly_type, severity,
                                         actual_value, expected_value, deviation_score, description,
                                         organization_id, detected_at, job_id
                                     )
                                     VALUES (%s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, NOW(), %s)
                                 """, (
                                     dataset_id, anomaly['anomaly_type'], anomaly['severity'],
                                     actual_json, expected_json,
                                     anomaly['deviation'], anomaly['message'], org_id, payload.job_id
                                 ))
                             except Exception as e:
                                 logger.error(f"Error inserting anomaly: {e}")
                    conn.commit()

                # Trigger Alerts
                if all_anomalies:
                    try:
                        AlertService.check_and_send_alerts(all_anomalies, org_id)
                    except Exception as e:
                        logger.error(f"Failed to send alerts: {e}")
                logger.info(f"Processed alerts for {len(all_anomalies)} anomalies")

            except Exception as e:
                logger.error(f"Error in anomaly detection/alerting: {e}")

    except Exception as e:
        logger.error(f"Error processing metadata for job {payload.job_id}: {e}")

def process_schema(payload: IngestSchemaPayload, org_id: str):
    """Process ingested schemas (runs in background)"""
    all_schema_anomalies = []

    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            for schema in payload.schemas:
                # Normalize fields to use "type" key (frontend expects "type", Scala sends "data_type")
                fields = []
                for f in schema.fields:
                    fields.append({
                        "name": f.name,
                        "type": f.data_type,
                        "nullable": f.nullable,
                    })
                fields_json = json.dumps({"fields": fields})

                # Ensure dataset exists (upsert) — schema POSTs may arrive
                # before metadata POSTs, so we can't assume the dataset was
                # already created by process_metadata.
                cursor.execute("""
                    INSERT INTO datasets (name, dataset_type, organization_id, updated_at)
                    VALUES (%s, 'table', %s, NOW())
                    ON CONFLICT (organization_id, name) DO UPDATE SET
                        organization_id = COALESCE(EXCLUDED.organization_id, datasets.organization_id),
                        updated_at = NOW()
                    RETURNING id
                """, (schema.dataset_name, org_id))
                dataset_id = cursor.fetchone()['id']

                # Skip if this exact schema (by hash) already exists for this dataset
                cursor.execute("""
                    SELECT sv.id FROM schema_versions sv
                    WHERE sv.dataset_id = %s AND sv.schema_hash = %s
                    LIMIT 1
                """, (dataset_id, schema.version_hash))

                if cursor.fetchone():
                    continue  # Schema unchanged, skip

                # Fetch previous schema for diff (before closing it)
                cursor.execute("""
                    SELECT sv.schema_json FROM schema_versions sv
                    WHERE sv.dataset_id = %s AND sv.valid_to IS NULL
                    LIMIT 1
                """, (dataset_id,))
                prev_row = cursor.fetchone()
                old_fields = prev_row['schema_json'].get('fields', []) if prev_row else None

                # Detect schema change anomaly
                change_type = "INITIAL"
                change_description = "Schema captured from Spark listener"

                if old_fields is not None:
                    from services.anomaly_service import AnomalyService
                    anomalies, change_type, change_description = AnomalyService.detect_schema_change(
                        schema.dataset_name, old_fields, fields
                    )
                    if anomalies:
                        all_schema_anomalies.extend(anomalies)

                # Close previous version (SCD Type 2)
                cursor.execute("""
                    UPDATE schema_versions SET valid_to = NOW()
                    WHERE dataset_id = %s AND valid_to IS NULL
                """, (dataset_id,))

                # Insert new version with actual change info
                cursor.execute("""
                    INSERT INTO schema_versions (
                        dataset_id, schema_hash, schema_json, organization_id, valid_from,
                        change_type, change_description
                    )
                    VALUES (%s, %s, %s::jsonb, %s, NOW(), %s, %s)
                """, (dataset_id, schema.version_hash, fields_json, org_id,
                      change_type, change_description))

            conn.commit()
            logger.info(f"Processed {len(payload.schemas)} schemas for job {payload.job_id}")

    except Exception as e:
        logger.error(f"An unexpected error occurred in process_metadata: {str(e)}")

    # Trigger alerts for schema change anomalies (outside the DB transaction)
    if all_schema_anomalies:
        try:
            from services.alert_service import AlertService
            AlertService.check_and_send_alerts(all_schema_anomalies, org_id)
            print(f"Processed alerts for {len(all_schema_anomalies)} schema change anomalies")
        except Exception as e:
            print(f"Error in schema change alerting: {e}")
            import traceback
            traceback.print_exc()
