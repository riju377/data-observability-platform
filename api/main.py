"""
Data Observability Platform REST API

FastAPI application that exposes lineage, schema, and anomaly data
collected by the Spark listener.
"""
from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import uvicorn

from auth import get_current_org, OrgContext

from models import (
    Dataset, LineageEdge, LineageGraph, DatasetDependency,
    ColumnLineageEdge, ColumnDependency, ColumnLineageGraph,
    ImpactedColumn, ImpactedDataset, ImpactAnalysis,
    SchemaVersion, FieldChange, SchemaDiff,
    DatasetMetrics, MetricsTimeSeries,
    Anomaly, AnomalySeverity, AnomalyStatus,
    HealthResponse, ErrorResponse,
    AlertRule, AlertRuleCreate, AlertHistory,
    JobExecution, StageMetrics
)
from database import execute_query, execute_single, execute_insert, test_connection
from alerting import AlertEngine
import json

# Import new routers for API Gateway
from routers import ingest as ingest_router
from routers import auth as auth_router

# ============================================
# FastAPI App Setup
# ============================================

app = FastAPI(
    title="Data Observability Platform API",
    description="REST API for querying data lineage, schema versions, and anomalies",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify allowed origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global Exception Handler for logging
from fastapi import Request
from fastapi.responses import JSONResponse
import traceback
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    error_msg = f"Unhandled Exception: {str(exc)}\n{traceback.format_exc()}"
    logger.error(error_msg)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error", "debug_message": str(exc)}
    )

# Include API Gateway routers
app.include_router(ingest_router.router)
app.include_router(auth_router.router)

# ============================================
# Health Check
# ============================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check():
    """
    Health check endpoint

    Returns system status and database connectivity
    """
    db_status = "connected" if test_connection() else "disconnected"

    return HealthResponse(
        status="healthy" if db_status == "connected" else "unhealthy",
        database=db_status,
        version="1.0.0"
    )


# ============================================
# Dataset Endpoints
# ============================================

@app.get("/datasets", response_model=List[Dataset], tags=["Datasets"])
async def list_datasets(
    dataset_type: Optional[str] = Query(None, description="Filter by dataset type"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    org: OrgContext = Depends(get_current_org)
):
    """
    List all tracked datasets for the authenticated organization

    Returns a list of all datasets that have been observed by the platform.
    Supports filtering by dataset type and pagination.

    **Example:**
    ```
    GET /datasets?dataset_type=table&limit=20&offset=0
    ```
    """
    query = """
        SELECT
            id::text,
            name,
            dataset_type,
            location,
            created_at,
            updated_at
        FROM datasets
        WHERE organization_id = %s
    """

    params = [org.org_id]
    if dataset_type:
        query += " AND dataset_type = %s"
        params.append(dataset_type)

    query += " ORDER BY updated_at DESC"
    query += f" LIMIT {limit} OFFSET {offset}"

    try:
        results = execute_query(query, tuple(params))
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}", response_model=Dataset, tags=["Datasets"])
async def get_dataset(dataset_name: str, org: OrgContext = Depends(get_current_org)):
    """
    Get details for a specific dataset

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips
    ```
    """
    query = """
        SELECT
            id::text,
            name,
            dataset_type,
            location,
            created_at,
            updated_at
        FROM datasets
        WHERE name = %s AND organization_id = %s
    """

    try:
        result = execute_single(query, (dataset_name, org.org_id))
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_name}' not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Lineage Endpoints
# ============================================

@app.get("/datasets/{dataset_name}/upstream", response_model=List[DatasetDependency], tags=["Lineage"])
async def get_upstream_dependencies(
    dataset_name: str,
    max_depth: int = Query(10, ge=1, le=20, description="Maximum depth to traverse"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get all upstream dependencies (parents) of a dataset

    Returns datasets that the specified dataset depends on, recursively up to max_depth.

    **Example:**
    ```
    GET /datasets/silver.enriched_trips/upstream?max_depth=5
    ```

    **Response:**
    ```json
    [
        {"dataset_name": "bronze.taxi_trips", "depth": 1},
        {"dataset_name": "raw.taxi_data", "depth": 2}
    ]
    ```
    """
    query = """
        SELECT dataset_name, depth
        FROM get_upstream_datasets(%s, %s)
        WHERE depth <= %s
        ORDER BY depth, dataset_name
    """

    try:
        results = execute_query(query, (dataset_name, org.org_id, max_depth))
        if not results:
            # Check if dataset exists in this org
            dataset_check = execute_single(
                "SELECT id FROM datasets WHERE name = %s AND organization_id = %s",
                (dataset_name, org.org_id)
            )
            if not dataset_check:
                raise HTTPException(
                    status_code=404,
                    detail=f"Dataset '{dataset_name}' not found"
                )
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/downstream", response_model=List[DatasetDependency], tags=["Lineage"])
async def get_downstream_dependencies(
    dataset_name: str,
    max_depth: int = Query(10, ge=1, le=20, description="Maximum depth to traverse"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get all downstream dependencies (children) of a dataset

    Returns datasets that depend on the specified dataset, recursively up to max_depth.

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips/downstream?max_depth=5
    ```

    **Response:**
    ```json
    [
        {"dataset_name": "silver.enriched_trips", "depth": 1},
        {"dataset_name": "gold.daily_metrics", "depth": 2}
    ]
    ```

    **Use Case:**
    "If I change bronze.taxi_trips, what downstream tables will be affected?"
    """
    query = """
        SELECT dataset_name, depth
        FROM get_downstream_datasets(%s, %s)
        WHERE depth <= %s
        ORDER BY depth, dataset_name
    """

    try:
        results = execute_query(query, (dataset_name, org.org_id, max_depth))
        if not results:
            # Check if dataset exists in this org
            dataset_check = execute_single(
                "SELECT id FROM datasets WHERE name = %s AND organization_id = %s",
                (dataset_name, org.org_id)
            )
            if not dataset_check:
                raise HTTPException(
                    status_code=404,
                    detail=f"Dataset '{dataset_name}' not found"
                )
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/lineage", response_model=LineageGraph, tags=["Lineage"])
async def get_lineage_graph(dataset_name: str, org: OrgContext = Depends(get_current_org)):
    """
    Get complete lineage graph for a dataset

    Returns the dataset, all upstream dependencies, all downstream dependencies,
    and the edges connecting them.

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips/lineage
    ```

    **Response includes:**
    - The dataset itself
    - All upstream datasets (what it reads from)
    - All downstream datasets (what reads from it)
    - All lineage edges with job information
    """
    # Get the dataset
    dataset_query = """
        SELECT
            id::text,
            name,
            dataset_type,
            location,
            created_at,
            updated_at
        FROM datasets
        WHERE name = %s AND organization_id = %s
    """

    try:
        dataset = execute_single(dataset_query, (dataset_name, org.org_id))
        if not dataset:
            raise HTTPException(
                status_code=404,
                detail=f"Dataset '{dataset_name}' not found"
            )

        # Get upstream datasets
        upstream_query = """
            SELECT DISTINCT d.id::text, d.name, d.dataset_type, d.location,
                   d.created_at, d.updated_at
            FROM get_upstream_datasets(%s, %s) ud
            JOIN datasets d ON ud.dataset_name = d.name
        """
        upstream = execute_query(upstream_query, (dataset_name, org.org_id))

        # Get downstream datasets
        downstream_query = """
            SELECT DISTINCT d.id::text, d.name, d.dataset_type, d.location,
                   d.created_at, d.updated_at
            FROM get_downstream_datasets(%s, %s) dd
            JOIN datasets d ON dd.dataset_name = d.name
        """
        downstream = execute_query(downstream_query, (dataset_name, org.org_id))

        # Get all edges in the lineage graph
        # Collect all dataset names in the graph (selected + upstream + downstream)
        all_dataset_names = [dataset_name]
        all_dataset_names.extend([d['name'] for d in upstream])
        all_dataset_names.extend([d['name'] for d in downstream])

        # Get edges where both source AND target are in our lineage graph
        if all_dataset_names:
            placeholders = ','.join(['%s'] * len(all_dataset_names))
            edges_query = f"""
                SELECT
                    le.id::text,
                    le.source_dataset_id::text,
                    le.target_dataset_id::text,
                    ds.name as source_dataset_name,
                    dt.name as target_dataset_name,
                    le.job_id,
                    le.job_name,
                    le.created_at
                FROM lineage_edges le
                JOIN datasets ds ON le.source_dataset_id = ds.id
                JOIN datasets dt ON le.target_dataset_id = dt.id
                WHERE ds.name IN ({placeholders})
                  AND dt.name IN ({placeholders})
            """
            edges = execute_query(edges_query, tuple(all_dataset_names + all_dataset_names))
        else:
            edges = []

        return LineageGraph(
            dataset=dataset,
            upstream=upstream,
            downstream=downstream,
            edges=edges
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Column Lineage Endpoints
# ============================================

@app.get("/datasets/{dataset_name}/columns/{column_name}/upstream",
         response_model=List[ColumnDependency], tags=["Column Lineage"])
async def get_column_upstream(
    dataset_name: str,
    column_name: str,
    max_depth: int = Query(10, ge=1, le=20, description="Maximum traversal depth"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get upstream columns that feed into this column

    Returns all source columns that contribute to the target column,
    with transformation type and expression.

    **Example:**
    ```
    GET /datasets/gold_daily_metrics/columns/total_revenue/upstream
    ```

    **Use Case:** "Where does this column's data come from?"
    """
    query = "SELECT * FROM get_upstream_columns(%s, %s, %s)"

    try:
        results = execute_query(query, (dataset_name, column_name, org.org_id))
        return [ColumnDependency(**r) for r in results if r['depth'] <= max_depth]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/columns/{column_name}/downstream",
         response_model=List[ColumnDependency], tags=["Column Lineage"])
async def get_column_downstream(
    dataset_name: str,
    column_name: str,
    max_depth: int = Query(10, ge=1, le=20, description="Maximum traversal depth"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get downstream columns that use this column

    Returns all target columns that are derived from the source column.

    **Example:**
    ```
    GET /datasets/bronze_taxi_trips/columns/fare_amount/downstream
    ```

    **Use Case:** "What will break if I change this column?"
    """
    query = "SELECT * FROM get_downstream_columns(%s, %s, %s)"

    try:
        results = execute_query(query, (dataset_name, column_name, org.org_id))
        return [ColumnDependency(**r) for r in results if r['depth'] <= max_depth]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/columns/{column_name}/lineage",
         response_model=ColumnLineageGraph, tags=["Column Lineage"])
async def get_column_lineage_graph(
    dataset_name: str,
    column_name: str,
    org: OrgContext = Depends(get_current_org)
):
    """
    Get complete column lineage graph

    Returns upstream columns, downstream columns, and all edges.

    **Example:**
    ```
    GET /datasets/silver_enriched_trips/columns/total_amount/lineage
    ```
    """
    try:
        # Get upstream
        upstream = execute_query(
            "SELECT * FROM get_upstream_columns(%s, %s, %s)",
            (dataset_name, column_name, org.org_id)
        )

        # Get downstream
        downstream = execute_query(
            "SELECT * FROM get_downstream_columns(%s, %s, %s)",
            (dataset_name, column_name, org.org_id)
        )

        # Collect all relevant dataset/column pairs for edge query
        all_columns = [(dataset_name, column_name)]
        all_columns.extend([(r['dataset_name'], r['column_name']) for r in upstream])
        all_columns.extend([(r['dataset_name'], r['column_name']) for r in downstream])

        # Get all edges between these columns
        if all_columns:
            # Build conditions for each dataset/column pair
            dataset_names = list(set([c[0] for c in all_columns]))

            edges_query = """
                SELECT
                    cle.id::text,
                    cle.source_dataset_id::text,
                    ds.name as source_dataset_name,
                    cle.source_column,
                    cle.target_dataset_id::text,
                    dt.name as target_dataset_name,
                    cle.target_column,
                    cle.transform_type,
                    cle.expression,
                    cle.job_id,
                    cle.created_at
                FROM column_lineage_edges cle
                JOIN datasets ds ON cle.source_dataset_id = ds.id
                JOIN datasets dt ON cle.target_dataset_id = dt.id
                WHERE ds.name = ANY(%s) OR dt.name = ANY(%s)
            """
            edges = execute_query(edges_query, (dataset_names, dataset_names))
        else:
            edges = []

        return ColumnLineageGraph(
            dataset_name=dataset_name,
            column_name=column_name,
            upstream=[ColumnDependency(**r) for r in upstream],
            downstream=[ColumnDependency(**r) for r in downstream],
            edges=[ColumnLineageEdge(**e) for e in edges]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/column-lineage", tags=["Column Lineage"])
async def get_dataset_column_lineage(dataset_name: str, org: OrgContext = Depends(get_current_org)):
    """
    Get all column lineage edges for a dataset

    Returns all column-level dependencies for the specified dataset.

    **Example:**
    ```
    GET /datasets/gold_daily_metrics/column-lineage
    ```
    """
    query = """
        SELECT
            cle.id::text,
            cle.source_dataset_id::text,
            ds.name as source_dataset_name,
            cle.source_column,
            cle.target_dataset_id::text,
            dt.name as target_dataset_name,
            cle.target_column,
            cle.transform_type,
            cle.expression,
            cle.job_id,
            cle.created_at
        FROM column_lineage_edges cle
        JOIN datasets ds ON cle.source_dataset_id = ds.id
        JOIN datasets dt ON cle.target_dataset_id = dt.id
        WHERE ds.name = %s OR dt.name = %s
        ORDER BY cle.created_at DESC
    """

    try:
        results = execute_query(query, (dataset_name, dataset_name))
        return [ColumnLineageEdge(**r) for r in results]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Impact Analysis Endpoints
# ============================================

@app.get("/datasets/{dataset_name}/columns/{column_name}/impact",
         response_model=ImpactAnalysis, tags=["Impact Analysis"])
async def get_column_impact(
    dataset_name: str,
    column_name: str,
    max_depth: int = Query(10, ge=1, le=20, description="Maximum depth to traverse"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Analyze the downstream impact if a column changes

    Shows all datasets and columns that would be affected if the source column
    is modified, deleted, or has a schema change.

    **Example:**
    ```
    GET /datasets/bronze_taxi_trips/columns/fare_amount/impact
    ```

    **Use Case:** "What reports/dashboards will break if I change this column?"
    """
    try:
        # Get all downstream columns
        downstream = execute_query(
            "SELECT * FROM get_downstream_columns(%s, %s, %s)",
            (dataset_name, column_name, org.org_id)
        )

        # Filter by max_depth
        downstream = [r for r in downstream if r['depth'] <= max_depth]

        if not downstream:
            return ImpactAnalysis(
                source_dataset=dataset_name,
                source_column=column_name,
                impact_summary={"direct_dependents": 0, "transitive_dependents": 0},
                impacted_datasets=[],
                total_datasets_affected=0,
                total_columns_affected=0,
                max_depth=0,
                criticality="LOW"
            )

        # Group by dataset
        datasets_map = {}
        for col in downstream:
            ds_name = col['dataset_name']
            if ds_name not in datasets_map:
                datasets_map[ds_name] = {
                    'columns': [],
                    'max_depth': 0
                }
            datasets_map[ds_name]['columns'].append(ImpactedColumn(
                dataset_name=col['dataset_name'],
                column_name=col['column_name'],
                transform_type=col.get('transform_type'),
                depth=col['depth']
            ))
            datasets_map[ds_name]['max_depth'] = max(
                datasets_map[ds_name]['max_depth'],
                col['depth']
            )

        # Get dataset types
        dataset_names = list(datasets_map.keys())
        dataset_info = {}
        if dataset_names:
            info_query = "SELECT name, dataset_type FROM datasets WHERE name = ANY(%s)"
            info_results = execute_query(info_query, (dataset_names,))
            for r in info_results:
                dataset_info[r['name']] = r.get('dataset_type')

        # Build impacted datasets list
        impacted_datasets = []
        for ds_name, data in datasets_map.items():
            impacted_datasets.append(ImpactedDataset(
                dataset_name=ds_name,
                dataset_type=dataset_info.get(ds_name),
                columns=data['columns'],
                total_columns_affected=len(data['columns']),
                max_depth=data['max_depth']
            ))

        # Sort by max_depth (closest dependencies first)
        impacted_datasets.sort(key=lambda x: x.max_depth)

        # Calculate summary
        total_columns = len(downstream)
        total_datasets = len(datasets_map)
        max_depth_found = max(col['depth'] for col in downstream) if downstream else 0
        direct_dependents = len([c for c in downstream if c['depth'] == 1])
        transitive_dependents = len([c for c in downstream if c['depth'] > 1])

        # Determine criticality
        if total_datasets >= 5 or total_columns >= 10:
            criticality = "CRITICAL"
        elif total_datasets >= 3 or total_columns >= 5:
            criticality = "HIGH"
        elif total_datasets >= 2 or total_columns >= 3:
            criticality = "MEDIUM"
        else:
            criticality = "LOW"

        return ImpactAnalysis(
            source_dataset=dataset_name,
            source_column=column_name,
            impact_summary={
                "direct_dependents": direct_dependents,
                "transitive_dependents": transitive_dependents
            },
            impacted_datasets=impacted_datasets,
            total_datasets_affected=total_datasets,
            total_columns_affected=total_columns,
            max_depth=max_depth_found,
            criticality=criticality
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Schema Endpoints
# ============================================

@app.get("/datasets/{dataset_name}/schema", response_model=SchemaVersion, tags=["Schema"])
async def get_current_schema(dataset_name: str, org: OrgContext = Depends(get_current_org)):
    """
    Get current schema for a dataset

    Returns the most recent schema version (where valid_to IS NULL).

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips/schema
    ```
    """
    query = """
        SELECT
            sv.id::text,
            sv.dataset_id::text,
            sv.schema_json as schema_data,
            sv.schema_hash,
            sv.valid_from,
            sv.valid_to,
            sv.change_type,
            sv.change_description,
            (sv.valid_to IS NULL) as is_current
        FROM schema_versions sv
        JOIN datasets d ON sv.dataset_id = d.id
        WHERE d.name = %s
          AND sv.valid_to IS NULL
        LIMIT 1
    """

    try:
        result = execute_single(query, (dataset_name,))
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No schema found for dataset '{dataset_name}'"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/schema/history", response_model=List[SchemaVersion], tags=["Schema"])
async def get_schema_history(
    dataset_name: str,
    limit: int = Query(10, ge=1, le=100, description="Maximum number of versions to return"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get schema version history for a dataset

    Returns all historical schema versions, ordered by valid_from descending.

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips/schema/history?limit=5
    ```

    **Use Case:**
    "Show me how this table's schema has evolved over time"
    """
    query = """
        SELECT
            sv.id::text,
            sv.dataset_id::text,
            sv.schema_json as schema_data,
            sv.schema_hash,
            sv.valid_from,
            sv.valid_to,
            sv.change_type,
            sv.change_description,
            (sv.valid_to IS NULL) as is_current
        FROM schema_versions sv
        JOIN datasets d ON sv.dataset_id = d.id
        WHERE d.name = %s
        ORDER BY sv.valid_from DESC
        LIMIT %s
    """

    try:
        results = execute_query(query, (dataset_name, limit))
        if not results:
            # Check if dataset exists
            dataset_check = execute_single(
                "SELECT id FROM datasets WHERE name = %s",
                (dataset_name,)
            )
            if not dataset_check:
                raise HTTPException(
                    status_code=404,
                    detail=f"Dataset '{dataset_name}' not found"
                )
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/schema/diff", response_model=SchemaDiff, tags=["Schema"])
async def get_schema_diff(
    dataset_name: str,
    from_version: str = Query(..., description="Schema version ID to compare from"),
    to_version: str = Query(..., description="Schema version ID to compare to")
):
    """
    Compare two schema versions and show differences

    Returns added, removed, and modified fields between two versions.

    **Example:**
    ```
    GET /datasets/bronze_taxi_trips/schema/diff?from_version=abc123&to_version=def456
    ```

    **Use Case:** "What changed between these two schema versions?"
    """
    try:
        # Get both schema versions
        query = """
            SELECT
                sv.id::text,
                sv.schema_json as schema_data,
                sv.valid_from
            FROM schema_versions sv
            WHERE sv.id::text = %s
        """

        from_schema = execute_single(query, (from_version,))
        to_schema = execute_single(query, (to_version,))

        if not from_schema:
            raise HTTPException(status_code=404, detail=f"Schema version '{from_version}' not found")
        if not to_schema:
            raise HTTPException(status_code=404, detail=f"Schema version '{to_version}' not found")

        # Parse schema fields
        from_fields = {f['name']: f for f in from_schema['schema_data'].get('fields', [])}
        to_fields = {f['name']: f for f in to_schema['schema_data'].get('fields', [])}

        changes = []
        added_count = 0
        removed_count = 0
        modified_count = 0
        is_breaking = False

        # Find added fields
        for name, field in to_fields.items():
            if name not in from_fields:
                changes.append(FieldChange(
                    field_name=name,
                    change_type="ADDED",
                    old_value=None,
                    new_value=field
                ))
                added_count += 1

        # Find removed fields
        for name, field in from_fields.items():
            if name not in to_fields:
                changes.append(FieldChange(
                    field_name=name,
                    change_type="REMOVED",
                    old_value=field,
                    new_value=None
                ))
                removed_count += 1
                is_breaking = True

        # Find modified fields
        for name, from_field in from_fields.items():
            if name in to_fields:
                to_field = to_fields[name]
                if from_field != to_field:
                    changes.append(FieldChange(
                        field_name=name,
                        change_type="MODIFIED",
                        old_value=from_field,
                        new_value=to_field
                    ))
                    modified_count += 1
                    # Type change is breaking
                    if from_field.get('type') != to_field.get('type'):
                        is_breaking = True
                    # Nullable to non-nullable is breaking
                    if from_field.get('nullable', True) and not to_field.get('nullable', True):
                        is_breaking = True

        return SchemaDiff(
            dataset_name=dataset_name,
            from_version=from_version,
            to_version=to_version,
            from_date=from_schema['valid_from'],
            to_date=to_schema['valid_from'],
            changes=changes,
            added_count=added_count,
            removed_count=removed_count,
            modified_count=modified_count,
            is_breaking=is_breaking
        )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Metrics Endpoints
# ============================================

@app.get("/datasets/{dataset_name}/metrics", response_model=List[DatasetMetrics], tags=["Metrics"])
async def get_dataset_metrics(
    dataset_name: str,
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of data points"),
    hours: Optional[int] = Query(None, ge=1, le=720, description="Filter last N hours"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get time-series metrics for a dataset

    Returns row counts, sizes, and execution times over time.

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips/metrics?limit=50&hours=24
    ```

    **Use Case:**
    - Plot row count trends over time
    - Detect gradual data degradation
    - Monitor data volumes
    """
    query = """
        SELECT
            dm.id::text,
            dm.dataset_id::text,
            dm.job_id,
            dm.timestamp,
            dm.row_count,
            dm.byte_size,
            dm.file_count,
            dm.execution_time_ms
        FROM dataset_metrics dm
        JOIN datasets d ON dm.dataset_id = d.id
        WHERE d.name = %s
    """

    params = [dataset_name]

    if hours:
        query += f" AND dm.timestamp > NOW() - INTERVAL '{hours} hours'"

    query += " ORDER BY dm.timestamp DESC"
    query += f" LIMIT {limit}"

    try:
        results = execute_query(query, tuple(params))
        if not results:
            # Check if dataset exists
            dataset_check = execute_single(
                "SELECT id FROM datasets WHERE name = %s",
                (dataset_name,)
            )
            if not dataset_check:
                raise HTTPException(
                    status_code=404,
                    detail=f"Dataset '{dataset_name}' not found"
                )
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/datasets/{dataset_name}/metrics/stats", tags=["Metrics"])
async def get_metrics_statistics(
    dataset_name: str,
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get statistical summary of dataset metrics

    Returns mean, stddev, min, max, percentiles for row counts.

    **Example:**
    ```
    GET /datasets/bronze.taxi_trips/metrics/stats?days=30
    ```

    **Response:**
    ```json
    {
        "dataset_name": "bronze.taxi_trips",
        "days_analyzed": 30,
        "row_count": {
            "mean": 1000000,
            "stddev": 150000,
            "min": 800000,
            "max": 1500000,
            "p5": 850000,
            "p95": 1350000
        },
        "data_points": 30
    }
    ```
    """
    query = """
        SELECT
            COUNT(*) as data_points,
            AVG(row_count) as mean_row_count,
            STDDEV(row_count) as stddev_row_count,
            MIN(row_count) as min_row_count,
            MAX(row_count) as max_row_count,
            PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY row_count) as p5_row_count,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY row_count) as p95_row_count,
            AVG(byte_size) as mean_byte_size,
            MIN(byte_size) as min_byte_size,
            MAX(byte_size) as max_byte_size
        FROM dataset_metrics dm
        JOIN datasets d ON dm.dataset_id = d.id
        WHERE d.name = %s
          AND dm.timestamp > NOW() - (INTERVAL '1 day' * %s)
          AND dm.row_count IS NOT NULL
    """

    try:
        result = execute_single(query, (dataset_name, days))
        if not result or result['data_points'] == 0:
            raise HTTPException(
                status_code=404,
                detail=f"No metrics found for dataset '{dataset_name}' in last {days} days"
            )

        return {
            "dataset_name": dataset_name,
            "days_analyzed": days,
            "row_count": {
                "mean": float(result['mean_row_count']) if result['mean_row_count'] else 0,
                "stddev": float(result['stddev_row_count']) if result['stddev_row_count'] else 0,
                "min": result['min_row_count'],
                "max": result['max_row_count'],
                "p5": result['p5_row_count'],
                "p95": result['p95_row_count']
            },
            "byte_size": {
                "mean": float(result['mean_byte_size']) if result['mean_byte_size'] else 0,
                "min": result['min_byte_size'],
                "max": result['max_byte_size']
            },
            "data_points": result['data_points']
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Anomaly Endpoints
# ============================================

@app.get("/anomalies", response_model=List[Anomaly], tags=["Anomalies"])
async def list_anomalies(
    dataset_name: Optional[str] = Query(None, description="Filter by dataset name"),
    severity: Optional[AnomalySeverity] = Query(None, description="Filter by severity"),
    status: Optional[AnomalyStatus] = Query(None, description="Filter by status"),
    hours: Optional[int] = Query(24, ge=1, le=720, description="Filter last N hours"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    org: OrgContext = Depends(get_current_org)
):
    """
    List detected anomalies for the authenticated organization

    Returns anomalies with optional filtering by dataset, severity, status, and time window.

    **Example:**
    ```
    GET /anomalies?severity=CRITICAL&status=OPEN&hours=24&limit=50
    ```

    **Use Cases:**
    - Dashboard showing recent critical anomalies
    - Alert feed for data operations team
    - Historical analysis of data issues
    """
    query = """
        SELECT
            a.id::text,
            a.dataset_id::text,
            d.name as dataset_name,
            a.anomaly_type,
            a.severity,
            a.detected_at,
            a.expected_value,
            a.actual_value,
            a.deviation_score,
            a.job_id,
            a.description,
            a.status,
            a.resolved_at,
            a.resolved_by,
            a.resolution_notes
        FROM anomalies a
        JOIN datasets d ON a.dataset_id = d.id
        WHERE a.organization_id = %s
    """

    params = [org.org_id]

    if dataset_name:
        query += " AND d.name = %s"
        params.append(dataset_name)

    if severity:
        query += " AND a.severity = %s"
        params.append(severity.value)

    if status:
        query += " AND a.status = %s"
        params.append(status.value)

    if hours:
        query += " AND a.detected_at > NOW() - (INTERVAL '1 hour' * %s)"
        params.append(hours)

    query += " ORDER BY a.detected_at DESC"
    query += f" LIMIT {limit}"

    try:
        results = execute_query(query, tuple(params))
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/anomalies/{anomaly_id}", response_model=Anomaly, tags=["Anomalies"])
async def get_anomaly(anomaly_id: str, org: OrgContext = Depends(get_current_org)):
    """
    Get details for a specific anomaly

    **Example:**
    ```
    GET /anomalies/123e4567-e89b-12d3-a456-426614174000
    ```
    """
    query = """
        SELECT
            a.id::text,
            a.dataset_id::text,
            d.name as dataset_name,
            a.anomaly_type,
            a.severity,
            a.detected_at,
            a.expected_value,
            a.actual_value,
            a.deviation_score,
            a.job_id,
            a.description,
            a.status,
            a.resolved_at,
            a.resolved_by,
            a.resolution_notes
        FROM anomalies a
        JOIN datasets d ON a.dataset_id = d.id
        WHERE a.id = %s::uuid
    """

    try:
        result = execute_single(query, (anomaly_id,))
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Anomaly '{anomaly_id}' not found"
            )
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/anomalies/stats/summary", tags=["Anomalies"])
async def get_anomaly_summary(
    hours: int = Query(24, ge=1, le=720, description="Time window in hours"),
    org: OrgContext = Depends(get_current_org)
):
    """
    Get summary statistics of anomalies for the authenticated organization

    Returns counts by severity, status, and type.

    **Example:**
    ```
    GET /anomalies/stats/summary?hours=24
    ```

    **Response:**
    ```json
    {
        "total_anomalies": 15,
        "by_severity": {
            "CRITICAL": 5,
            "WARNING": 8,
            "INFO": 2
        },
        "by_status": {
            "OPEN": 10,
            "RESOLVED": 5
        },
        "by_type": {
            "ROW_COUNT_DROP": 7,
            "ROW_COUNT_SPIKE": 5,
            "SCHEMA_CHANGE": 3
        }
    }
    ```
    """
    query = """
        SELECT
            COUNT(*) as total,
            severity,
            status,
            anomaly_type
        FROM anomalies
        WHERE organization_id = %s
          AND detected_at > NOW() - (INTERVAL '1 hour' * %s)
        GROUP BY severity, status, anomaly_type
    """

    try:
        results = execute_query(query, (org.org_id, hours))

        # Aggregate results
        summary = {
            "total_anomalies": sum(r['total'] for r in results),
            "time_window_hours": hours,
            "by_severity": {},
            "by_status": {},
            "by_type": {}
        }

        for row in results:
            # By severity
            severity = row['severity']
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + row['total']

            # By status
            status = row['status']
            summary['by_status'][status] = summary['by_status'].get(status, 0) + row['total']

            # By type
            anom_type = row['anomaly_type']
            summary['by_type'][anom_type] = summary['by_type'].get(anom_type, 0) + row['total']

        return summary
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Alert Rules Endpoints
# ============================================

# Initialize alert engine
alert_engine = AlertEngine()


@app.post("/alert-rules", response_model=AlertRule, tags=["Alerts"])
async def create_alert_rule(rule: AlertRuleCreate):
    """
    Create a new alert rule

    Alert rules define when and how to send email notifications for anomalies.
    """
    try:
        query = """
            INSERT INTO alert_rules
            (name, description, dataset_name, anomaly_type, severity,
             channel_type, channel_config, enabled, deduplication_minutes)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id::text, name, description, dataset_name, anomaly_type, severity,
                      channel_type, channel_config, enabled, deduplication_minutes,
                      created_at, updated_at
        """
        result = execute_single(query, (
            rule.name,
            rule.description,
            rule.dataset_name,
            rule.anomaly_type,
            rule.severity,
            rule.channel_type.value if hasattr(rule.channel_type, 'value') else rule.channel_type,
            json.dumps(rule.channel_config),
            rule.enabled,
            rule.deduplication_minutes
        ), commit=True)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/alert-rules", response_model=List[AlertRule], tags=["Alerts"])
async def list_alert_rules(
    enabled: Optional[bool] = Query(None, description="Filter by enabled status"),
    org: OrgContext = Depends(get_current_org)
):
    """
    List all alert rules

    Optionally filter by enabled status.
    """
    try:
        if enabled is not None:
            query = """
                SELECT id::text, name, description, dataset_name, anomaly_type, severity,
                       channel_type, channel_config, enabled, deduplication_minutes,
                       created_at, updated_at
                FROM alert_rules WHERE enabled = %s ORDER BY created_at DESC
            """
            results = execute_query(query, (enabled,))
        else:
            query = """
                SELECT id::text, name, description, dataset_name, anomaly_type, severity,
                       channel_type, channel_config, enabled, deduplication_minutes,
                       created_at, updated_at
                FROM alert_rules ORDER BY created_at DESC
            """
            results = execute_query(query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/alert-rules/{rule_id}", response_model=AlertRule, tags=["Alerts"])
async def get_alert_rule(rule_id: str, org: OrgContext = Depends(get_current_org)):
    """Get alert rule details by ID"""
    try:
        query = """
            SELECT id::text, name, description, dataset_name, anomaly_type, severity,
                   channel_type, channel_config, enabled, deduplication_minutes,
                   created_at, updated_at
            FROM alert_rules WHERE id = %s
        """
        result = execute_single(query, (rule_id,))
        if not result:
            raise HTTPException(status_code=404, detail=f"Alert rule '{rule_id}' not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.put("/alert-rules/{rule_id}", response_model=AlertRule, tags=["Alerts"])
async def update_alert_rule(rule_id: str, rule: AlertRuleCreate):
    """Update an existing alert rule"""
    try:
        query = """
            UPDATE alert_rules
            SET name = %s, description = %s, dataset_name = %s, anomaly_type = %s,
                severity = %s, channel_type = %s, channel_config = %s,
                enabled = %s, deduplication_minutes = %s, updated_at = NOW()
            WHERE id = %s
            RETURNING id::text, name, description, dataset_name, anomaly_type, severity,
                      channel_type, channel_config, enabled, deduplication_minutes,
                      created_at, updated_at
        """
        result = execute_single(query, (
            rule.name,
            rule.description,
            rule.dataset_name,
            rule.anomaly_type,
            rule.severity,
            rule.channel_type.value if hasattr(rule.channel_type, 'value') else rule.channel_type,
            json.dumps(rule.channel_config),
            rule.enabled,
            rule.deduplication_minutes,
            rule_id
        ), commit=True)
        if not result:
            raise HTTPException(status_code=404, detail=f"Alert rule '{rule_id}' not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.delete("/alert-rules/{rule_id}", tags=["Alerts"])
async def delete_alert_rule(rule_id: str):
    """Delete an alert rule"""
    try:
        query = "DELETE FROM alert_rules WHERE id = %s RETURNING id::text"
        result = execute_single(query, (rule_id,), commit=True)
        if not result:
            raise HTTPException(status_code=404, detail=f"Alert rule '{rule_id}' not found")
        return {"message": "Alert rule deleted successfully", "id": result['id']}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Alert History Endpoints
# ============================================

@app.get("/alert-history", response_model=List[AlertHistory], tags=["Alerts"])
async def list_alert_history(
    hours: int = Query(24, ge=1, le=720, description="Time window in hours"),
    status: Optional[str] = Query(None, description="Filter by status (SENT, FAILED, DEDUPLICATED)"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """
    List alert history

    Shows all alerts sent in the specified time window.
    """
    try:
        if status:
            query = """
                SELECT id::text, alert_rule_id::text, anomaly_id::text,
                       channel_type, channel_context, response_payload, status, sent_at,
                       error_message
                FROM alert_history
                WHERE sent_at > NOW() - (INTERVAL '1 hour' * %s)
                AND status = %s
                ORDER BY sent_at DESC
                LIMIT %s
            """
            results = execute_query(query, (hours, status, limit))
        else:
            query = """
                SELECT id::text, alert_rule_id::text, anomaly_id::text,
                       channel_type, channel_context, response_payload, status, sent_at,
                       error_message
                FROM alert_history
                WHERE sent_at > NOW() - (INTERVAL '1 hour' * %s)
                ORDER BY sent_at DESC
                LIMIT %s
            """
            results = execute_query(query, (hours, limit))
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Test Alert Endpoint
# ============================================

@app.post("/test-alert", tags=["Alerts"])
async def test_alert(anomaly_id: Optional[str] = Query(None, description="Anomaly ID to test with (uses mock if not provided)")):
    """
    Test alert system by sending a test email

    If anomaly_id is provided, uses real anomaly data.
    Otherwise, creates a mock anomaly for testing.

    Note: Check EMAIL_BACKEND in .env file:
    - console: Prints to terminal (no actual email)
    - gmail/smtp: Sends real email (requires credentials in .env)
    """
    try:
        if anomaly_id:
            # Get real anomaly
            query = """
                SELECT a.*, d.name as dataset_name
                FROM anomalies a
                JOIN datasets d ON a.dataset_id = d.id
                WHERE a.id = %s
            """
            anomaly = execute_single(query, (anomaly_id,))
            if not anomaly:
                raise HTTPException(status_code=404, detail=f"Anomaly '{anomaly_id}' not found")
        else:
            # Use mock anomaly for testing
            anomaly = {
                "id": "test-mock-123",
                "dataset_name": "test_dataset",
                "anomaly_type": "ROW_COUNT_DROP",
                "severity": "CRITICAL",
                "detected_at": "2026-01-27T10:00:00",
                "expected_value": {"row_count": 1000},
                "actual_value": {"row_count": 500},
                "deviation_score": 3.5,
                "description": "TEST ALERT: Row count dropped by 50% (this is a test)"
            }

        # Send alert using alert engine
        alert_engine.check_and_send_alerts(anomaly)

        return {
            "status": "success",
            "message": "Test alert processed",
            "anomaly_id": anomaly['id'],
            "note": "Check your email or console output (depending on EMAIL_BACKEND setting)"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending test alert: {str(e)}")


# ============================================
# Job Execution & Stage Metrics Endpoints
# ============================================

@app.get("/jobs", response_model=List[JobExecution], tags=["Jobs"])
async def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status (Running, Success, Failed)"),
    job_name: Optional[str] = Query(None, description="Filter by job name (substring match)"),
    application_id: Optional[str] = Query(None, description="Filter by application ID"),
    hours: Optional[int] = Query(None, ge=1, le=720, description="Filter last N hours"),
    limit: int = Query(50, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip"),
    org: OrgContext = Depends(get_current_org)
):
    """
    List job executions

    Returns Spark job execution records with optional filtering.

    **Example:**
    ```
    GET /jobs?status=Success&hours=24&limit=20
    ```
    """
    query = """
        SELECT
            id::text,
            job_id,
            job_name,
            application_id,
            started_at,
            ended_at,
            status,
            error_message,
            total_tasks,
            failed_tasks,
            shuffle_read_bytes,
            shuffle_write_bytes,
            metadata,
            EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000 as duration_ms
        FROM job_executions
        WHERE 1=1
    """

    params = []

    if status:
        query += " AND status = %s"
        params.append(status)

    if job_name:
        query += " AND job_name ILIKE %s"
        params.append(f"%{job_name}%")

    if application_id:
        query += " AND application_id = %s"
        params.append(application_id)

    if hours:
        query += " AND started_at > NOW() - (INTERVAL '1 hour' * %s)"
        params.append(hours)

    query += " ORDER BY started_at DESC"
    query += f" LIMIT {limit} OFFSET {offset}"

    try:
        results = execute_query(query, tuple(params) if params else None)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/jobs/{job_uuid}", response_model=JobExecution, tags=["Jobs"])
async def get_job(job_uuid: str, org: OrgContext = Depends(get_current_org)):
    """
    Get a single job execution by UUID

    **Example:**
    ```
    GET /jobs/123e4567-e89b-12d3-a456-426614174000
    ```
    """
    query = """
        SELECT
            id::text,
            job_id,
            job_name,
            application_id,
            started_at,
            ended_at,
            status,
            error_message,
            total_tasks,
            failed_tasks,
            shuffle_read_bytes,
            shuffle_write_bytes,
            metadata,
            EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000 as duration_ms
        FROM job_executions
        WHERE id = %s::uuid
    """

    try:
        result = execute_single(query, (job_uuid,))
        if not result:
            raise HTTPException(status_code=404, detail=f"Job execution '{job_uuid}' not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/jobs/{job_uuid}/stages", response_model=List[StageMetrics], tags=["Jobs"])
async def get_job_stages(job_uuid: str, org: OrgContext = Depends(get_current_org)):
    """
    Get all stage metrics for a job execution

    Returns per-stage breakdown of metrics including I/O, shuffle, compute, and spill.

    **Example:**
    ```
    GET /jobs/123e4567-e89b-12d3-a456-426614174000/stages
    ```
    """
    # First verify the job exists and get its job_id
    job_query = "SELECT job_id, application_id FROM job_executions WHERE id = %s::uuid"

    try:
        job = execute_single(job_query, (job_uuid,))
        if not job:
            raise HTTPException(status_code=404, detail=f"Job execution '{job_uuid}' not found")

        query = """
            SELECT
                id::text, job_id, application_id, stage_id, stage_name,
                stage_attempt_id, num_tasks, started_at, ended_at, duration_ms,
                input_bytes, input_records, output_bytes, output_records,
                shuffle_read_bytes, shuffle_read_records,
                shuffle_remote_bytes_read, shuffle_local_bytes_read,
                shuffle_remote_bytes_read_to_disk, shuffle_fetch_wait_time_ms,
                shuffle_remote_blocks_fetched, shuffle_local_blocks_fetched,
                shuffle_write_bytes, shuffle_write_records, shuffle_write_time_ns,
                executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms,
                executor_deserialize_time_ms, executor_deserialize_cpu_time_ns,
                result_serialization_time_ms, result_size_bytes,
                memory_bytes_spilled, disk_bytes_spilled, peak_execution_memory,
                created_at
            FROM stage_metrics
            WHERE job_id = %s
        """

        params = [job['job_id']]

        if job.get('application_id'):
            query += " AND application_id = %s"
            params.append(job['application_id'])

        query += " ORDER BY stage_id, stage_attempt_id"

        results = execute_query(query, tuple(params))
        return results
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/jobs/{job_uuid}/stages/{stage_id}", response_model=StageMetrics, tags=["Jobs"])
async def get_job_stage(job_uuid: str, stage_id: int):
    """
    Get metrics for a specific stage of a job execution

    **Example:**
    ```
    GET /jobs/123e4567-e89b-12d3-a456-426614174000/stages/2
    ```
    """
    job_query = "SELECT job_id, application_id FROM job_executions WHERE id = %s::uuid"

    try:
        job = execute_single(job_query, (job_uuid,))
        if not job:
            raise HTTPException(status_code=404, detail=f"Job execution '{job_uuid}' not found")

        query = """
            SELECT
                id::text, job_id, application_id, stage_id, stage_name,
                stage_attempt_id, num_tasks, started_at, ended_at, duration_ms,
                input_bytes, input_records, output_bytes, output_records,
                shuffle_read_bytes, shuffle_read_records,
                shuffle_remote_bytes_read, shuffle_local_bytes_read,
                shuffle_remote_bytes_read_to_disk, shuffle_fetch_wait_time_ms,
                shuffle_remote_blocks_fetched, shuffle_local_blocks_fetched,
                shuffle_write_bytes, shuffle_write_records, shuffle_write_time_ns,
                executor_run_time_ms, executor_cpu_time_ns, jvm_gc_time_ms,
                executor_deserialize_time_ms, executor_deserialize_cpu_time_ns,
                result_serialization_time_ms, result_size_bytes,
                memory_bytes_spilled, disk_bytes_spilled, peak_execution_memory,
                created_at
            FROM stage_metrics
            WHERE job_id = %s AND stage_id = %s
        """

        params = [job['job_id'], stage_id]

        if job.get('application_id'):
            query += " AND application_id = %s"
            params.append(job['application_id'])

        query += " ORDER BY stage_attempt_id DESC LIMIT 1"

        result = execute_single(query, tuple(params))
        if not result:
            raise HTTPException(status_code=404, detail=f"Stage {stage_id} not found for job '{job_uuid}'")
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


@app.get("/jobs/stats/summary", tags=["Jobs"])
async def get_jobs_summary(
    hours: int = Query(24, ge=1, le=720, description="Time window in hours")
):
    """
    Get summary statistics of job executions

    Returns counts by status, average duration, and top failing jobs.

    **Example:**
    ```
    GET /jobs/stats/summary?hours=24
    ```
    """
    query = """
        SELECT
            COUNT(*) as total_jobs,
            SUM(CASE WHEN status = 'Success' THEN 1 ELSE 0 END) as success_count,
            SUM(CASE WHEN status = 'Failed' THEN 1 ELSE 0 END) as failed_count,
            SUM(CASE WHEN status = 'Running' THEN 1 ELSE 0 END) as running_count,
            AVG(EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000) as avg_duration_ms,
            MAX(EXTRACT(EPOCH FROM (ended_at - started_at)) * 1000) as max_duration_ms
        FROM job_executions
        WHERE started_at > NOW() - (INTERVAL '1 hour' * %s)
    """

    try:
        result = execute_single(query, (hours,))

        return {
            "time_window_hours": hours,
            "total_jobs": result['total_jobs'] or 0,
            "success_count": result['success_count'] or 0,
            "failed_count": result['failed_count'] or 0,
            "running_count": result['running_count'] or 0,
            "success_rate": round((result['success_count'] or 0) / max(result['total_jobs'] or 1, 1) * 100, 2),
            "avg_duration_ms": round(float(result['avg_duration_ms'] or 0), 2),
            "max_duration_ms": round(float(result['max_duration_ms'] or 0), 2)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# ============================================
# Run Server
# ============================================

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,  # Auto-reload on code changes
        log_level="info"
    )