-- Database initialization script for Data Observability Platform
-- Creates tables for lineage graph, metadata, and anomalies

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- LINEAGE GRAPH TABLES
-- =====================================================

-- Tables/Datasets (nodes in the lineage graph)
CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(500) NOT NULL,  -- fully qualified name (e.g., "bronze.taxi_trips")
    dataset_type VARCHAR(50),     -- table, view, file, api, etc.
    location VARCHAR(1000),       -- physical location (S3 path, table name, etc.)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name)
);

CREATE INDEX idx_datasets_name ON datasets(name);

-- Lineage edges (dependencies between datasets)
CREATE TABLE IF NOT EXISTS lineage_edges (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    source_dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    target_dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    job_id VARCHAR(200),          -- Spark job ID that created this edge
    job_name VARCHAR(500),        -- Human-readable job name
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_dataset_id, target_dataset_id, job_id)
);

CREATE INDEX idx_lineage_source ON lineage_edges(source_dataset_id);
CREATE INDEX idx_lineage_target ON lineage_edges(target_dataset_id);
CREATE INDEX idx_lineage_job ON lineage_edges(job_id);

-- =====================================================
-- SCHEMA TRACKING
-- =====================================================

-- Schema versions (SCD Type 2)
CREATE TABLE IF NOT EXISTS schema_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    schema_json JSONB NOT NULL,   -- Full schema as JSON
    schema_hash VARCHAR(64),      -- Hash for quick comparison
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,           -- NULL = current version
    change_type VARCHAR(50),      -- ADDED_COLUMN, DROPPED_COLUMN, TYPE_CHANGE, etc.
    change_description TEXT
);

CREATE INDEX idx_schema_dataset ON schema_versions(dataset_id);
CREATE INDEX idx_schema_valid ON schema_versions(dataset_id, valid_to);
CREATE INDEX idx_schema_hash ON schema_versions(schema_hash);

-- =====================================================
-- METRICS & STATISTICS
-- =====================================================

-- Dataset metrics (row counts, size, etc.)
CREATE TABLE IF NOT EXISTS dataset_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Volume metrics
    row_count BIGINT,
    byte_size BIGINT,
    file_count INTEGER,

    -- Quality metrics
    null_count JSONB,             -- {"column_name": null_count, ...}
    distinct_count JSONB,         -- {"column_name": distinct_count, ...}

    -- Execution metrics
    execution_time_ms BIGINT,
    records_processed BIGINT
);

CREATE INDEX idx_metrics_dataset_time ON dataset_metrics(dataset_id, timestamp DESC);
CREATE INDEX idx_metrics_job ON dataset_metrics(job_id);

-- =====================================================
-- ANOMALY DETECTION
-- =====================================================

-- Detected anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    anomaly_type VARCHAR(100) NOT NULL,  -- ROW_COUNT_SPIKE, SCHEMA_CHANGE, FRESHNESS_VIOLATION, etc.
    severity VARCHAR(20),                -- INFO, WARNING, CRITICAL
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Anomaly details
    expected_value JSONB,
    actual_value JSONB,
    deviation_score NUMERIC(10, 4),     -- How many stddev from normal

    -- Context
    job_id VARCHAR(200),
    description TEXT,
    metadata JSONB,                      -- Additional context

    -- Resolution
    status VARCHAR(50) DEFAULT 'OPEN',   -- OPEN, ACKNOWLEDGED, RESOLVED, FALSE_POSITIVE
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(200),
    resolution_notes TEXT
);

CREATE INDEX idx_anomalies_dataset ON anomalies(dataset_id);
CREATE INDEX idx_anomalies_type ON anomalies(anomaly_type);
CREATE INDEX idx_anomalies_status ON anomalies(status);
CREATE INDEX idx_anomalies_detected ON anomalies(detected_at DESC);

-- =====================================================
-- DATA QUALITY RULES
-- =====================================================

-- User-defined data quality rules
CREATE TABLE IF NOT EXISTS quality_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(100),              -- NOT_NULL, UNIQUE, RANGE, PATTERN, CUSTOM
    column_name VARCHAR(200),
    rule_definition JSONB,               -- Rule parameters
    severity VARCHAR(20) DEFAULT 'WARNING',
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_quality_dataset ON quality_rules(dataset_id);

-- Quality rule violations
CREATE TABLE IF NOT EXISTS quality_violations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES quality_rules(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    violation_count BIGINT,
    sample_violations JSONB,             -- Sample of violating records
    metadata JSONB
);

CREATE INDEX idx_violations_rule ON quality_violations(rule_id);
CREATE INDEX idx_violations_detected ON quality_violations(detected_at DESC);

-- =====================================================
-- JOB EXECUTION TRACKING
-- =====================================================

-- Spark job executions
CREATE TABLE IF NOT EXISTS job_executions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id VARCHAR(200) NOT NULL,
    job_name VARCHAR(500),
    application_id VARCHAR(200),
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status VARCHAR(50),                  -- RUNNING, SUCCESS, FAILED
    error_message TEXT,

    -- Execution stats
    total_tasks INTEGER,
    failed_tasks INTEGER,
    total_stages INTEGER,
    shuffle_read_bytes BIGINT,
    shuffle_write_bytes BIGINT,

    metadata JSONB                       -- Additional Spark metrics
);

CREATE INDEX idx_jobs_id ON job_executions(job_id);
CREATE INDEX idx_jobs_name ON job_executions(job_name);
CREATE INDEX idx_jobs_started ON job_executions(started_at DESC);

-- =====================================================
-- STAGE-LEVEL METRICS
-- =====================================================

-- Per-stage metrics for granular performance analysis
CREATE TABLE IF NOT EXISTS stage_metrics (
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
    input_bytes BIGINT,
    input_records BIGINT,
    output_bytes BIGINT,
    output_records BIGINT,

    -- Shuffle Read
    shuffle_read_bytes BIGINT,
    shuffle_read_records BIGINT,
    shuffle_remote_bytes_read BIGINT,
    shuffle_local_bytes_read BIGINT,
    shuffle_remote_bytes_read_to_disk BIGINT,
    shuffle_fetch_wait_time_ms BIGINT,
    shuffle_remote_blocks_fetched BIGINT,
    shuffle_local_blocks_fetched BIGINT,

    -- Shuffle Write
    shuffle_write_bytes BIGINT,
    shuffle_write_records BIGINT,
    shuffle_write_time_ns BIGINT,

    -- Compute
    executor_run_time_ms BIGINT,
    executor_cpu_time_ns BIGINT,
    jvm_gc_time_ms BIGINT,
    executor_deserialize_time_ms BIGINT,
    executor_deserialize_cpu_time_ns BIGINT,
    result_serialization_time_ms BIGINT,
    result_size_bytes BIGINT,

    -- Memory / Spill
    memory_bytes_spilled BIGINT,
    disk_bytes_spilled BIGINT,
    peak_execution_memory BIGINT,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE UNIQUE INDEX idx_stage_metrics_unique
    ON stage_metrics(job_id, COALESCE(application_id, ''), stage_id, stage_attempt_id);

CREATE INDEX idx_stage_metrics_job_id ON stage_metrics(job_id);
CREATE INDEX idx_stage_metrics_app_id ON stage_metrics(application_id);
CREATE INDEX idx_stage_metrics_job_exec ON stage_metrics(job_execution_id);
CREATE INDEX idx_stage_metrics_spill ON stage_metrics(disk_bytes_spilled) WHERE disk_bytes_spilled > 0;
CREATE INDEX idx_stage_metrics_gc ON stage_metrics(jvm_gc_time_ms) WHERE jvm_gc_time_ms > 0;

-- =====================================================
-- FRESHNESS SLA TRACKING
-- =====================================================

CREATE TABLE IF NOT EXISTS freshness_sla (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    sla_hours INTEGER NOT NULL,
    last_updated TIMESTAMP,
    is_stale BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id)
);

CREATE INDEX idx_freshness_dataset ON freshness_sla(dataset_id);
CREATE INDEX idx_freshness_stale ON freshness_sla(is_stale, last_updated);

-- =====================================================
-- VIEWS FOR EASY QUERYING
-- =====================================================

-- View: Latest schema for each dataset
CREATE OR REPLACE VIEW current_schemas AS
SELECT
    d.id as dataset_id,
    d.name as dataset_name,
    sv.schema_json,
    sv.valid_from as schema_updated_at
FROM datasets d
JOIN schema_versions sv ON d.id = sv.dataset_id
WHERE sv.valid_to IS NULL;

-- View: Lineage with dataset names
CREATE OR REPLACE VIEW lineage_graph AS
SELECT
    le.id,
    ds.name as source_dataset,
    dt.name as target_dataset,
    le.job_name,
    le.created_at
FROM lineage_edges le
JOIN datasets ds ON le.source_dataset_id = ds.id
JOIN datasets dt ON le.target_dataset_id = dt.id;

-- View: Recent anomalies summary
CREATE OR REPLACE VIEW recent_anomalies AS
SELECT
    d.name as dataset_name,
    a.anomaly_type,
    a.severity,
    a.detected_at,
    a.status,
    a.description
FROM anomalies a
JOIN datasets d ON a.dataset_id = d.id
WHERE a.detected_at > NOW() - INTERVAL '7 days'
ORDER BY a.detected_at DESC;

-- =====================================================
-- HELPER FUNCTIONS
-- =====================================================

-- Function to get all downstream dependencies of a dataset
CREATE OR REPLACE FUNCTION get_downstream_datasets(dataset_name_param VARCHAR)
RETURNS TABLE(dataset_name VARCHAR, depth INTEGER) AS $$
WITH RECURSIVE downstream AS (
    -- Base case: find direct children
    SELECT
        dt.name as dataset_name,
        1 as depth
    FROM datasets ds
    JOIN lineage_edges le ON ds.id = le.source_dataset_id
    JOIN datasets dt ON le.target_dataset_id = dt.id
    WHERE ds.name = dataset_name_param

    UNION

    -- Recursive case: find children of children
    SELECT
        dt.name as dataset_name,
        d.depth + 1 as depth
    FROM downstream d
    JOIN datasets ds ON d.dataset_name = ds.name
    JOIN lineage_edges le ON ds.id = le.source_dataset_id
    JOIN datasets dt ON le.target_dataset_id = dt.id
    WHERE d.depth < 10  -- Prevent infinite loops
)
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM downstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

-- Function to get all upstream dependencies of a dataset
CREATE OR REPLACE FUNCTION get_upstream_datasets(dataset_name_param VARCHAR)
RETURNS TABLE(dataset_name VARCHAR, depth INTEGER) AS $$
WITH RECURSIVE upstream AS (
    -- Base case: find direct parents
    SELECT
        ds.name as dataset_name,
        1 as depth
    FROM datasets dt
    JOIN lineage_edges le ON dt.id = le.target_dataset_id
    JOIN datasets ds ON le.source_dataset_id = ds.id
    WHERE dt.name = dataset_name_param

    UNION

    -- Recursive case: find parents of parents
    SELECT
        ds.name as dataset_name,
        u.depth + 1 as depth
    FROM upstream u
    JOIN datasets dt ON u.dataset_name = dt.name
    JOIN lineage_edges le ON dt.id = le.target_dataset_id
    JOIN datasets ds ON le.source_dataset_id = ds.id
    WHERE u.depth < 10  -- Prevent infinite loops
)
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM upstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

-- Insert some sample data for testing
INSERT INTO datasets (name, dataset_type, location) VALUES
    ('raw.taxi_trips', 'table', 's3://bucket/raw/taxi/'),
    ('bronze.taxi_trips', 'table', 'iceberg://warehouse/bronze.taxi_trips'),
    ('silver.enriched_trips', 'table', 'iceberg://warehouse/silver.enriched_trips')
ON CONFLICT (name) DO NOTHING;