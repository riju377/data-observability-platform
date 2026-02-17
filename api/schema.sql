-- =====================================================
-- MASTER SCHEMA DEFINITION - Data Observability Platform
-- =====================================================
-- Contains all table definitions, extensions, and functions.
-- Generated from:
-- 1. scripts/init-db.sql
-- 2. scripts/V3__create_auth_schema.sql
-- 3. api/migrations/001_alerting_system.sql
-- 4. api/migrations/002_column_lineage.sql
-- 5. api/migrations/003_data_quality_rules.sql

-- Enable Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";  -- For hashing

-- =====================================================
-- AUTHENTICATION & MULTI-TENANCY
-- =====================================================

-- Organizations (Tenants)
CREATE TABLE IF NOT EXISTS organizations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL UNIQUE,
    slug VARCHAR(100) UNIQUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Users
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255),
    name VARCHAR(255),
    role VARCHAR(50) NOT NULL DEFAULT 'viewer',
    email_verified BOOLEAN DEFAULT FALSE,
    invite_token VARCHAR(255),
    invite_expires_at TIMESTAMP WITH TIME ZONE,
    last_login_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_users_org ON users(organization_id);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);

-- API Keys
CREATE TABLE IF NOT EXISTS api_keys (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    key_hash VARCHAR(64) NOT NULL,
    key_prefix VARCHAR(12) NOT NULL,
    name VARCHAR(255) NOT NULL,
    scopes JSONB DEFAULT '["ingest"]',
    created_by UUID REFERENCES users(id),
    last_used_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    revoked BOOLEAN DEFAULT FALSE,
    revoked_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_org ON api_keys(organization_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(key_prefix);

-- =====================================================
-- CORE METADATA
-- =====================================================

-- Datasets
CREATE TABLE IF NOT EXISTS datasets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    name VARCHAR(500) NOT NULL,
    dataset_type VARCHAR(50),
    location VARCHAR(1000),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(organization_id, name)
);

CREATE INDEX IF NOT EXISTS idx_datasets_org_name ON datasets(organization_id, name);
CREATE INDEX IF NOT EXISTS idx_datasets_org ON datasets(organization_id);

-- Lineage Edges
CREATE TABLE IF NOT EXISTS lineage_edges (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    source_dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    target_dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    job_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_dataset_id, target_dataset_id)
);

CREATE INDEX IF NOT EXISTS idx_lineage_source ON lineage_edges(source_dataset_id);
CREATE INDEX IF NOT EXISTS idx_lineage_target ON lineage_edges(target_dataset_id);
CREATE INDEX IF NOT EXISTS idx_lineage_job ON lineage_edges(job_id);
CREATE INDEX IF NOT EXISTS idx_lineage_edges_org ON lineage_edges(organization_id);

-- Schema Versions
CREATE TABLE IF NOT EXISTS schema_versions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    schema_json JSONB NOT NULL,
    schema_hash VARCHAR(64),
    valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP,
    change_type VARCHAR(50),
    change_description TEXT
);

CREATE INDEX IF NOT EXISTS idx_schema_dataset ON schema_versions(dataset_id);
CREATE INDEX IF NOT EXISTS idx_schema_valid ON schema_versions(dataset_id, valid_to);
CREATE INDEX IF NOT EXISTS idx_schema_hash ON schema_versions(schema_hash);

-- Dataset Metrics
CREATE TABLE IF NOT EXISTS dataset_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    row_count BIGINT,
    byte_size BIGINT,
    file_count INTEGER,
    null_count JSONB,
    distinct_count JSONB,
    partition_key VARCHAR(1000),         -- Generalized output path signature
    job_name VARCHAR(500),               -- Job name for scoping
    execution_time_ms BIGINT,
    records_processed BIGINT
);

CREATE INDEX IF NOT EXISTS idx_metrics_dataset_time ON dataset_metrics(dataset_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_job ON dataset_metrics(job_id);
CREATE INDEX IF NOT EXISTS idx_metrics_scope ON dataset_metrics(dataset_id, job_name, partition_key);

-- Anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    anomaly_type VARCHAR(100) NOT NULL,
    severity VARCHAR(20),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expected_value JSONB,
    actual_value JSONB,
    deviation_score NUMERIC(10, 4),
    job_id VARCHAR(200),
    description TEXT,
    metadata JSONB,
    status VARCHAR(50) DEFAULT 'OPEN',
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(200),
    resolution_notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_anomalies_dataset ON anomalies(dataset_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_type ON anomalies(anomaly_type);
CREATE INDEX IF NOT EXISTS idx_anomalies_status ON anomalies(status);
CREATE INDEX IF NOT EXISTS idx_anomalies_detected ON anomalies(detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_anomalies_org ON anomalies(organization_id);

-- Freshness SLA
CREATE TABLE IF NOT EXISTS freshness_sla (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    sla_hours INTEGER NOT NULL,
    last_updated TIMESTAMP,
    is_stale BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(dataset_id)
);

CREATE INDEX IF NOT EXISTS idx_freshness_dataset ON freshness_sla(dataset_id);
CREATE INDEX IF NOT EXISTS idx_freshness_stale ON freshness_sla(is_stale, last_updated);
CREATE INDEX IF NOT EXISTS idx_freshness_org ON freshness_sla(organization_id);

-- =====================================================
-- JOB EXECUTIONS (Denormalized)
-- =====================================================

-- Jobs (Master table for job definitions and latest execution state)
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    job_name VARCHAR(500) NOT NULL,
    description TEXT,
    
    -- Execution State (Denormalized from migration 007)
    status VARCHAR(50),
    metadata JSONB,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    last_execution_id VARCHAR(200), -- Spark's job_id/app_id
    execution_metrics JSONB,

    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(organization_id, job_name)
);

CREATE INDEX IF NOT EXISTS idx_jobs_org_name ON jobs(organization_id, job_name);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at DESC);

-- =====================================================
-- ALERTING SYSTEM
-- =====================================================

-- Alert Rules
CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    name VARCHAR(200) NOT NULL,
    description TEXT,
    dataset_name VARCHAR(500),
    anomaly_type VARCHAR(100),
    severity VARCHAR(20),
    channel_type VARCHAR(50) NOT NULL,
    channel_config JSONB NOT NULL,
    enabled BOOLEAN DEFAULT true,
    deduplication_minutes INT DEFAULT 60,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled);
CREATE INDEX IF NOT EXISTS idx_alert_rules_org ON alert_rules(organization_id);

-- Alert History
CREATE TABLE IF NOT EXISTS alert_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_rule_id UUID REFERENCES alert_rules(id) ON DELETE CASCADE,
    anomaly_id UUID REFERENCES anomalies(id) ON DELETE CASCADE,
    channel_type VARCHAR(50) NOT NULL,
    channel_context JSONB,
    response_payload JSONB,
    status VARCHAR(20) NOT NULL,
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_alert_history_anomaly ON alert_history(anomaly_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_sent ON alert_history(sent_at DESC);

-- =====================================================
-- COLUMN LINEAGE
-- =====================================================

CREATE TABLE IF NOT EXISTS column_lineage_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    source_dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    source_column VARCHAR(255) NOT NULL,
    target_dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    target_column VARCHAR(255) NOT NULL,
    transform_type VARCHAR(50) NOT NULL,
    expression TEXT,
    job_id VARCHAR(100),
    job_name VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_dataset_id, source_column, target_dataset_id, target_column, job_id)
);

CREATE INDEX IF NOT EXISTS idx_col_lineage_source ON column_lineage_edges(source_dataset_id, source_column);
CREATE INDEX IF NOT EXISTS idx_col_lineage_target ON column_lineage_edges(target_dataset_id, target_column);

-- =====================================================
-- DATA QUALITY
-- =====================================================

-- Legacy Quality Rules (dataset-linked)
CREATE TABLE IF NOT EXISTS quality_rules (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID REFERENCES organizations(id),
    dataset_id UUID NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
    rule_name VARCHAR(200) NOT NULL,
    rule_type VARCHAR(100),
    column_name VARCHAR(200),
    rule_definition JSONB,
    severity VARCHAR(20) DEFAULT 'WARNING',
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quality_violations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    rule_id UUID NOT NULL REFERENCES quality_rules(id) ON DELETE CASCADE,
    job_id VARCHAR(200),
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    violation_count BIGINT,
    sample_violations JSONB,
    metadata JSONB
);

-- Note: 'data_quality_rules' is a newer variant using dataset_name
CREATE TABLE IF NOT EXISTS data_quality_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Can link to org if needed, adding for consistency
    organization_id UUID REFERENCES organizations(id),
    name VARCHAR(255) NOT NULL,
    description TEXT,
    dataset_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    rule_type VARCHAR(50) NOT NULL,
    rule_config JSONB NOT NULL DEFAULT '{}',
    severity VARCHAR(20) DEFAULT 'WARNING',
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    UNIQUE(organization_id, dataset_name, column_name, rule_type, name)
);

CREATE TABLE IF NOT EXISTS data_quality_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id UUID REFERENCES data_quality_rules(id) ON DELETE CASCADE,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    passed BOOLEAN NOT NULL,
    actual_value JSONB,
    expected_value JSONB,
    violation_count INTEGER DEFAULT 0,
    sample_violations JSONB,
    execution_time_ms INTEGER,
    job_id VARCHAR(100)
);

-- =====================================================
-- VIEWS
-- =====================================================

CREATE OR REPLACE VIEW current_schemas AS
SELECT
    d.id as dataset_id,
    d.name as dataset_name,
    sv.schema_json,
    sv.valid_from as schema_updated_at,
    d.organization_id
FROM datasets d
JOIN schema_versions sv ON d.id = sv.dataset_id
WHERE sv.valid_to IS NULL;

CREATE OR REPLACE VIEW lineage_graph AS
SELECT
    le.id,
    ds.name as source_dataset,
    dt.name as target_dataset,
    le.job_name,
    le.created_at,
    le.organization_id
FROM lineage_edges le
JOIN datasets ds ON le.source_dataset_id = ds.id
JOIN datasets dt ON le.target_dataset_id = dt.id;

CREATE OR REPLACE VIEW recent_anomalies AS
SELECT
    d.name as dataset_name,
    a.anomaly_type,
    a.severity,
    a.detected_at,
    a.status,
    a.description,
    a.organization_id
FROM anomalies a
JOIN datasets d ON a.dataset_id = d.id
WHERE a.detected_at > NOW() - INTERVAL '7 days'
ORDER BY a.detected_at DESC;

-- =====================================================
-- FUNCTIONS (Recursive Lineage — org-scoped)
-- =====================================================

CREATE OR REPLACE FUNCTION get_downstream_datasets(dataset_name_param VARCHAR, p_org_id UUID)
RETURNS TABLE(dataset_name VARCHAR, depth INTEGER) AS $$
WITH RECURSIVE downstream AS (
    SELECT dt.name as dataset_name, 1 as depth
    FROM datasets ds
    JOIN lineage_edges le ON ds.id = le.source_dataset_id
    JOIN datasets dt ON le.target_dataset_id = dt.id
    WHERE ds.name = dataset_name_param AND ds.organization_id = p_org_id
    UNION
    SELECT dt.name as dataset_name, d.depth + 1 as depth
    FROM downstream d
    JOIN datasets ds ON d.dataset_name = ds.name AND ds.organization_id = p_org_id
    JOIN lineage_edges le ON ds.id = le.source_dataset_id
    JOIN datasets dt ON le.target_dataset_id = dt.id
    WHERE d.depth < 10
)
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM downstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION get_upstream_datasets(dataset_name_param VARCHAR, p_org_id UUID)
RETURNS TABLE(dataset_name VARCHAR, depth INTEGER) AS $$
WITH RECURSIVE upstream AS (
    SELECT ds.name as dataset_name, 1 as depth
    FROM datasets dt
    JOIN lineage_edges le ON dt.id = le.target_dataset_id
    JOIN datasets ds ON le.source_dataset_id = ds.id
    WHERE dt.name = dataset_name_param AND dt.organization_id = p_org_id
    UNION
    SELECT ds.name as dataset_name, u.depth + 1 as depth
    FROM upstream u
    JOIN datasets dt ON u.dataset_name = dt.name AND dt.organization_id = p_org_id
    JOIN lineage_edges le ON dt.id = le.target_dataset_id
    JOIN datasets ds ON le.source_dataset_id = ds.id
    WHERE u.depth < 10
)
SELECT DISTINCT dataset_name, MIN(depth) as depth
FROM upstream
GROUP BY dataset_name
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

-- Function to get all upstream column dependencies
CREATE OR REPLACE FUNCTION get_upstream_columns(dataset_name_param VARCHAR, column_name_param VARCHAR, p_org_id UUID)
RETURNS TABLE(dataset_name VARCHAR, column_name VARCHAR, transform_type VARCHAR, expression TEXT, depth INTEGER) AS $$
WITH RECURSIVE upstream AS (
    -- Base case: find direct parents
    SELECT
        ds.name as dataset_name,
        cle.source_column as column_name,
        cle.transform_type,
        cle.expression,
        1 as depth
    FROM column_lineage_edges cle
    JOIN datasets dt ON cle.target_dataset_id = dt.id
    JOIN datasets ds ON cle.source_dataset_id = ds.id
    WHERE dt.name = dataset_name_param AND cle.target_column = column_name_param
      AND dt.organization_id = p_org_id

    UNION

    -- Recursive case: find parents of parents
    SELECT
        ds.name as dataset_name,
        cle.source_column as column_name,
        cle.transform_type,
        cle.expression,
        u.depth + 1 as depth
    FROM upstream u
    JOIN datasets dt ON u.dataset_name = dt.name AND dt.organization_id = p_org_id
    JOIN column_lineage_edges cle ON dt.id = cle.target_dataset_id AND u.column_name = cle.target_column
    JOIN datasets ds ON cle.source_dataset_id = ds.id
    WHERE u.depth < 10
)
SELECT DISTINCT dataset_name, column_name, transform_type, expression, MIN(depth) as depth
FROM upstream
GROUP BY dataset_name, column_name, transform_type, expression
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

-- Function to get all downstream column dependencies
CREATE OR REPLACE FUNCTION get_downstream_columns(dataset_name_param VARCHAR, column_name_param VARCHAR, p_org_id UUID)
RETURNS TABLE(dataset_name VARCHAR, column_name VARCHAR, transform_type VARCHAR, expression TEXT, depth INTEGER) AS $$
WITH RECURSIVE downstream AS (
    -- Base case: find direct children
    SELECT
        dt.name as dataset_name,
        cle.target_column as column_name,
        cle.transform_type,
        cle.expression,
        1 as depth
    FROM column_lineage_edges cle
    JOIN datasets ds ON cle.source_dataset_id = ds.id
    JOIN datasets dt ON cle.target_dataset_id = dt.id
    WHERE ds.name = dataset_name_param AND cle.source_column = column_name_param
      AND ds.organization_id = p_org_id

    UNION

    -- Recursive case: find children of children
    SELECT
        dt.name as dataset_name,
        cle.target_column as column_name,
        cle.transform_type,
        cle.expression,
        d.depth + 1 as depth
    FROM downstream d
    JOIN datasets ds ON d.dataset_name = ds.name AND ds.organization_id = p_org_id
    JOIN column_lineage_edges cle ON ds.id = cle.source_dataset_id AND d.column_name = cle.source_column
    JOIN datasets dt ON cle.target_dataset_id = dt.id
    WHERE d.depth < 10
)
SELECT DISTINCT dataset_name, column_name, transform_type, expression, MIN(depth) as depth
FROM downstream
GROUP BY dataset_name, column_name, transform_type, expression
ORDER BY depth, dataset_name;
$$ LANGUAGE SQL;

-- =====================================================
-- SEED DATA
-- =====================================================

-- Default Organization
INSERT INTO organizations (id, name, slug) VALUES 
    ('00000000-0000-0000-0000-000000000001', 'Default Organization', 'default')
ON CONFLICT (id) DO NOTHING;

-- Admin User (Initial)
INSERT INTO users (id, organization_id, email, name, role, invite_token, invite_expires_at)
VALUES (
    gen_random_uuid(),
    '00000000-0000-0000-0000-000000000001',
    'admin@localhost',
    'Admin User',
    'admin',
    'initial-setup-token-change-me',
    NOW() + INTERVAL '7 days'
)
ON CONFLICT (email) DO NOTHING;

-- Demo API Key
INSERT INTO api_keys (id, organization_id, key_hash, key_prefix, name, scopes)
VALUES (
    gen_random_uuid(),
    '00000000-0000-0000-0000-000000000001',
    encode(sha256('obs_live_DEMO_KEY_FOR_TESTING_ONLY'::bytea), 'hex'),
    'obs_live_DEM',
    'Demo API Key (Replace in Production)',
    '["ingest", "read"]'
)
ON CONFLICT DO NOTHING;
