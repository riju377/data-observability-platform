-- ============================================
-- Multi-Tenancy Fixes Migration
-- ============================================
-- Scopes dataset uniqueness per organization,
-- adds organization_id to tables missing it,
-- and updates SQL functions to filter by org.
-- ============================================

-- 1. datasets: UNIQUE(name) → UNIQUE(organization_id, name)
-- 1. datasets: UNIQUE(name) -> UNIQUE(organization_id, name)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'datasets_organization_id_name_key') THEN
        ALTER TABLE datasets ADD CONSTRAINT datasets_organization_id_name_key UNIQUE(organization_id, name);
    END IF;
END $$;

DROP INDEX IF EXISTS idx_datasets_name;
CREATE INDEX IF NOT EXISTS idx_datasets_org_name ON datasets(organization_id, name);

-- 2. freshness_sla: add organization_id
ALTER TABLE freshness_sla ADD COLUMN IF NOT EXISTS organization_id UUID REFERENCES organizations(id);
CREATE INDEX IF NOT EXISTS idx_freshness_org ON freshness_sla(organization_id);

-- Backfill freshness_sla org from parent dataset
UPDATE freshness_sla fs
SET organization_id = d.organization_id
FROM datasets d
WHERE fs.dataset_id = d.id AND fs.organization_id IS NULL;

-- 3. job_executions: add organization_id
ALTER TABLE job_executions ADD COLUMN IF NOT EXISTS organization_id UUID REFERENCES organizations(id);
CREATE INDEX IF NOT EXISTS idx_jobs_org ON job_executions(organization_id);

-- 4. stage_metrics: add organization_id
ALTER TABLE stage_metrics ADD COLUMN IF NOT EXISTS organization_id UUID REFERENCES organizations(id);

-- 5. data_quality_rules: scope UNIQUE to org
-- 5. data_quality_rules: scope UNIQUE to org
ALTER TABLE data_quality_rules DROP CONSTRAINT IF EXISTS data_quality_rules_dataset_name_column_name_rule_type_name_key;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'data_quality_rules_org_unique') THEN
        ALTER TABLE data_quality_rules ADD CONSTRAINT data_quality_rules_org_unique 
            UNIQUE(organization_id, dataset_name, column_name, rule_type, name);
    END IF;
END $$;

-- 6. Recreate all 4 SQL functions with org_id parameter
-- (These use CREATE OR REPLACE so they're safe to re-run, but we drop old signatures first)
DROP FUNCTION IF EXISTS get_downstream_datasets(character varying) CASCADE;
DROP FUNCTION IF EXISTS get_upstream_datasets(character varying) CASCADE;
DROP FUNCTION IF EXISTS get_upstream_columns(character varying, character varying) CASCADE;
DROP FUNCTION IF EXISTS get_downstream_columns(character varying, character varying) CASCADE;

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

CREATE OR REPLACE FUNCTION get_upstream_columns(dataset_name_param VARCHAR, column_name_param VARCHAR, p_org_id UUID)
RETURNS TABLE(dataset_name VARCHAR, column_name VARCHAR, transform_type VARCHAR, expression TEXT, depth INTEGER) AS $$
WITH RECURSIVE upstream AS (
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

CREATE OR REPLACE FUNCTION get_downstream_columns(dataset_name_param VARCHAR, column_name_param VARCHAR, p_org_id UUID)
RETURNS TABLE(dataset_name VARCHAR, column_name VARCHAR, transform_type VARCHAR, expression TEXT, depth INTEGER) AS $$
WITH RECURSIVE downstream AS (
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
