-- ============================================
-- Column-Level Lineage Migration
-- ============================================

-- Column lineage edges table
CREATE TABLE IF NOT EXISTS column_lineage_edges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- Source column
    source_dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    source_column VARCHAR(255) NOT NULL,

    -- Target column
    target_dataset_id UUID REFERENCES datasets(id) ON DELETE CASCADE,
    target_column VARCHAR(255) NOT NULL,

    -- Transformation info
    transform_type VARCHAR(50) NOT NULL,  -- DIRECT, JOIN, AGGREGATE, EXPRESSION, FILTER, CASE
    expression TEXT,                       -- Original expression, e.g., "SUM(amount)", "CASE WHEN..."

    -- Job tracking
    job_id VARCHAR(100),
    job_name VARCHAR(500),

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Unique constraint to prevent duplicate edges
    UNIQUE(source_dataset_id, source_column, target_dataset_id, target_column, job_id)
);

-- Indexes for efficient traversal
CREATE INDEX IF NOT EXISTS idx_col_lineage_source
    ON column_lineage_edges(source_dataset_id, source_column);
CREATE INDEX IF NOT EXISTS idx_col_lineage_target
    ON column_lineage_edges(target_dataset_id, target_column);
CREATE INDEX IF NOT EXISTS idx_col_lineage_job
    ON column_lineage_edges(job_id);

-- ============================================
-- Recursive Functions for Column Lineage
-- ============================================

-- Get all upstream columns (what feeds into this column)
CREATE OR REPLACE FUNCTION get_upstream_columns(
    p_dataset_name VARCHAR,
    p_column_name VARCHAR
)
RETURNS TABLE(
    dataset_name VARCHAR,
    column_name VARCHAR,
    transform_type VARCHAR,
    expression TEXT,
    depth INT
) AS $$
WITH RECURSIVE upstream AS (
    -- Base case: direct parents
    SELECT
        ds.name AS dataset_name,
        cle.source_column AS column_name,
        cle.transform_type,
        cle.expression,
        1 AS depth
    FROM column_lineage_edges cle
    JOIN datasets ds ON cle.source_dataset_id = ds.id
    JOIN datasets dt ON cle.target_dataset_id = dt.id
    WHERE dt.name = p_dataset_name
      AND cle.target_column = p_column_name

    UNION

    -- Recursive case: parents of parents
    SELECT
        ds.name AS dataset_name,
        cle.source_column AS column_name,
        cle.transform_type,
        cle.expression,
        u.depth + 1
    FROM upstream u
    JOIN datasets d ON u.dataset_name = d.name
    JOIN column_lineage_edges cle ON cle.target_dataset_id = d.id
                                  AND cle.target_column = u.column_name
    JOIN datasets ds ON cle.source_dataset_id = ds.id
    WHERE u.depth < 10  -- Prevent infinite loops
)
SELECT DISTINCT
    upstream.dataset_name,
    upstream.column_name,
    upstream.transform_type,
    upstream.expression,
    MIN(upstream.depth) AS depth
FROM upstream
GROUP BY upstream.dataset_name, upstream.column_name, upstream.transform_type, upstream.expression
ORDER BY depth, dataset_name, column_name;
$$ LANGUAGE SQL;


-- Get all downstream columns (what uses this column)
CREATE OR REPLACE FUNCTION get_downstream_columns(
    p_dataset_name VARCHAR,
    p_column_name VARCHAR
)
RETURNS TABLE(
    dataset_name VARCHAR,
    column_name VARCHAR,
    transform_type VARCHAR,
    expression TEXT,
    depth INT
) AS $$
WITH RECURSIVE downstream AS (
    -- Base case: direct children
    SELECT
        dt.name AS dataset_name,
        cle.target_column AS column_name,
        cle.transform_type,
        cle.expression,
        1 AS depth
    FROM column_lineage_edges cle
    JOIN datasets ds ON cle.source_dataset_id = ds.id
    JOIN datasets dt ON cle.target_dataset_id = dt.id
    WHERE ds.name = p_dataset_name
      AND cle.source_column = p_column_name

    UNION

    -- Recursive case: children of children
    SELECT
        dt.name AS dataset_name,
        cle.target_column AS column_name,
        cle.transform_type,
        cle.expression,
        d.depth + 1
    FROM downstream d
    JOIN datasets ds ON d.dataset_name = ds.name
    JOIN column_lineage_edges cle ON cle.source_dataset_id = ds.id
                                  AND cle.source_column = d.column_name
    JOIN datasets dt ON cle.target_dataset_id = dt.id
    WHERE d.depth < 10  -- Prevent infinite loops
)
SELECT DISTINCT
    downstream.dataset_name,
    downstream.column_name,
    downstream.transform_type,
    downstream.expression,
    MIN(downstream.depth) AS depth
FROM downstream
GROUP BY downstream.dataset_name, downstream.column_name, downstream.transform_type, downstream.expression
ORDER BY depth, dataset_name, column_name;
$$ LANGUAGE SQL;
