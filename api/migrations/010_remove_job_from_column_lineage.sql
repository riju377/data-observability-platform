-- ============================================
-- Remove job_id and job_name from Column Lineage
-- ============================================
-- These columns tracked individual Spark query executions,
-- causing duplicate edges. Column lineage is static metadata
-- and doesn't need per-query tracking.
-- ============================================

-- Drop the existing unique constraint (includes job_id)
ALTER TABLE column_lineage_edges
DROP CONSTRAINT IF EXISTS column_lineage_edges_source_dataset_id_source_column_target_key;

-- Drop job_id and job_name columns
ALTER TABLE column_lineage_edges
DROP COLUMN IF EXISTS job_id,
DROP COLUMN IF EXISTS job_name;

-- Create correct unique constraint (4 columns only)
-- This ensures the same column lineage edge is stored only once
ALTER TABLE column_lineage_edges
ADD CONSTRAINT column_lineage_edges_unique
UNIQUE (source_dataset_id, source_column, target_dataset_id, target_column);
