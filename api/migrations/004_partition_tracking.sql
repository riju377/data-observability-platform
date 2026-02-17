-- Migration: 004_partition_tracking.sql
-- Description: Add partition_key and job_name to dataset_metrics for scoped anomaly detection

-- Add new columns
ALTER TABLE dataset_metrics 
ADD COLUMN IF NOT EXISTS partition_key TEXT,
ADD COLUMN IF NOT EXISTS job_name TEXT;

-- Create index for fast history lookup by scope
CREATE INDEX IF NOT EXISTS idx_metrics_scope ON dataset_metrics (dataset_id, job_name, partition_key);

-- Comment
COMMENT ON COLUMN dataset_metrics.partition_key IS 'Generalized output path signature (e.g. .../country=US/dt=*)';
COMMENT ON COLUMN dataset_metrics.job_name IS 'Name of the job that produced this metric (e.g. Daily_Sales_ETL)';
