-- Migration: 004_partition_tracking.sql
-- Description: Add partition_key and job_name to dataset_metrics for scoped anomaly detection

-- Add new columns
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dataset_metrics') THEN
        ALTER TABLE dataset_metrics 
        ADD COLUMN IF NOT EXISTS partition_key TEXT,
        ADD COLUMN IF NOT EXISTS job_name TEXT;
    END IF;
END $$;

-- Create index for fast history lookup by scope
-- Create index for fast history lookup by scope
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'dataset_metrics') THEN
        CREATE INDEX IF NOT EXISTS idx_metrics_scope ON dataset_metrics (dataset_id, job_name, partition_key);
    END IF;
END $$;

-- Comment
COMMENT ON COLUMN dataset_metrics.partition_key IS 'Generalized output path signature (e.g. .../country=US/dt=*)';
COMMENT ON COLUMN dataset_metrics.job_name IS 'Name of the job that produced this metric (e.g. Daily_Sales_ETL)';
