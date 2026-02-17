-- Migration: Simplify Jobs Schema (Denormalize)
-- 1. Add execution-related columns directly to jobs table
-- 2. Drop job_executions and stage_metrics tables

-- Add execution columns to jobs
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS status VARCHAR(50);
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS metadata JSONB;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS ended_at TIMESTAMP;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_execution_id VARCHAR(200); -- Spark's job_id/app_id
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS execution_metrics JSONB;

-- Since the user wants to "keep all in same table", we can drop the normalized tables
-- Dependencies first
DROP TABLE IF EXISTS stage_metrics;
DROP TABLE IF EXISTS job_executions CASCADE;

-- Note: We retain uniqueness on (organization_id, job_name) from previous migrations
