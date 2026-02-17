-- Migration: Simplify Jobs Schema (Denormalize)
-- 1. Add execution-related columns directly to jobs table
-- 2. Drop job_executions and stage_metrics tables

-- Add execution columns to jobs
-- Add execution columns to jobs
-- Note: 'IF NOT EXISTS' in ALTER TABLE ADD COLUMN is only supported in newer Postgres (v9.6+), 
-- which we assume. If older, we'd need a DO block check similar to other migrations.
-- We'll use the DO block pattern for maximum safety and consistency.
DO $$
BEGIN
    -- status
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='status') THEN
        ALTER TABLE jobs ADD COLUMN status VARCHAR(50);
    END IF;

    -- metadata
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='metadata') THEN
        ALTER TABLE jobs ADD COLUMN metadata JSONB;
    END IF;

    -- started_at
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='started_at') THEN
        ALTER TABLE jobs ADD COLUMN started_at TIMESTAMP;
    END IF;

    -- ended_at
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='ended_at') THEN
        ALTER TABLE jobs ADD COLUMN ended_at TIMESTAMP;
    END IF;

    -- last_execution_id
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='last_execution_id') THEN
        ALTER TABLE jobs ADD COLUMN last_execution_id VARCHAR(200); -- Spark's job_id/app_id
    END IF;

    -- execution_metrics
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='execution_metrics') THEN
        ALTER TABLE jobs ADD COLUMN execution_metrics JSONB;
    END IF;
END $$;

-- Since the user wants to "keep all in same table", we can drop the normalized tables
-- Dependencies first
DROP TABLE IF EXISTS stage_metrics;
DROP TABLE IF EXISTS job_executions CASCADE;

-- Note: We retain uniqueness on (organization_id, job_name) from previous migrations
