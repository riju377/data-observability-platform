-- Migration 008: Add executor configuration columns to jobs table
-- These columns are required by ingest.py but were missing from previous migrations.
-- Without them, job upserts fail silently, causing "last ran" to never update.

DO $$
BEGIN
    -- executor_memory_mb: configured memory per executor (from spark.executor.memory)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='executor_memory_mb') THEN
        ALTER TABLE jobs ADD COLUMN executor_memory_mb BIGINT;
    END IF;

    -- executor_cores: configured cores per executor (from spark.executor.cores)
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='executor_cores') THEN
        ALTER TABLE jobs ADD COLUMN executor_cores INTEGER;
    END IF;
END $$;
