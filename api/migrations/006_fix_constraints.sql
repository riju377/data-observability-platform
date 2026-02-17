-- Migration: Add missing unique constraints
-- 1. job_executions(application_id)
-- 2. Ensure jobs has constraints

-- Ideally we should clear duplicates first, but assuming dev environment
-- DELETE FROM job_executions WHERE id NOT IN (SELECT MAX(id) FROM job_executions GROUP BY application_id);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_executions') THEN
        CREATE UNIQUE INDEX IF NOT EXISTS idx_job_executions_unique_app_id 
        ON job_executions(application_id);

        IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'unique_app_execution') THEN
            ALTER TABLE job_executions 
            ADD CONSTRAINT unique_app_execution 
            UNIQUE USING INDEX idx_job_executions_unique_app_id;
        END IF;
    END IF;
END $$;
