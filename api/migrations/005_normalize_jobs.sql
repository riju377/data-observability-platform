-- Migration: Normalize Jobs and Job Executions
-- 1. Create 'jobs' table for logical definitions
-- 2. Link 'job_executions' to 'jobs'

-- 1. Create jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_id UUID REFERENCES organizations(id),
    job_name VARCHAR(500) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(organization_id, job_name)
);

CREATE INDEX IF NOT EXISTS idx_jobs_org_name ON jobs(organization_id, job_name);

-- 2. Add job_id_ref column to job_executions (temporary name to avoid conflict with existing job_id string)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_executions') THEN
        ALTER TABLE job_executions ADD COLUMN IF NOT EXISTS job_definition_id UUID REFERENCES jobs(id);
    END IF;
END $$;

-- 3. Backfill jobs from existing job_executions (Best effort)
-- We assume 'default' organization for migration if not strictly known, but we can try to infer or leave null.
-- Actually, we can infer organization_id from the context if we had it, but job_executions doesn't have org_id directly (it's in dataset_metrics).
-- Let's just create the column for now.

-- 4. Make application_id the main identifier for job_executions
-- We need to ensure application_id is present and used for uniqueness
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'job_executions') THEN
        CREATE INDEX IF NOT EXISTS idx_job_executions_app_id ON job_executions(application_id);
    END IF;
END $$;
-- We can't easily convert the Primary Key without dropping the old one, but we can add a unique constraint
-- ADD CONSTRAINT unique_app_execution UNIQUE (application_id); -- This might fail if duplicates exist or nulls.
