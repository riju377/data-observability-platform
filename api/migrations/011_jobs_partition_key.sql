-- Migration: Add partition_key and partition_context to jobs table
-- Enables per-country (or per-region) job tracking instead of one row per job_name

-- Step 1: Add columns
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='partition_key') THEN
        ALTER TABLE jobs ADD COLUMN partition_key VARCHAR(1000) DEFAULT 'GLOBAL' NOT NULL;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='jobs' AND column_name='partition_context') THEN
        ALTER TABLE jobs ADD COLUMN partition_context JSONB;
    END IF;
END $$;

-- Step 2: Drop old unique constraint, create new one scoped by partition_key
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'jobs_organization_id_job_name_key') THEN
        ALTER TABLE jobs DROP CONSTRAINT jobs_organization_id_job_name_key;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_jobs_org_name_partition') THEN
        ALTER TABLE jobs ADD CONSTRAINT uq_jobs_org_name_partition
            UNIQUE (organization_id, job_name, partition_key);
    END IF;
END $$;

-- Step 3: Indexes for filtering
CREATE INDEX IF NOT EXISTS idx_jobs_partition_key ON jobs(partition_key);
CREATE INDEX IF NOT EXISTS idx_jobs_partition_context ON jobs USING GIN (partition_context);
