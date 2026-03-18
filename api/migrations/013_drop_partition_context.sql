-- Migration: Drop partition_context from jobs table
-- partition_context (JSONB) is redundant with partition_key (VARCHAR).
-- Country filtering now parses partition_key directly (e.g. "country=US").

ALTER TABLE jobs DROP COLUMN IF EXISTS partition_context;
DROP INDEX IF EXISTS idx_jobs_partition_context;
