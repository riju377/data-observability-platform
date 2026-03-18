-- Migration: Drop unused columns from jobs table
-- description: always NULL, never populated
-- metadata: only stored {"application_id": "..."} which duplicates last_execution_id

ALTER TABLE jobs DROP COLUMN IF EXISTS description;
ALTER TABLE jobs DROP COLUMN IF EXISTS metadata;
