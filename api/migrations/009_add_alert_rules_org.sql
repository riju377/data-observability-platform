-- Migration 009: Add organization_id to alert_rules table
-- This column is required for multi-tenancy security (org-scoped alert rules)

DO $$
BEGIN
    -- Add organization_id column to alert_rules
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name='alert_rules' AND column_name='organization_id'
    ) THEN
        -- Add the column (nullable initially)
        ALTER TABLE alert_rules ADD COLUMN organization_id UUID REFERENCES organizations(id);

        -- Backfill: assign existing alert rules to the first organization
        -- In production, you'd want to map rules to correct orgs based on business logic
        UPDATE alert_rules
        SET organization_id = (SELECT id FROM organizations ORDER BY created_at LIMIT 1)
        WHERE organization_id IS NULL;

        -- Make it required
        ALTER TABLE alert_rules ALTER COLUMN organization_id SET NOT NULL;

        -- Add index for performance
        CREATE INDEX idx_alert_rules_org ON alert_rules(organization_id);

        RAISE NOTICE 'Added organization_id column to alert_rules table';
    ELSE
        RAISE NOTICE 'Column alert_rules.organization_id already exists, skipping';
    END IF;
END $$;
