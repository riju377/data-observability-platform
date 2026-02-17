-- ============================================
-- Alerting System Migration (Multi-Channel Support)
-- ============================================

-- Alert rules table
-- Drop old tables if exist (Force migration)
DROP TABLE IF EXISTS alert_history;
DROP TABLE IF EXISTS alert_rules;

-- Alert rules table
CREATE TABLE alert_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(200) NOT NULL,
    description TEXT,

    -- Matching criteria (NULL = match all)
    dataset_name VARCHAR(500),              -- NULL = all datasets
    anomaly_type VARCHAR(100),              -- NULL = all types
    severity VARCHAR(20),                   -- CRITICAL, WARNING, INFO

    -- Channel configuration (Generic)
    channel_type VARCHAR(50) NOT NULL,      -- EMAIL, SLACK, TEAMS, WEBHOOK
    channel_config JSONB NOT NULL,          -- { "email_to": [...], "webhook_url": "..." }

    -- Control
    enabled BOOLEAN DEFAULT true,
    deduplication_minutes INT DEFAULT 60,   -- Don't repeat alert within N minutes

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Alert history table
-- Alert history table
CREATE TABLE alert_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_rule_id UUID REFERENCES alert_rules(id) ON DELETE CASCADE,
    anomaly_id UUID REFERENCES anomalies(id) ON DELETE CASCADE,

    -- Channel Snapshot
    channel_type VARCHAR(50) NOT NULL,
    channel_context JSONB,                  -- Snapshot of config used

    -- Response
    response_payload JSONB,                 -- Full response from provider
    
    -- Status
    status VARCHAR(20) NOT NULL,            -- SENT, FAILED, DEDUPLICATED
    sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    error_message TEXT
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_alert_history_anomaly ON alert_history(anomaly_id);
CREATE INDEX IF NOT EXISTS idx_alert_history_sent ON alert_history(sent_at DESC);
CREATE INDEX IF NOT EXISTS idx_alert_history_status ON alert_history(status);
CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled ON alert_rules(enabled);
CREATE INDEX IF NOT EXISTS idx_alert_rules_channel ON alert_rules(channel_type);

-- Sample alert rules
-- 1. Critical Email Alert
INSERT INTO alert_rules (name, description, severity, channel_type, channel_config)
SELECT 'Critical Anomalies (Email)', 'Email alert on all CRITICAL severity anomalies', 'CRITICAL', 'EMAIL',
 '{"email_to": ["your-email@example.com"], "subject_template": "[CRITICAL] Data Anomaly: {dataset_name}"}'::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM alert_rules WHERE name = 'Critical Anomalies (Email)'
);

-- 2. Sales Team Slack Alert
INSERT INTO alert_rules (name, description, dataset_name, severity, channel_type, channel_config)
SELECT 'Sales Alerts (Slack)', 'Slack alert for sales_transactions', 'sales_transactions', 'WARNING', 'SLACK',
 '{"webhook_url": "https://hooks.slack.com/services/XXX/YYY/ZZZ", "channel": "#data-alerts"}'::jsonb
WHERE NOT EXISTS (
    SELECT 1 FROM alert_rules WHERE name = 'Sales Alerts (Slack)'
);