"""
Alert Service

Checks detected anomalies against user-defined Alert Rules and dispatches
notifications via Email, Slack, Teams, or Webhook providers.
"""
from typing import List, Optional
import json
from datetime import datetime

from database import execute_query, execute_single, execute_insert
from alerting import AlertFactory


class AlertService:
    @staticmethod
    def check_and_send_alerts(anomalies: List[dict], org_id: str):
        """
        Check anomalies against rules and send alerts.

        Args:
            anomalies: List of anomaly dictionaries (from AnomalyService)
            org_id: Organization ID
        """
        if not anomalies:
            return

        rules = AlertService._get_alert_rules(org_id)
        if not rules:
            print(f"No active alert rules for org {org_id}")
            return

        for anomaly in anomalies:
            anomaly_id = AlertService._persist_anomaly(anomaly, org_id)
            if not anomaly_id:
                continue

            dataset_name = anomaly['dataset_name']
            severity = anomaly['severity']

            matching_rules = [
                r for r in rules
                if AlertService._matches_rule(r, dataset_name, severity)
            ]

            for rule in matching_rules:
                AlertService._dispatch_alert(rule, anomaly, anomaly_id)

    @staticmethod
    def _get_alert_rules(org_id: str) -> List[dict]:
        """Fetch active alert rules, preferring org-specific then global."""
        query = """
            SELECT id::text, name, description, dataset_name, anomaly_type,
                   severity, channel_type, channel_config,
                   enabled, deduplication_minutes
            FROM alert_rules
            WHERE enabled = true
              AND (organization_id = %s OR organization_id IS NULL)
            ORDER BY created_at DESC
        """
        return execute_query(query, (org_id,))

    @staticmethod
    def _matches_rule(rule: dict, dataset_name: str, severity: str) -> bool:
        # Dataset name pattern matching (supports wildcard: "sales_*", "*", or exact)
        pattern = rule.get('dataset_name') or '*'
        if pattern != '*':
            if pattern.endswith('*'):
                if not dataset_name.startswith(pattern[:-1]):
                    return False
            elif dataset_name != pattern:
                return False

        # Severity matching
        rule_severity = rule.get('severity')
        if rule_severity and rule_severity != severity:
            return False

        return True

    @staticmethod
    def _persist_anomaly(anomaly: dict, org_id: str) -> Optional[str]:
        ds = execute_single(
            "SELECT id FROM datasets WHERE name = %s AND organization_id = %s",
            (anomaly['dataset_name'], org_id)
        )
        if not ds:
            print(f"Cannot persist anomaly for unknown dataset: {anomaly['dataset_name']}")
            return None

        dataset_id = ds['id']

        query = """
            INSERT INTO anomalies (
                dataset_id, anomaly_type, severity,
                actual_value, expected_value, deviation_score,
                description, detected_at, organization_id, status
            ) VALUES (
                %s, %s, %s,
                %s::jsonb, %s::jsonb, %s,
                %s, NOW(), %s, 'OPEN'
            ) RETURNING id
        """

        val_json = json.dumps({"value": anomaly['current_value']})
        exp_json = json.dumps({"value": anomaly['expected_value']})

        result = execute_single(query, (
            dataset_id,
            anomaly['anomaly_type'],
            anomaly['severity'],
            val_json,
            exp_json,
            anomaly['deviation'],
            anomaly['message'],
            org_id
        ), commit=True)

        return result['id'] if result else None

    @staticmethod
    def _dispatch_alert(rule: dict, anomaly: dict, anomaly_id: str):
        """Send alert via the configured channel provider and log the result."""
        channel_type = rule['channel_type']
        channel_config = rule.get('channel_config', {})

        # Ensure channel_config is a dict (may come as string from some DB drivers)
        if isinstance(channel_config, str):
            channel_config = json.loads(channel_config)

        # Build anomaly dict in the format providers expect
        provider_anomaly = {
            "id": str(anomaly_id),
            "dataset_name": anomaly['dataset_name'],
            "anomaly_type": anomaly['anomaly_type'],
            "severity": anomaly['severity'],
            "description": anomaly['message'],
            "detected_at": datetime.now().isoformat(),
            "actual_value": {"value": anomaly['current_value']},
            "expected_value": {"value": anomaly['expected_value']},
        }

        print(f"[ALERT] Rule: '{rule['name']}' -> {channel_type}")
        print(f"   [{anomaly['severity']}] {anomaly['dataset_name']}: {anomaly['message']}")

        try:
            provider = AlertFactory.get_provider(channel_type)
            result = provider.send_alert(provider_anomaly, channel_config)
            status = result.get('status', 'FAILED')
            error_message = result.get('error')
        except Exception as e:
            result = {}
            status = "FAILED"
            error_message = str(e)
            print(f"   Alert dispatch error: {e}")

        print(f"   Result: {status}")

        # Log to alert_history
        AlertService._log_alert_history(
            rule_id=rule['id'],
            anomaly_id=anomaly_id,
            channel_type=channel_type,
            channel_config=channel_config,
            response_payload=result,
            status=status,
            error_message=error_message
        )

        # Mark anomaly as alerted
        if status == "SENT":
            try:
                execute_insert(
                    "UPDATE anomalies SET status = 'ALERTED' WHERE id = %s",
                    (anomaly_id,)
                )
            except Exception:
                pass

    @staticmethod
    def _log_alert_history(rule_id, anomaly_id, channel_type,
                           channel_config, response_payload, status,
                           error_message=None):
        """Record alert attempt in alert_history table."""
        try:
            execute_insert("""
                INSERT INTO alert_history
                    (alert_rule_id, anomaly_id, channel_type,
                     channel_context, response_payload, status, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                rule_id,
                anomaly_id,
                channel_type,
                json.dumps(channel_config),
                json.dumps(response_payload),
                status,
                error_message
            ))
        except Exception as e:
            print(f"   Failed to log alert history: {e}")
