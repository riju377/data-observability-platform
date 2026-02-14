"""
Generic Multi-Channel Alerting System

Sends alerts via Email, Slack, Microsoft Teams, or Webhook.
Implements Strategy Pattern for extensibility.
"""

import os
import json
import smtplib
import requests
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Environment, FileSystemLoader
from dotenv import load_dotenv

from models import ChannelType
from database import execute_query, execute_single, execute_insert

# Load environment variables
load_dotenv()


class AlertProvider(ABC):
    """Abstract Base Class for Alert Providers"""

    @abstractmethod
    def send_alert(self, anomaly: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send alert to the channel.
        Returns dict with status ("SENT", "FAILED") and optional response/error info.
        """
        pass


class EmailProvider(AlertProvider):
    """Sender for Email Alerts"""

    def __init__(self):
        self.backend = os.getenv('EMAIL_BACKEND', 'console')
        # SMTP (legacy/blocked on Render free tier)
        self.smtp_host = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('EMAIL_PORT', '587'))
        self.use_tls = os.getenv('EMAIL_USE_TLS', 'true').lower() == 'true'
        self.username = os.getenv('EMAIL_USERNAME')
        self.password = os.getenv('EMAIL_PASSWORD')
        self.from_email = os.getenv('EMAIL_FROM', 'Data Observability <noreply@dataobs.com>')
        
        # Gmail API (OAuth 2.0)
        self.gmail_client_id = os.getenv('GMAIL_CLIENT_ID')
        self.gmail_client_secret = os.getenv('GMAIL_CLIENT_SECRET')
        self.gmail_refresh_token = os.getenv('GMAIL_REFRESH_TOKEN')
        
        self.api_base_url = os.getenv('API_BASE_URL', 'http://localhost:8000')
        self.dashboard_url = os.getenv('DASHBOARD_URL', 'http://localhost:8000')
        self.template_env = Environment(loader=FileSystemLoader('templates'))

    def send_alert(self, anomaly: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        to_emails = config.get('email_to', [])
        cc_emails = config.get('email_cc', [])
        subject_template = config.get('subject_template')
        
        if not to_emails:
            return {"status": "FAILED", "error": "No recipients (email_to) specified"}

        try:
            subject = self._generate_subject(anomaly, subject_template)
            html_body = self._render_email_template(anomaly)

            if self.backend == 'console':
                return self._send_console(to_emails, subject, html_body)
            elif self.backend in ['smtp', 'gmail']:
                return self._send_smtp(to_emails, cc_emails, subject, html_body)
            elif self.backend == 'gmail_api':
                return self._send_gmail_api(to_emails, cc_emails, subject, html_body)
            else:
                return {"status": "FAILED", "error": f"Unknown email backend: {self.backend}"}
        except Exception as e:
            return {"status": "FAILED", "error": str(e)}

    def _generate_subject(self, anomaly: Dict[str, Any], template: Optional[str] = None) -> str:
        if template:
            subject = template
            subject = subject.replace('{dataset_name}', anomaly.get('dataset_name', 'Unknown'))
            subject = subject.replace('{anomaly_type}', anomaly.get('anomaly_type', 'Unknown'))
            subject = subject.replace('{severity}', anomaly.get('severity', 'INFO'))
            return subject

        severity = anomaly.get('severity', 'INFO')
        dataset = anomaly.get('dataset_name', 'Unknown')
        return f"[{severity}] Data Anomaly: {dataset}"

    def _format_anomaly_type(self, anomaly_type: str) -> str:
        """Convert raw anomaly type to a human-readable display name."""
        display_map = {
            'schema_change': 'Schema Change',
            'row_count_anomaly': 'Row Count Anomaly',
            'freshness_sla_breach': 'Freshness SLA Breach',
            'volume_spike': 'Volume Spike',
            'volume_drop': 'Volume Drop',
            'null_rate_spike': 'Null Rate Spike',
            'completeness_drop': 'Completeness Drop',
            'data_drift': 'Data Drift',
        }
        return display_map.get(anomaly_type, anomaly_type.replace('_', ' ').title())

    def _format_detected_at(self, detected_at) -> str:
        """Format detected_at into a clean, readable string."""
        try:
            if isinstance(detected_at, str):
                dt = datetime.fromisoformat(detected_at.replace('Z', '+00:00'))
            elif isinstance(detected_at, datetime):
                dt = detected_at
            else:
                return str(detected_at)
            return dt.strftime('%b %d, %Y %I:%M %p')
        except:
            return str(detected_at)

    def _render_email_template(self, anomaly: Dict[str, Any]) -> str:
        template = self.template_env.get_template('anomaly_alert.html')
        
        # Get downstream count safely
        downstream_count = 0
        try:
            query = "SELECT COUNT(*) as count FROM get_downstream_datasets(%s, %s)"
            result = execute_single(query, (anomaly.get('dataset_name'), anomaly.get('organization_id')))
            downstream_count = result['count'] if result else 0
        except:
            pass

        anomaly_type = anomaly.get('anomaly_type', 'Unknown')
        detected_at = anomaly.get('detected_at', datetime.now().isoformat())
        dataset_name = anomaly.get('dataset_name', 'Unknown')

        return template.render(
            severity=anomaly.get('severity', 'INFO'),
            dataset_name=dataset_name,
            anomaly_type=anomaly_type,
            anomaly_type_display=self._format_anomaly_type(anomaly_type),
            detected_at_display=self._format_detected_at(detected_at),
            description=anomaly.get('description', 'No description available'),
            expected_value=json.dumps(anomaly.get('expected_value', {}), indent=2),
            actual_value=json.dumps(anomaly.get('actual_value', {}), indent=2),
            anomaly_id=anomaly.get('id', 'unknown'),
            downstream_count=downstream_count,
            dashboard_url=self.dashboard_url,
            lineage_url=f"{self.dashboard_url}/lineage",
        )

    def _send_console(self, to_emails, subject, body):
        print("\n" + "="*60 + "\nEMAIL ALERT (Console)\n" + "="*60)
        print(f"To: {to_emails}\nSubject: {subject}")
        print("-" * 60)
        print(body[:300] + "..." if len(body) > 300 else body)
        print("="*60 + "\n")
        return {"status": "SENT", "backend": "console", "message": "Printed to console"}

    def _send_smtp(self, to_emails, cc_emails, subject, html_body):
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.from_email
        msg['To'] = ', '.join(to_emails)
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)
        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
            if self.use_tls:
                server.starttls()
            if self.username and self.password:
                server.login(self.username, self.password)
            server.send_message(msg)
        return {"status": "SENT", "backend": "smtp"}

    def _send_gmail_api(self, to_emails, cc_emails, subject, html_body):
        """Send email via Google Gmail API (OAuth 2.0).
        
        Requires:
        - GMAIL_CLIENT_ID
        - GMAIL_CLIENT_SECRET
        - GMAIL_REFRESH_TOKEN
        """
        if not all([self.gmail_client_id, self.gmail_client_secret, self.gmail_refresh_token]):
            return {"status": "FAILED", "error": "Missing Gmail OAuth credentials"}

        import base64

        # 1. Refresh Access Token
        token_url = "https://oauth2.googleapis.com/token"
        token_data = {
            "client_id": self.gmail_client_id,
            "client_secret": self.gmail_client_secret,
            "refresh_token": self.gmail_refresh_token,
            "grant_type": "refresh_token",
        }
        res = requests.post(token_url, data=token_data)
        if res.status_code != 200:
            return {"status": "FAILED", "error": f"Token refresh failed: {res.text}"}
        
        access_token = res.json().get("access_token")

        # 2. Construct MIME Message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.from_email
        msg['To'] = ', '.join(to_emails)
        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)
        msg.attach(MIMEText(html_body, 'html'))
        
        # Encode as URL-safe base64 string
        raw_message = base64.urlsafe_b64encode(msg.as_bytes()).decode('utf-8')

        # 3. Send Email
        api_url = "https://gmail.googleapis.com/gmail/v1/users/me/messages/send"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {"raw": raw_message}
        
        response = requests.post(api_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            return {"status": "SENT", "backend": "gmail_api", "id": response.json().get("id")}
        else:
            return {"status": "FAILED", "error": f"Gmail API error: {response.text}"}


class SlackProvider(AlertProvider):
    """Sender for Slack Webhook Alerts"""

    def send_alert(self, anomaly: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        webhook_url = config.get('webhook_url')
        if not webhook_url:
            return {"status": "FAILED", "error": "Missing webhook_url"}

        severity = anomaly.get('severity', 'INFO')
        color = {'CRITICAL': '#FF0000', 'WARNING': '#FFA500', 'INFO': '#36C5F0'}.get(severity, '#36C5F0')
        emoji = {'CRITICAL': ':rotating_light:', 'WARNING': ':warning:', 'INFO': ':information_source:'}.get(severity, '')

        payload = {
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{emoji} {severity} Anomaly Detected",
                        "emoji": True
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Dataset:*\n{anomaly.get('dataset_name')}"},
                        {"type": "mrkdwn", "text": f"*Type:*\n{anomaly.get('anomaly_type')}"}
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Description:*\n{anomaly.get('description')}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {"type": "mrkdwn", "text": f"Running on Data Observability Platform • {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                    ]
                }
            ]
        }
        
        try:
            response = requests.post(webhook_url, json=payload, timeout=5)
            if response.status_code == 200:
                return {"status": "SENT", "backend": "slack"}
            else:
                return {"status": "FAILED", "error": f"Slack API Error: {response.text}", "status_code": response.status_code}
        except Exception as e:
            return {"status": "FAILED", "error": str(e)}


class TeamsProvider(AlertProvider):
    """Sender for Microsoft Teams Webhook Alerts"""

    def send_alert(self, anomaly: Dict[str, Any], config: Dict[str, Any]) -> Dict[str, Any]:
        webhook_url = config.get('webhook_url')
        if not webhook_url:
            return {"status": "FAILED", "error": "Missing webhook_url"}

        severity = anomaly.get('severity', 'INFO')
        color = {'CRITICAL': 'Attention', 'WARNING': 'Warning', 'INFO': 'Good'}.get(severity, 'Good') # Teams uses specific color names or hex

        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "summary": f"{severity} Anomaly: {anomaly.get('dataset_name')}",
            "sections": [{
                "activityTitle": f"{severity} Data Anomaly Detected",
                "activitySubtitle": f"Dataset: {anomaly.get('dataset_name')}",
                "facts": [
                    {"name": "Anomaly Type", "value": anomaly.get('anomaly_type')},
                    {"name": "Description", "value": anomaly.get('description')},
                    {"name": "Detected At", "value": str(anomaly.get('detected_at'))}
                ],
                "markdown": True
            }]
        }

        try:
            response = requests.post(webhook_url, json=payload, timeout=5)
            if response.status_code == 200:
                return {"status": "SENT", "backend": "teams"}
            else:
                return {"status": "FAILED", "error": f"Teams API Error: {response.text}"}
        except Exception as e:
            return {"status": "FAILED", "error": str(e)}


class AlertFactory:
    """Factory to get the correct provider"""
    
    @staticmethod
    def get_provider(channel_type: str) -> AlertProvider:
        if channel_type == ChannelType.EMAIL.value:
            return EmailProvider()
        elif channel_type == ChannelType.SLACK.value:
            return SlackProvider()
        elif channel_type == ChannelType.TEAMS.value:
            return TeamsProvider()
        else:
            raise ValueError(f"Unsupported channel type: {channel_type}")


class AlertEngine:
    """Manages alert rules and dispatching"""

    def check_and_send_alerts(self, anomaly: Dict[str, Any]):
        rules = self._get_matching_rules(anomaly)
        
        if not rules:
            print(f"No matching alert rules for anomaly {anomaly.get('id')}")
            return

        for rule in rules:
            if self._is_deduplicated(anomaly, rule):
                print(f"Alert deduplicated for rule '{rule['name']}'")
                self._log_alert(rule['id'], anomaly['id'], rule['channel_type'], rule['channel_config'], {}, "DEDUPLICATED")
                continue

            print(f"Sending alert via rule '{rule['name']}' ({rule['channel_type']})")
            
            try:
                provider = AlertFactory.get_provider(rule['channel_type'])
                result = provider.send_alert(anomaly, rule['channel_config'])
                status = result.get('status', 'FAILED')
                
                print(f"Alert result: {status}")
                
                self._log_alert(
                    rule_id=rule['id'],
                    anomaly_id=anomaly['id'],
                    channel_type=rule['channel_type'],
                    channel_context=rule['channel_config'],
                    response_payload=result,
                    status=status,
                    error_message=result.get('error')
                )
            except Exception as e:
                print(f"Error executing rule {rule['id']}: {e}")
                self._log_alert(
                    rule_id=rule['id'],
                    anomaly_id=anomaly['id'],
                    channel_type=rule['channel_type'],
                    channel_context=rule['channel_config'],
                    response_payload={},
                    status="FAILED",
                    error_message=str(e)
                )

    def _get_matching_rules(self, anomaly: Dict[str, Any]) -> list:
        query = """
            SELECT * FROM alert_rules
            WHERE enabled = true
            AND (dataset_name IS NULL OR dataset_name = %s)
            AND (anomaly_type IS NULL OR anomaly_type = %s)
            AND (severity IS NULL OR severity = %s)
        """
        return execute_query(query, (
            anomaly.get('dataset_name'),
            anomaly.get('anomaly_type'),
            anomaly.get('severity')
        ))

    def _is_deduplicated(self, anomaly: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        # Deduplication based on channel and dataset
        dedup_minutes = rule.get('deduplication_minutes', 60)
        
        query = """
            SELECT COUNT(*) as count FROM alert_history ah
            JOIN anomalies a ON ah.anomaly_id = a.id
            JOIN datasets d ON a.dataset_id = d.id
            WHERE ah.alert_rule_id = %s
            AND d.name = %s
            AND a.anomaly_type = %s
            AND ah.sent_at > NOW() - (%s * INTERVAL '1 minute')
            AND ah.status = 'SENT'
        """
        result = execute_single(query, (
            rule['id'],
            anomaly.get('dataset_name'),
            anomaly.get('anomaly_type'),
            dedup_minutes
        ))
        return result['count'] > 0 if result else False

    def _log_alert(self, rule_id, anomaly_id, channel_type, channel_context, response_payload, status, error_message=None):
        query = """
            INSERT INTO alert_history
            (alert_rule_id, anomaly_id, channel_type, channel_context, response_payload, status, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        execute_insert(query, (
            rule_id,
            anomaly_id,
            channel_type,
            json.dumps(channel_context),
            json.dumps(response_payload),
            status,
            error_message
        ))