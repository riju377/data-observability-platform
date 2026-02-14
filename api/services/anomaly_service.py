"""
Anomaly Detection Service

Replaces the Scala-based anomaly detection.
Analyzes dataset metrics to detect statistical anomalies.
"""
from typing import List, Optional, Tuple
from datetime import datetime
import math

from database import execute_query, execute_single, execute_insert
from models import Anomaly, DatasetMetrics

# Anomaly Types
ROW_COUNT_SPIKE = "RowCountSpike"
ROW_COUNT_DROP = "RowCountDrop"
SCHEMA_CHANGE = "SchemaChange"

class AnomalyService:
    @staticmethod
    def detect_anomalies(
        dataset_name: str, 
        metrics: DatasetMetrics, 
        org_id: str
    ) -> List[dict]:
        """
        Detect anomalies for a dataset based on current metrics and history.
        
        Args:
            dataset_name: Name of the dataset
            metrics: Current metrics
            org_id: Organization ID
            
        Returns:
            List of anomaly dictionaries
        """
        anomalies = []
        
        # Only analyze if we have row count
        if metrics.row_count is None:
            return anomalies
            
        current_row_count = metrics.row_count
        
        # Fetch historical stats (last 30 days)
        stats = AnomalyService._get_historical_stats(dataset_name, org_id)
        
        if not stats or stats['count'] < 5:
             # Need at least 5 data points for meaningful stats
             return anomalies
             
        mean = stats['mean']
        std_dev = stats['stddev'] or 0.0
        
        # Threshold: 3 Standard Deviations (99.7% confidence)
        threshold_sigma = 3.0
        
        upper_bound = mean + (threshold_sigma * std_dev)
        lower_bound = max(0, mean - (threshold_sigma * std_dev))
        
        # 1. Check for Spikes
        if current_row_count > upper_bound:
            deviation_score = (current_row_count - mean) / mean if mean > 0 else 1.0
            anomalies.append({
                "dataset_name": dataset_name,
                "anomaly_type": ROW_COUNT_SPIKE,
                "severity": "WARNING",
                "current_value": float(current_row_count),
                "expected_value": float(mean),
                "threshold": float(threshold_sigma),
                "deviation": deviation_score,
                "message": f"Row count {current_row_count} is {threshold_sigma}σ above mean {mean:.0f}"
            })
            
        # 2. Check for Drops
        elif current_row_count < lower_bound:
             deviation_score = (mean - current_row_count) / mean if mean > 0 else 1.0
             anomalies.append({
                "dataset_name": dataset_name,
                "anomaly_type": ROW_COUNT_DROP,
                "severity": "CRITICAL",  # Drops are usually more serious
                "current_value": float(current_row_count),
                "expected_value": float(mean),
                "threshold": float(threshold_sigma),
                "deviation": deviation_score,
                "message": f"Row count {current_row_count} is {threshold_sigma}σ below mean {mean:.0f}"
            })
            
        # 3. Check for fixed percentage drop (>50%)
        # Useful when stddev is tight but a large relative drop happens
        if mean > 0:
            percent_change = (current_row_count - mean) / mean
            if percent_change < -0.5: 
                # Avoid duplicate alert if already caught by sigma rule
                if not any(a['anomaly_type'] == ROW_COUNT_DROP for a in anomalies):
                    anomalies.append({
                        "dataset_name": dataset_name,
                        "anomaly_type": ROW_COUNT_DROP,
                        "severity": "CRITICAL",
                        "current_value": float(current_row_count),
                        "expected_value": float(mean),
                        "threshold": 0.5,
                        "deviation": abs(percent_change),
                        "message": f"Row count dropped by {abs(percent_change)*100:.1f}% (Mean: {mean:.0f})"
                    })

        return anomalies

    @staticmethod
    def _get_historical_stats(dataset_name: str, org_id: str, days: int = 30) -> Optional[dict]:
        """Fetch mean and stddev for row_count from DB"""
        query = """
            SELECT
                AVG(row_count) as mean,
                STDDEV(row_count) as stddev,
                COUNT(*) as count
            FROM dataset_metrics dm
            JOIN datasets d ON dm.dataset_id = d.id
            WHERE d.name = %s
              AND d.organization_id = %s
              AND dm.timestamp > NOW() - (INTERVAL '1 day' * %s)
              AND dm.row_count IS NOT NULL
        """
        result = execute_single(query, (dataset_name, org_id, days))

        if result and result['count'] > 0:
            return result
        return None

    @staticmethod
    def _generate_schema_description(changes: List[str], change_type: str, column_diff: int) -> str:
        """
        Generate a professional, human-readable description for schema changes.

        Args:
            changes: List of raw change strings
            change_type: BREAKING, NON_BREAKING, or MODIFICATION
            column_diff: Difference in column count (positive = added, negative = removed)

        Returns:
            Professional description string
        """
        if not changes:
            return "No schema changes detected."

        # Count different types of changes
        added_cols = [c for c in changes if "Column added:" in c]
        removed_cols = [c for c in changes if "Column removed:" in c]
        type_changes = [c for c in changes if "Type changed:" in c]
        null_to_not_null = [c for c in changes if "NULLABLE -> NOT NULL" in c]
        not_null_to_null = [c for c in changes if "NOT NULL -> NULLABLE" in c]

        parts = []

        # Summary statement based on severity
        if change_type == "BREAKING":
            parts.append("⚠️ Breaking schema changes detected that may impact downstream consumers.")
        elif change_type == "NON_BREAKING":
            parts.append("Schema has been extended with new columns.")
        else:
            parts.append("Schema modifications detected.")

        # Detail the changes in natural language
        details = []
        if added_cols:
            col_names = [c.split(":")[1].strip().split("(")[0].strip() for c in added_cols]
            if len(col_names) == 1:
                details.append(f"Added 1 new column: '{col_names[0]}'")
            else:
                col_list = ', '.join([f"'{c}'" for c in col_names])
                details.append(f"Added {len(col_names)} new columns: {col_list}")

        if removed_cols:
            col_names = [c.split(":")[1].strip().split("(")[0].strip() for c in removed_cols]
            if len(col_names) == 1:
                details.append(f"Removed column '{col_names[0]}'")
            else:
                col_list = ', '.join([f"'{c}'" for c in col_names])
                details.append(f"Removed {len(col_names)} columns: {col_list}")

        if type_changes:
            col_names = [c.split(":")[1].strip().split("(")[0].strip() for c in type_changes]
            if len(col_names) == 1:
                details.append(f"Data type changed for column '{col_names[0]}'")
            else:
                col_list = ', '.join([f"'{c}'" for c in col_names])
                details.append(f"Data types changed for {len(col_names)} columns: {col_list}")

        if null_to_not_null:
            col_names = [c.split(":")[1].strip().split("(")[0].strip() for c in null_to_not_null]
            if len(col_names) == 1:
                details.append(f"Column '{col_names[0]}' no longer accepts null values")
            else:
                col_list = ', '.join([f"'{c}'" for c in col_names])
                details.append(f"{len(col_names)} columns no longer accept null values: {col_list}")

        if not_null_to_null:
            col_names = [c.split(":")[1].strip().split("(")[0].strip() for c in not_null_to_null]
            if len(col_names) == 1:
                details.append(f"Column '{col_names[0]}' now accepts null values")
            else:
                col_list = ', '.join([f"'{c}'" for c in col_names])
                details.append(f"{len(col_names)} columns now accept null values: {col_list}")

        if details:
            parts.append(" ".join(details))

        return " ".join(parts)

    @staticmethod
    def detect_schema_change(
        dataset_name: str,
        old_fields: List[dict],
        new_fields: List[dict],
    ) -> Tuple[List[dict], str, str]:
        """
        Compare two schema versions and produce anomalies for each change.

        Args:
            dataset_name: Name of the dataset
            old_fields: Previous schema fields [{"name", "type", "nullable"}, ...]
            new_fields: New schema fields

        Returns:
            (anomalies_list, change_type, change_description)
        """
        old_map = {f["name"]: f for f in old_fields}
        new_map = {f["name"]: f for f in new_fields}

        old_names = set(old_map.keys())
        new_names = set(new_map.keys())

        changes: List[str] = []
        breaking = False

        # Columns added
        added = new_names - old_names
        diffs = []
        
        for col in sorted(added):
            dtype = new_map[col].get('type', '?')
            changes.append(f"Column added: {col} ({dtype})")
            diffs.append({
                "action": "COLUMN_ADDED",
                "column": col,
                "details": f"Type: {dtype}",
                "severity": "info"
            })

        # Columns removed (breaking)
        removed = old_names - new_names
        for col in sorted(removed):
            dtype = old_map[col].get('type', '?')
            changes.append(f"Column removed: {col} (was {dtype})")
            breaking = True
            diffs.append({
                "action": "COLUMN_REMOVED",
                "column": col,
                "details": f"Was: {dtype}",
                "severity": "critical"
            })

        # Type / nullability changes on surviving columns
        for col in sorted(old_names & new_names):
            old_type = old_map[col].get("type", "")
            new_type = new_map[col].get("type", "")
            old_null = old_map[col].get("nullable", True)
            new_null = new_map[col].get("nullable", True)

            if old_type != new_type:
                changes.append(f"Type changed: {col} ({old_type} -> {new_type})")
                breaking = True
                diffs.append({
                    "action": "TYPE_CHANGED",
                    "column": col,
                    "details": f"{old_type} → {new_type}",
                    "severity": "warning"
                })
                
            if old_null != new_null:
                if new_null:
                    changes.append(f"Nullability changed: {col} (NOT NULL -> NULLABLE)")
                    diffs.append({
                        "action": "NULLABILITY_CHANGED",
                        "column": col,
                        "details": "NOT NULL → NULLABLE",
                        "severity": "info"
                    })
                else:
                    changes.append(f"Nullability changed: {col} (NULLABLE -> NOT NULL)")
                    breaking = True
                    diffs.append({
                        "action": "NULLABILITY_CHANGED",
                        "column": col,
                        "details": "NULLABLE → NOT NULL",
                        "severity": "critical"
                    })

        if not changes:
            return [], "UNCHANGED", ""

        # Classify
        if removed or any("Type changed" in c for c in changes) or any("NOT NULL" in c and "->" in c for c in changes):
            change_type = "BREAKING"
            severity = "CRITICAL"
        elif added:
            change_type = "NON_BREAKING"
            severity = "WARNING"
        else:
            change_type = "MODIFICATION"
            severity = "WARNING"

        # Generate professional, human-readable description
        description = AnomalyService._generate_schema_description(changes, change_type, len(new_fields) - len(old_fields))

        anomalies = [{
            "dataset_name": dataset_name,
            "anomaly_type": SCHEMA_CHANGE,
            "severity": severity,
            "current_value": {
                "count": len(new_fields),
                "diff": diffs
            },
            "expected_value": {
                "count": len(old_fields)
            },
            "threshold": 0,
            "deviation": len(changes),
            "message": f"Schema {change_type.lower().replace('_', '-')}: {description}",
        }]

        return anomalies, change_type, description
