"""
Anomaly Detection Service

Replaces the Scala-based anomaly detection.
Analyzes dataset metrics to detect statistical anomalies.
"""
from typing import List, Optional, Tuple
from datetime import datetime
import math
from decimal import Decimal

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
        org_id: str,
        job_name: Optional[str] = None,
        partition_key: Optional[str] = None,
        job_id: Optional[str] = None
    ) -> List[dict]:
        """
        Detect anomalies for a dataset based on current metrics and history.
        
        Args:
            dataset_name: Name of the dataset
            metrics: Current metrics
            org_id: Organization ID
            job_name: Name of the job (optional but recommended for scoping)
            partition_key: Partition signature (optional but recommended for scoping)
            job_id: Current Job ID (to exclude from history)
            
        Returns:
            List of anomaly dictionaries
        """
        anomalies = []
        
        # Determine scoping strategy
        # Ideally, we scope by (dataset_id, job_name, partition_key)
        # If partition_key is missing, fall back to global or job-only
        
        # Fetch historical stats
        stats = AnomalyService._get_historical_stats(dataset_name, org_id, job_name, partition_key, exclude_job_id=job_id)
        
        if not stats or stats['count'] < 5:
             # Need at least 5 data points (in the same scope) for meaningful stats
             return anomalies
             
        # =========================================================
        # Row Count Anomalies (Scoped)
        # =========================================================
        if metrics.row_count is not None:
            mean = float(stats['avg_row_count'])
            std_dev = float(stats['stddev_row_count'] or 0.0)
            threshold_sigma = 3.0
            
            upper_bound = mean + (threshold_sigma * std_dev)
            lower_bound = max(0.0, mean - (threshold_sigma * std_dev))
            
            current = metrics.row_count
            
            # Construct context string for message
            context_str = ""
            if partition_key and partition_key != "UNKNOWN":
                context_str = f" in partition '{partition_key}'"
            elif job_name:
                context_str = f" for job '{job_name}'"

            if current > upper_bound:
                deviation = (current - mean) / mean if mean > 0 else 1.0
                anomalies.append({
                    "dataset_name": dataset_name,
                    "anomaly_type": ROW_COUNT_SPIKE,
                    "severity": "WARNING",
                    "current_value": float(current),
                    "expected_value": float(mean),
                    "threshold": float(threshold_sigma),
                    "deviation": deviation,
                    "message": f"Row count spike{context_str}: {current} (Expected ~{mean:.0f}, >{threshold_sigma}σ)"
                })
            elif current < lower_bound:
                 deviation = (mean - current) / mean if mean > 0 else 1.0
                 anomalies.append({
                    "dataset_name": dataset_name,
                    "anomaly_type": ROW_COUNT_DROP,
                    "severity": "CRITICAL",
                    "current_value": float(current),
                    "expected_value": float(mean),
                    "threshold": float(threshold_sigma),
                    "deviation": deviation,
                    "message": f"Row count drop{context_str}: {current} (Expected ~{mean:.0f}, >{threshold_sigma}σ)"
                })

        # =========================================================
        # Volume Anomalies (Scoped)
        # =========================================================
        if metrics.byte_size is not None:
            mean_vol = float(stats['avg_byte_size'])
            std_dev_vol = float(stats['stddev_byte_size'] or 0.0)
            threshold_sigma_vol = 3.0
            
            # If stddev is very small (stable history), enforcing rigid sigma might cause false positives 
            # on small fluctuations. Ensure min stddev or min deviation?
            # For now, standard 3-sigma.
            
            upper_vol = mean_vol + (threshold_sigma_vol * std_dev_vol)
            lower_vol = max(0.0, mean_vol - (threshold_sigma_vol * std_dev_vol))
            
            current_vol = metrics.byte_size
            
            # Helper to format bytes
            def fmt_bytes(b):
                for unit in ['B', 'KB', 'MB', 'GB']:
                    if b < 1024: return f"{b:.1f}{unit}"
                    b /= 1024
                return f"{b:.1f}TB"

            context_str = ""
            if partition_key and partition_key != "UNKNOWN":
                context_str = f" in partition '{partition_key}'"
            elif job_name:
                context_str = f" for job '{job_name}'"

            # Check Spikes
            if current_vol > upper_vol:
                deviation = (current_vol - mean_vol) / mean_vol if mean_vol > 0 else 1.0
                anomalies.append({
                    "dataset_name": dataset_name,
                    "anomaly_type": "VolumeSpike", # String constant not yet defined, using literal
                    "severity": "WARNING",
                    "current_value": float(current_vol),
                    "expected_value": float(mean_vol),
                    "threshold": float(threshold_sigma_vol),
                    "deviation": deviation,
                    "message": f"Volume spike{context_str}: {fmt_bytes(current_vol)} (Expected ~{fmt_bytes(mean_vol)})"
                })
            # Check Drops
            elif current_vol < lower_vol:
                 deviation = (mean_vol - current_vol) / mean_vol if mean_vol > 0 else 1.0
                 anomalies.append({
                    "dataset_name": dataset_name,
                    "anomaly_type": "VolumeDrop",
                    "severity": "CRITICAL",
                    "current_value": float(current_vol),
                    "expected_value": float(mean_vol),
                    "threshold": float(threshold_sigma_vol),
                    "deviation": deviation,
                    "message": f"Volume drop{context_str}: {fmt_bytes(current_vol)} (Expected ~{fmt_bytes(mean_vol)})"
                })

        return anomalies

    @staticmethod
    def _get_historical_stats(
        dataset_name: str, 
        org_id: str, 
        job_name: Optional[str] = None, 
        partition_key: Optional[str] = None,
        exclude_job_id: Optional[str] = None,
        days: int = 30
    ) -> Optional[dict]:
        """Fetch scoped stats (Row Count AND Byte Size) from DB"""
        
        # Build query dynamically based on available scope
        where_clauses = [
            "d.name = %s",
            "d.organization_id = %s",
            "dm.timestamp > NOW() - (INTERVAL '1 day' * %s)"
        ]
        params = [dataset_name, org_id, days]
        
        if exclude_job_id:
            where_clauses.append("dm.job_id != %s")
            params.append(exclude_job_id)

        if partition_key and partition_key != "UNKNOWN":
            where_clauses.append("dm.partition_key = %s")
            params.append(partition_key)
        elif job_name:
            # Fallback to job-level scoping if no partition key
            where_clauses.append("dm.job_name = %s")
            params.append(job_name)
            
        query = f"""
            SELECT
                COUNT(*) as count,
                AVG(row_count) as avg_row_count,
                STDDEV(row_count) as stddev_row_count,
                AVG(byte_size) as avg_byte_size,
                STDDEV(byte_size) as stddev_byte_size
            FROM dataset_metrics dm
            JOIN datasets d ON dm.dataset_id = d.id
            WHERE {" AND ".join(where_clauses)}
        """
        
        result = execute_single(query, tuple(params))

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
