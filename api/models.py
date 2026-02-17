"""
Pydantic models for API request/response validation
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum


# ============================================
# Dataset Models
# ============================================

class DatasetType(str, Enum):
    """Types of datasets"""
    TABLE = "table"
    VIEW = "view"
    FILE = "file"
    API = "api"


class Dataset(BaseModel):
    """Dataset information"""
    id: str
    name: str
    dataset_type: Optional[str] = None
    location: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "name": "bronze.taxi_trips",
                "dataset_type": "table",
                "location": "s3://bucket/bronze/taxi_trips",
                "created_at": "2024-01-20T10:30:00",
                "updated_at": "2024-01-24T15:45:00"
            }
        }


# ============================================
# Lineage Models
# ============================================

class LineageEdge(BaseModel):
    """Edge in the lineage graph"""
    id: str
    source_dataset_id: str
    target_dataset_id: str
    source_dataset_name: str
    target_dataset_name: str
    job_id: Optional[str] = None
    job_name: Optional[str] = None
    created_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": "456e7890-e89b-12d3-a456-426614174001",
                "source_dataset_id": "123e4567-e89b-12d3-a456-426614174000",
                "target_dataset_id": "789e0123-e89b-12d3-a456-426614174002",
                "source_dataset_name": "raw.taxi_trips",
                "target_dataset_name": "bronze.taxi_trips",
                "job_id": "job-123",
                "job_name": "IngestTaxiData",
                "created_at": "2024-01-24T15:45:00"
            }
        }


class LineageGraph(BaseModel):
    """Complete lineage graph for a dataset"""
    dataset: Dataset
    upstream: List[Dataset]
    downstream: List[Dataset]
    edges: List[LineageEdge]

    class Config:
        json_schema_extra = {
            "example": {
                "dataset": {
                    "id": "123",
                    "name": "bronze.taxi_trips",
                    "dataset_type": "table",
                    "location": "s3://bucket/bronze/taxi",
                    "created_at": "2024-01-20T10:30:00",
                    "updated_at": "2024-01-24T15:45:00"
                },
                "upstream": [],
                "downstream": [{"id": "456", "name": "silver.enriched_trips"}],
                "edges": []
            }
        }


class DatasetDependency(BaseModel):
    """Dataset with depth information for recursive queries"""
    dataset_name: str
    depth: int

    class Config:
        json_schema_extra = {
            "example": {
                "dataset_name": "silver.enriched_trips",
                "depth": 1
            }
        }


# ============================================
# Column Lineage Models
# ============================================

class TransformType(str, Enum):
    """Types of column transformations"""
    DIRECT = "DIRECT"           # Direct column mapping
    EXPRESSION = "EXPRESSION"   # Calculated expression
    AGGREGATE = "AGGREGATE"     # Aggregation (SUM, COUNT, etc.)
    JOIN = "JOIN"               # Join key
    FILTER = "FILTER"           # Used in WHERE clause
    CASE = "CASE"               # CASE WHEN expression


class ColumnLineageEdge(BaseModel):
    """Edge in the column lineage graph"""
    id: str
    source_dataset_id: str
    source_dataset_name: Optional[str] = None
    source_column: str
    target_dataset_id: str
    target_dataset_name: Optional[str] = None
    target_column: str
    transform_type: str
    expression: Optional[str] = None
    job_id: Optional[str] = None
    created_at: Optional[datetime] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "edge-123",
                "source_dataset_id": "ds-1",
                "source_dataset_name": "bronze_taxi_trips",
                "source_column": "fare_amount",
                "target_dataset_id": "ds-2",
                "target_dataset_name": "gold_daily_metrics",
                "target_column": "total_revenue",
                "transform_type": "AGGREGATE",
                "expression": "SUM(fare_amount)"
            }
        }


class ColumnDependency(BaseModel):
    """Column with lineage information"""
    dataset_name: str
    column_name: str
    transform_type: Optional[str] = None
    expression: Optional[str] = None
    depth: int

    class Config:
        json_schema_extra = {
            "example": {
                "dataset_name": "bronze_taxi_trips",
                "column_name": "fare_amount",
                "transform_type": "AGGREGATE",
                "expression": "SUM(fare_amount)",
                "depth": 1
            }
        }


class ColumnLineageGraph(BaseModel):
    """Complete column lineage graph"""
    dataset_name: str
    column_name: str
    upstream: List[ColumnDependency]
    downstream: List[ColumnDependency]
    edges: List[ColumnLineageEdge]

    class Config:
        json_schema_extra = {
            "example": {
                "dataset_name": "gold_daily_metrics",
                "column_name": "total_revenue",
                "upstream": [
                    {"dataset_name": "silver_enriched_trips", "column_name": "total_amount", "depth": 1},
                    {"dataset_name": "bronze_taxi_trips", "column_name": "fare_amount", "depth": 2}
                ],
                "downstream": [],
                "edges": []
            }
        }


# ============================================
# Impact Analysis Models
# ============================================

class ImpactedColumn(BaseModel):
    """A column that would be impacted by a change"""
    dataset_name: str
    column_name: str
    transform_type: Optional[str] = None
    depth: int

class ImpactedDataset(BaseModel):
    """A dataset with its impacted columns"""
    dataset_name: str
    dataset_type: Optional[str] = None
    columns: List[ImpactedColumn]
    total_columns_affected: int
    max_depth: int

class ImpactAnalysis(BaseModel):
    """Complete impact analysis for a column"""
    source_dataset: str
    source_column: str
    impact_summary: Dict[str, Any]
    impacted_datasets: List[ImpactedDataset]
    total_datasets_affected: int
    total_columns_affected: int
    max_depth: int
    criticality: str  # LOW, MEDIUM, HIGH, CRITICAL

    class Config:
        json_schema_extra = {
            "example": {
                "source_dataset": "bronze_taxi_trips",
                "source_column": "fare_amount",
                "impact_summary": {
                    "direct_dependents": 2,
                    "transitive_dependents": 5
                },
                "impacted_datasets": [
                    {
                        "dataset_name": "silver_enriched_trips",
                        "columns": [{"dataset_name": "silver_enriched_trips", "column_name": "total_amount", "depth": 1}],
                        "total_columns_affected": 1,
                        "max_depth": 1
                    }
                ],
                "total_datasets_affected": 3,
                "total_columns_affected": 7,
                "max_depth": 3,
                "criticality": "HIGH"
            }
        }


# ============================================
# Schema Models
# ============================================

class SchemaField(BaseModel):
    """Schema field definition"""
    name: str
    type: str
    nullable: bool
    comment: Optional[str] = None


class FieldChange(BaseModel):
    """A change to a schema field"""
    field_name: str
    change_type: str  # ADDED, REMOVED, MODIFIED
    old_value: Optional[Dict[str, Any]] = None
    new_value: Optional[Dict[str, Any]] = None


class SchemaDiff(BaseModel):
    """Difference between two schema versions"""
    dataset_name: str
    from_version: str
    to_version: str
    from_date: datetime
    to_date: datetime
    changes: List[FieldChange]
    added_count: int
    removed_count: int
    modified_count: int
    is_breaking: bool  # True if any removals or type changes

    class Config:
        json_schema_extra = {
            "example": {
                "dataset_name": "bronze_taxi_trips",
                "from_version": "v1",
                "to_version": "v2",
                "from_date": "2026-01-15T10:00:00",
                "to_date": "2026-01-20T15:00:00",
                "changes": [
                    {"field_name": "new_column", "change_type": "ADDED", "new_value": {"type": "string", "nullable": True}},
                    {"field_name": "old_column", "change_type": "REMOVED", "old_value": {"type": "int", "nullable": False}}
                ],
                "added_count": 1,
                "removed_count": 1,
                "modified_count": 0,
                "is_breaking": True
            }
        }


class SchemaVersion(BaseModel):
    """Schema version with SCD Type 2 tracking"""
    id: str
    dataset_id: str
    schema_data: Dict[str, Any]
    schema_hash: str
    valid_from: datetime
    valid_to: Optional[datetime] = None
    change_type: Optional[str] = None
    change_description: Optional[str] = None
    is_current: bool = Field(default=False, description="True if valid_to is NULL")

    class Config:
        json_schema_extra = {
            "example": {
                "id": "schema-123",
                "dataset_id": "dataset-456",
                "schema_data": {
                    "fields": [
                        {"name": "id", "type": "int", "nullable": False},
                        {"name": "name", "type": "string", "nullable": True}
                    ]
                },
                "schema_hash": "abc123def456",
                "valid_from": "2024-01-20T10:30:00",
                "valid_to": None,
                "change_type": "ADDED_COLUMN",
                "change_description": "Added email column",
                "is_current": True
            }
        }


# ============================================
# Metrics Models
# ============================================

class DatasetMetrics(BaseModel):
    """Dataset metrics snapshot"""
    id: str
    dataset_id: str
    job_id: Optional[str] = None
    timestamp: datetime
    row_count: Optional[int] = None
    byte_size: Optional[int] = None
    file_count: Optional[int] = None
    execution_time_ms: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "metric-123",
                "dataset_id": "dataset-456",
                "job_id": "job-789",
                "timestamp": "2024-01-24T15:45:00",
                "row_count": 1000000,
                "byte_size": 524288000,
                "file_count": 10,
                "execution_time_ms": 45000
            }
        }


class MetricsTimeSeries(BaseModel):
    """Time-series metrics for a dataset"""
    dataset_name: str
    metrics: List[DatasetMetrics]
    statistics: Optional[Dict[str, Any]] = None

    class Config:
        json_schema_extra = {
            "example": {
                "dataset_name": "bronze.taxi_trips",
                "metrics": [],
                "statistics": {
                    "avg_row_count": 1000000,
                    "max_row_count": 1500000,
                    "min_row_count": 800000,
                    "stddev_row_count": 150000
                }
            }
        }


# ============================================
# Anomaly Models
# ============================================

class AnomalySeverity(str, Enum):
    """Anomaly severity levels"""
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class AnomalyStatus(str, Enum):
    """Anomaly status"""
    OPEN = "OPEN"
    ALERTED = "ALERTED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    FALSE_POSITIVE = "FALSE_POSITIVE"


class Anomaly(BaseModel):
    """Detected anomaly"""
    id: str
    dataset_id: str
    dataset_name: Optional[str] = None
    anomaly_type: str
    severity: AnomalySeverity
    detected_at: datetime
    expected_value: Optional[Dict[str, Any]] = None
    actual_value: Optional[Dict[str, Any]] = None
    deviation_score: Optional[float] = None
    job_id: Optional[str] = None
    description: Optional[str] = None
    status: AnomalyStatus = AnomalyStatus.OPEN
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    resolution_notes: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "anomaly-123",
                "dataset_id": "dataset-456",
                "dataset_name": "bronze.taxi_trips",
                "anomaly_type": "ROW_COUNT_DROP",
                "severity": "CRITICAL",
                "detected_at": "2024-01-24T15:45:00",
                "expected_value": {"row_count": 1000000},
                "actual_value": {"row_count": 500000},
                "deviation_score": 3.5,
                "job_id": "job-789",
                "description": "Row count dropped by 50%",
                "status": "OPEN"
            }
        }


# ============================================
# Response Models
# ============================================

class ErrorResponse(BaseModel):
    """Error response"""
    error: str
    detail: Optional[str] = None

    class Config:
        json_schema_extra = {
            "example": {
                "error": "Dataset not found",
                "detail": "No dataset with name 'invalid.table' exists"
            }
        }


class PaginatedResponse(BaseModel):
    """Paginated response wrapper"""
    items: List[Any]
    total: int
    page: int
    page_size: int
    total_pages: int

    class Config:
        json_schema_extra = {
            "example": {
                "items": [],
                "total": 100,
                "page": 1,
                "page_size": 20,
                "total_pages": 5
            }
        }


class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    database: str
    version: str

    class Config:
        json_schema_extra = {
            "example": {
                "status": "healthy",
                "database": "connected",
                "version": "1.0.0"
            }
        }


# ============================================
# Alert Models
# ============================================

# ============================================
# Job Execution & Stage Metrics Models
# ============================================

class Job(BaseModel):
    """Denormalized Job Model (replacing JobExecution)"""
    id: str
    organization_id: Optional[str] = None
    job_name: str
    description: Optional[str] = None
    status: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    last_execution_id: Optional[str] = None
    execution_metrics: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Computed fields for backward compatibility
    @property
    def duration_ms(self) -> Optional[int]:
        if self.started_at and self.ended_at:
            return int((self.ended_at - self.started_at).total_seconds() * 1000)
        return None

    @property
    def application_id(self) -> Optional[str]:
        if self.metadata and isinstance(self.metadata, dict):
            # Casting to str or checking if key exists
            return self.metadata.get("application_id")
        return None

    @property
    def job_id(self) -> str:
        # Map last_execution_id to job_id for frontend compatibility
        return self.last_execution_id or self.id

    @property
    def total_tasks(self) -> Optional[int]:
        if self.execution_metrics and isinstance(self.execution_metrics, dict):
            return self.execution_metrics.get("totalTasks")
        return None

    @property
    def failed_tasks(self) -> Optional[int]:
        if self.execution_metrics and isinstance(self.execution_metrics, dict):
            return self.execution_metrics.get("failedTasks")
        return None

    @property
    def shuffle_read_bytes(self) -> Optional[int]:
        if self.execution_metrics and isinstance(self.execution_metrics, dict):
            return self.execution_metrics.get("shuffleReadBytes")
        return None

    @property
    def shuffle_write_bytes(self) -> Optional[int]:
        if self.execution_metrics and isinstance(self.execution_metrics, dict):
            return self.execution_metrics.get("shuffleWriteBytes")
        return None

    class Config:
        json_schema_extra = {
            "example": {
                "id": "job-uuid-123",
                "job_name": "Daily ETL",
                "status": "SUCCESS",
                "started_at": "2024-01-20T10:00:00",
                "ended_at": "2024-01-20T10:05:00",
                "last_execution_id": "spark-app-123",
                "execution_metrics": {
                    "totalTasks": 100,
                    "shuffleReadBytes": 1024000
                }
            }
        }


class ChannelType(str, Enum):
    """Supported alert channels"""
    EMAIL = "EMAIL"
    SLACK = "SLACK"
    TEAMS = "TEAMS"
    WEBHOOK = "WEBHOOK"


class EmailConfig(BaseModel):
    """Configuration for Email Channel"""
    email_to: List[str]
    email_cc: Optional[List[str]] = None
    subject_template: Optional[str] = None


class SlackConfig(BaseModel):
    """Configuration for Slack Channel"""
    webhook_url: str
    channel: Optional[str] = None
    username: Optional[str] = None


class TeamsConfig(BaseModel):
    """Configuration for MS Teams Channel"""
    webhook_url: str


class WebhookConfig(BaseModel):
    """Configuration for Generic Webhook"""
    url: str
    headers: Optional[Dict[str, str]] = None
    method: str = "POST"


class AlertRuleCreate(BaseModel):
    """Request model for creating alert rule"""
    name: str
    description: Optional[str] = None
    dataset_name: Optional[str] = None
    anomaly_type: Optional[str] = None
    severity: Optional[str] = None
    channel_type: ChannelType
    channel_config: Dict[str, Any]  # Validated at runtime based on type
    enabled: bool = True
    deduplication_minutes: int = 60

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Critical Anomalies",
                "description": "Alert on all CRITICAL severity anomalies",
                "severity": "CRITICAL",
                "channel_type": "SLACK",
                "channel_config": {"webhook_url": "https://hooks.slack.com/..."},
                "enabled": True,
                "deduplication_minutes": 60
            }
        }


class AlertRule(BaseModel):
    """Alert rule response"""
    id: str
    name: str
    description: Optional[str]
    dataset_name: Optional[str]
    anomaly_type: Optional[str]
    severity: Optional[str]
    channel_type: ChannelType
    channel_config: Dict[str, Any]
    enabled: bool
    deduplication_minutes: int
    created_at: datetime
    updated_at: datetime

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "name": "Critical Anomalies",
                "description": "Alert on all CRITICAL severity anomalies",
                "dataset_name": None,
                "anomaly_type": None,
                "severity": "CRITICAL",
                "channel_type": "EMAIL",
                "channel_config": {"email_to": ["data-team@example.com"]},
                "enabled": True,
                "deduplication_minutes": 60,
                "created_at": "2026-01-27T10:00:00",
                "updated_at": "2026-01-27T10:00:00"
            }
        }


class AlertHistory(BaseModel):
    """Alert history response"""
    id: str
    alert_rule_id: Optional[str]
    anomaly_id: str
    channel_type: str
    channel_context: Optional[Dict[str, Any]]
    response_payload: Optional[Dict[str, Any]]
    status: str
    sent_at: datetime
    error_message: Optional[str]

    class Config:
        json_schema_extra = {
            "example": {
                "id": "123e4567-e89b-12d3-a456-426614174000",
                "alert_rule_id": "rule-456",
                "anomaly_id": "anomaly-789",
                "channel_type": "SLACK",
                "channel_context": {"webhook_url": "..."},
                "response_payload": {"ok": True},
                "status": "SENT",
                "sent_at": "2026-01-27T10:00:00",
                "error_message": None
            }
        }