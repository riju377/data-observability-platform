import { memo } from 'react';
import { Handle, Position } from 'reactflow';
import { Database, FileText } from 'lucide-react';
import CopyButton from './CopyButton';
import './ColumnLineageNode.css';

function getDatasetIcon(datasetType) {
  switch (datasetType?.toLowerCase()) {
    case 'file':
    case 'csv':
    case 'parquet':
    case 'json':
      return FileText;
    default:
      return Database;
  }
}

function ColumnLineageNode({ data }) {
  const {
    displayName, bucket, location, dataset_type,
    columnName, nodeRole, transformColor,
  } = data;
  const Icon = getDatasetIcon(dataset_type);
  const variant = nodeRole === 'selected' ? 'dark' : 'light';

  return (
    <div
      className={`col-lineage-card col-lineage-card--${nodeRole}`}
      style={nodeRole !== 'selected' && transformColor ? { '--accent-color': transformColor } : undefined}
    >
      <Handle
        type="target"
        position={Position.Left}
        className={`col-lineage-handle col-lineage-handle--${nodeRole}`}
        style={nodeRole !== 'selected' && transformColor ? { background: transformColor } : undefined}
      />

      {/* Header: icon + type badge + bucket + dataset name */}
      <div className="col-lineage-card__header">
        <div className={`col-lineage-card__icon col-lineage-card__icon--${nodeRole}`}
          style={nodeRole !== 'selected' && transformColor ? { background: `${transformColor}15`, color: transformColor } : undefined}
        >
          <Icon size={14} />
        </div>
        {dataset_type && (
          <span className="col-lineage-card__type-badge">{dataset_type}</span>
        )}
        {bucket && (
          <span className="col-lineage-card__bucket-badge" title={bucket}>{bucket}</span>
        )}
        <div className="col-lineage-card__dataset-name">
          <span data-tooltip={displayName}>{displayName}</span>
          <CopyButton value={displayName} variant={variant} />
        </div>
      </div>

      {/* Location path */}
      {location && (
        <div className="col-lineage-card__path-row">
          <span className="col-lineage-card__path" data-tooltip={location}>
            {location}
          </span>
          <CopyButton value={location} variant={variant} />
        </div>
      )}

      {/* Column section - visually distinct */}
      <div className="col-lineage-card__column-section">
        <div className="col-lineage-card__column-label">Column</div>
        <div className="col-lineage-card__column-row">
          <code className="col-lineage-card__column-name" data-tooltip={columnName}>{columnName}</code>
          <CopyButton value={columnName} variant={variant} />
        </div>
      </div>

      <Handle
        type="source"
        position={Position.Right}
        className={`col-lineage-handle col-lineage-handle--${nodeRole}`}
        style={nodeRole !== 'selected' && transformColor ? { background: transformColor } : undefined}
      />
    </div>
  );
}

export default memo(ColumnLineageNode);
