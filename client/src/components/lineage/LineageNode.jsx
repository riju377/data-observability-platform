import { memo } from 'react';
import { Handle, Position } from 'reactflow';
import { Database, FileText } from 'lucide-react';
import CopyButton from './CopyButton';
import './LineageNode.css';

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

function LineageNode({ data }) {
  const { displayName, bucket, location, dataset_type, nodeRole } = data;
  const Icon = getDatasetIcon(dataset_type);
  const variant = nodeRole === 'selected' ? 'dark' : 'light';

  return (
    <div className={`lineage-card lineage-card--${nodeRole}`}>
      <Handle
        type="target"
        position={Position.Left}
        className={`lineage-handle lineage-handle--${nodeRole}`}
      />

      <div className="lineage-card__header">
        <div className={`lineage-card__icon lineage-card__icon--${nodeRole}`}>
          <Icon size={15} />
        </div>
        {dataset_type && (
          <span className="lineage-card__type-badge">{dataset_type}</span>
        )}
        {bucket && (
          <span className="lineage-card__bucket-badge" title={bucket}>{bucket}</span>
        )}
      </div>

      <div className="lineage-card__name-row">
        <span className="lineage-card__name" data-tooltip={displayName}>
          {displayName}
        </span>
        <CopyButton value={displayName} variant={variant} />
      </div>

      {location && (
        <div className="lineage-card__path-row">
          <span className="lineage-card__path" data-tooltip={location}>
            {location}
          </span>
          <CopyButton value={location} variant={variant} />
        </div>
      )}

      <Handle
        type="source"
        position={Position.Right}
        className={`lineage-handle lineage-handle--${nodeRole}`}
      />
    </div>
  );
}

export default memo(LineageNode);
