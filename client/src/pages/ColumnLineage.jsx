import { useState, useEffect, useMemo, useCallback } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType, getBezierPath, EdgeLabelRenderer } from 'reactflow';
import 'reactflow/dist/style.css';
import { getDatasets, getDatasetColumns, getColumnLineageGraph, getColumnImpact } from '../services/api';
import { Network, ChevronDown, Loader2, Database, Code, AlertTriangle, Zap, Target, Copy, Check, Info } from 'lucide-react';
import { getLayoutedElements } from '../utils/layoutGraph';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './ColumnLineage.css';

const TRANSFORM_COLORS = {
  DIRECT: '#4caf50',
  EXPRESSION: '#2196f3',
  AGGREGATE: '#9c27b0',
  JOIN: '#ff9800',
  FILTER: '#f44336',
  CASE: '#00bcd4',
};

const CRITICALITY_COLORS = {
  LOW: '#4caf50',
  MEDIUM: '#ff9800',
  HIGH: '#f44336',
  CRITICAL: '#9c27b0',
};

// Custom edge with hover tooltip for expression
function TooltipEdge({
  id, sourceX, sourceY, targetX, targetY,
  sourcePosition, targetPosition, style, markerEnd, data, label,
  labelStyle, labelBgStyle, labelBgPadding,
}) {
  const [hovered, setHovered] = useState(false);
  const [copied, setCopied] = useState(false);

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX, sourceY, targetX, targetY,
    sourcePosition, targetPosition,
  });

  const transformType = data?.transformType || 'UNKNOWN';
  const expression = data?.expression;
  const edgeColor = TRANSFORM_COLORS[transformType] || '#667eea';

  const handleCopy = () => {
    if (expression) {
      navigator.clipboard.writeText(expression);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  return (
    <>
      {/* Visible edge */}
      <path
        id={id}
        d={edgePath}
        fill="none"
        style={style}
        markerEnd={markerEnd}
      />
      {/* Interactive label with tooltip on hover */}
      <EdgeLabelRenderer>
        <div
          className="edge-label-container"
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents: 'all',
            cursor: 'help',
            zIndex: 9999,
          }}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          {/* Badge with copy overlay on hover */}
          <div
            className="edge-label-badge"
            onClick={(e) => {
              e.stopPropagation();
              if (expression) handleCopy();
            }}
            style={{
              fontSize: 10,
              fontWeight: 700,
              fontFamily: 'system-ui, -apple-system, sans-serif',
              letterSpacing: '0.3px',
              color: '#fff',
              background: copied ? '#4ade80' : edgeColor,
              border: `1.5px solid ${copied ? '#4ade80' : edgeColor}`,
              borderRadius: '5px',
              padding: '3px 8px',
              transition: 'all 0.2s ease',
              boxShadow: hovered ? `0 2px 8px ${edgeColor}60` : `0 1px 4px ${edgeColor}30`,
              display: 'flex',
              alignItems: 'center',
              gap: '4px',
              cursor: expression ? 'pointer' : 'help',
              position: 'relative',
            }}
          >
            {copied ? (
              <>
                <Check size={10} color="#fff" />
                <span style={{ color: '#fff' }}>Copied!</span>
              </>
            ) : (
              <>
                {transformType}
                {hovered && expression ? (
                  <Copy size={10} style={{ color: '#fff', opacity: 0.9 }} />
                ) : (
                  <Info size={10} style={{ color: '#1a1a2e', opacity: 1 }} />
                )}
              </>
            )}
          </div>

          {/* Tooltip on hover */}
          {hovered && (
            <div
              className="edge-tooltip"
              style={{
                position: 'absolute',
                bottom: '100%',
                left: '50%',
                transform: 'translateX(-50%)',
                marginBottom: '12px',
                pointerEvents: 'all',
              }}
            >
              <div className="edge-tooltip-header">
                <span
                  className="edge-tooltip-badge"
                  style={{ background: edgeColor }}
                >
                  {transformType}
                </span>
              </div>
              {expression ? (
                <div className="edge-tooltip-expression">
                  <code>{expression}</code>
                </div>
              ) : transformType === 'DIRECT' ? (
                <div className="edge-tooltip-description">
                  Direct column mapping (no transformation)
                </div>
              ) : null}
            </div>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

const edgeTypes = { tooltipEdge: TooltipEdge };

// Parse composite dataset name "bucket:dataset_name" into parts
function parseDatasetName(name) {
  if (!name) return { bucket: null, displayName: name || '' };
  const colonIdx = name.indexOf(':');
  if (colonIdx > 0 && !name.includes('://')) {
    return {
      bucket: name.substring(0, colonIdx),
      displayName: name.substring(colonIdx + 1),
    };
  }
  return { bucket: null, displayName: name };
}

function ColumnLineage() {
  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState('');
  const [columns, setColumns] = useState([]);
  const [selectedColumn, setSelectedColumn] = useState('');
  const [lineageData, setLineageData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [showImpact, setShowImpact] = useState(false);
  const [impactData, setImpactData] = useState(null);
  const [impactLoading, setImpactLoading] = useState(false);
  const [copiedPath, setCopiedPath] = useState(null);

  useEffect(() => {
    loadDatasets();
  }, []);

  const loadDatasets = async () => {
    try {
      const res = await getDatasets();
      const datasetList = res.data || [];
      setDatasets(datasetList);
      if (datasetList.length > 0) {
        const firstDataset = datasetList[0].name;
        setSelectedDataset(firstDataset);
        loadColumns(firstDataset);
      }
    } catch (error) {
      console.error('Failed to load datasets:', error);
      setDatasets([]);
    }
    setInitialLoading(false);
  };

  const loadColumns = async (datasetName) => {
    try {
      const res = await getDatasetColumns(datasetName);
      const edges = res.data || [];

      const targetColumns = edges
        .filter(e => e.target_dataset_name === datasetName)
        .map(e => e.target_column);

      const sourceColumns = edges
        .filter(e => e.source_dataset_name === datasetName)
        .map(e => e.source_column);

      const uniqueColumns = [...new Set([...targetColumns, ...sourceColumns])];
      const columnList = uniqueColumns.map(col => ({ column_name: col }));

      setColumns(columnList);
      if (columnList.length > 0) {
        const firstColumn = columnList[0].column_name;
        setSelectedColumn(firstColumn);
        loadLineage(datasetName, firstColumn);
      } else {
        setSelectedColumn('');
        setLineageData(null);
      }
    } catch (error) {
      console.error('Failed to load columns:', error);
      setColumns([]);
      setSelectedColumn('');
      setLineageData(null);
    }
  };

  const loadLineage = async (datasetName, columnName) => {
    if (!datasetName || !columnName) return;
    setLoading(true);
    try {
      const res = await getColumnLineageGraph(datasetName, columnName);
      setLineageData(res.data);
    } catch (error) {
      console.error('Failed to load column lineage:', error);
      setLineageData(null);
    }
    setLoading(false);
  };

  const loadImpact = async (datasetName, columnName) => {
    if (!datasetName || !columnName) return;
    setImpactLoading(true);
    try {
      const res = await getColumnImpact(datasetName, columnName);
      setImpactData(res.data);
    } catch (error) {
      console.error('Failed to load impact analysis:', error);
      setImpactData(null);
    }
    setImpactLoading(false);
  };

  const handleShowImpact = () => {
    if (!showImpact && selectedDataset && selectedColumn) {
      loadImpact(selectedDataset, selectedColumn);
    }
    setShowImpact(!showImpact);
  };

  const handleCopyPath = useCallback((path, event) => {
    event.stopPropagation();
    navigator.clipboard.writeText(path).then(() => {
      setCopiedPath(path);
      setTimeout(() => setCopiedPath(null), 2000);
    });
  }, []);

  // Lookup map: dataset name → { location, dataset_type }
  const datasetMap = useMemo(() => {
    const map = new Map();
    datasets.forEach(d => map.set(d.name, { location: d.location, dataset_type: d.dataset_type }));
    return map;
  }, [datasets]);

  const { nodes, edges } = useMemo(() => {
    if (!lineageData) return { nodes: [], edges: [] };

    const {
      dataset_name = '',
      column_name = '',
      upstream = [],
      downstream = [],
      edges: lineageEdges = []
    } = lineageData;

    const nodeMap = new Map();
    const nodeWidth = 240;
    const nodeHeight = 120;

    const getDatasetInfo = (name) => datasetMap.get(name) || {};

    // Helper: build column node label with bucket badge
    const buildColumnNodeLabel = (datasetName, columnName, info, isSelected = false) => {
      const { bucket, displayName } = parseDatasetName(datasetName);
      return (
        <div className={`column-node ${isSelected ? 'selected-node' : ''}`}>
          <div className="node-dataset-row">
            {info.dataset_type && (
              <span className="node-type-badge">{info.dataset_type}</span>
            )}
            {bucket && (
              <span className="node-bucket-badge" title={bucket}>{bucket}</span>
            )}
            <div className="node-dataset-wrapper">
              <div className="node-dataset" data-tooltip={displayName}>
                <span className="node-text-inner">{displayName}</span>
              </div>
              <div
                className="copy-path-btn"
                role="button"
                tabIndex={0}
                onClick={(e) => handleCopyPath(displayName, e)}
                title="Copy name"
              >
                {copiedPath === displayName ? (
                  <Check size={12} />
                ) : (
                  <Copy size={12} />
                )}
              </div>
            </div>
          </div>
          {info.location && (
            <div className="node-location-wrapper">
              <div className="node-location" data-tooltip={info.location}>
                <span className="node-text-inner">{info.location}</span>
              </div>
              <div
                className="copy-path-btn"
                role="button"
                tabIndex={0}
                onClick={(e) => handleCopyPath(info.location, e)}
                title="Copy full path"
              >
                {copiedPath === info.location ? (
                  <Check size={12} />
                ) : (
                  <Copy size={12} />
                )}
              </div>
            </div>
          )}
          <div className="node-column-wrapper">
            <div className="node-column" data-tooltip={columnName}>
              <span className="node-text-inner">{columnName}</span>
            </div>
            <div
              className="copy-path-btn"
              role="button"
              tabIndex={0}
              onClick={(e) => handleCopyPath(columnName, e)}
              title="Copy name"
            >
              {copiedPath === columnName ? (
                <Check size={12} />
              ) : (
                <Copy size={12} />
              )}
            </div>
          </div>
        </div>
      );
    };

    // Selected column node
    const selectedKey = `${dataset_name}.${column_name}`;
    const selectedInfo = getDatasetInfo(dataset_name);
    nodeMap.set(selectedKey, {
      id: selectedKey,
      data: {
        label: buildColumnNodeLabel(dataset_name, column_name, selectedInfo, true),
      },
      position: { x: 0, y: 0 },
      style: {
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        padding: '0',
        borderRadius: '10px',
        border: 'none',
        boxShadow: '0 4px 12px rgba(102, 126, 234, 0.4)',
        width: `${nodeWidth}px`,
      },
    });

    // Upstream nodes
    upstream.forEach((col) => {
      const key = `${col.dataset_name}.${col.column_name}`;
      const transformColor = TRANSFORM_COLORS[col.transform_type] || '#4caf50';
      const colInfo = getDatasetInfo(col.dataset_name);
      nodeMap.set(key, {
        id: key,
        data: {
          label: buildColumnNodeLabel(col.dataset_name, col.column_name, colInfo),
        },
        position: { x: 0, y: 0 },
        style: {
          background: '#ffffff',
          color: '#333',
          padding: '0',
          borderRadius: '8px',
          border: `2px solid ${transformColor}`,
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          width: `${nodeWidth}px`,
        },
      });
    });

    // Downstream nodes
    downstream.forEach((col) => {
      const key = `${col.dataset_name}.${col.column_name}`;
      const transformColor = TRANSFORM_COLORS[col.transform_type] || '#ff9800';
      const colInfo = getDatasetInfo(col.dataset_name);
      nodeMap.set(key, {
        id: key,
        data: {
          label: buildColumnNodeLabel(col.dataset_name, col.column_name, colInfo),
        },
        position: { x: 0, y: 0 },
        style: {
          background: '#ffffff',
          color: '#333',
          padding: '0',
          borderRadius: '8px',
          border: `2px solid ${transformColor}`,
          boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
          width: `${nodeWidth}px`,
        },
      });
    });

    // Create edges with bezier type
    const flowEdges = lineageEdges.map((e) => {
      const sourceKey = `${e.source_dataset_name}.${e.source_column}`;
      const targetKey = `${e.target_dataset_name}.${e.target_column}`;
      const edgeColor = TRANSFORM_COLORS[e.transform_type] || '#667eea';

      const transformLabel = e.transform_type || 'UNKNOWN';
      // Always show label for better UX (hover target)
      const labelText = transformLabel;


      return {
        id: e.id,
        source: sourceKey,
        target: targetKey,
        type: 'tooltipEdge',
        label: labelText,
        animated: true,
        style: {
          stroke: edgeColor,
          strokeWidth: 3,
          opacity: 0.9,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: edgeColor,
          width: 22,
          height: 22,
        },
        data: {
          expression: e.expression,
          transformType: e.transform_type,
        },
      };
    });

    const rawNodes = Array.from(nodeMap.values());

    // Apply dagre layout
    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      rawNodes,
      flowEdges,
      {
        direction: 'LR',
        nodeWidth,
        nodeHeight,
        rankSep: 300,
        nodeSep: 80,
      }
    );

    return { nodes: layoutedNodes, edges: layoutedEdges };
  }, [lineageData, datasetMap, copiedPath, handleCopyPath]);

  const stats = useMemo(() => {
    if (!lineageData) return null;
    return {
      upstream: lineageData.upstream.length,
      downstream: lineageData.downstream.length,
      edges: lineageData.edges.length,
    };
  }, [lineageData]);

  if (initialLoading) {
    return <LoadingSpinner message="Loading datasets..." />;
  }

  if (datasets.length === 0) {
    return (
      <div className="column-lineage">
        <PageHeader
          title="Column Lineage"
          description="Track field-level data flow and transformations"
          icon={Network}
        />
        <div className="empty-state">
          <Network size={48} strokeWidth={1.5} />
          <h3>No datasets found</h3>
          <p>Run a Spark job to capture column lineage.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="column-lineage">
      <PageHeader
        title="Column Lineage"
        description="Track field-level data flow and transformations"
        icon={Network}
      >
        <div className="selectors">
          <div className="selector">
            <label>Dataset</label>
            <div className="select-wrapper">
              <select
                value={selectedDataset}
                onChange={(e) => {
                  setSelectedDataset(e.target.value);
                  setShowImpact(false);
                  loadColumns(e.target.value);
                }}
              >
                {datasets.map((d) => {
                  const { bucket, displayName } = parseDatasetName(d.name);
                  return (
                    <option key={d.id} value={d.name}>
                      {bucket ? `${displayName} (${bucket})` : displayName}
                    </option>
                  );
                })}
              </select>
              <ChevronDown size={18} className="select-icon" />
            </div>
          </div>
          <div className="selector">
            <label>Column</label>
            <div className="select-wrapper">
              <select
                value={selectedColumn}
                onChange={(e) => {
                  setSelectedColumn(e.target.value);
                  setShowImpact(false);
                  loadLineage(selectedDataset, e.target.value);
                }}
                disabled={columns.length === 0}
              >
                {columns.length === 0 ? (
                  <option value="">No columns with lineage</option>
                ) : (
                  columns.map((c) => (
                    <option key={c.column_name} value={c.column_name}>
                      {c.column_name}
                    </option>
                  ))
                )}
              </select>
              <ChevronDown size={18} className="select-icon" />
            </div>
          </div>
        </div>
        <button
          className={`impact-btn ${showImpact ? 'active' : ''}`}
          onClick={handleShowImpact}
          disabled={!selectedColumn}
        >
          <Zap size={16} />
          <span>Impact Analysis</span>
        </button>
      </PageHeader>

      <div className="lineage-info-bar">
        <div className="transform-legend">
          <span className="legend-label">Transform Types:</span>
          {Object.entries(TRANSFORM_COLORS).map(([type, color]) => (
            <div key={type} className="legend-item">
              <span className="legend-dot" style={{ background: color }}></span>
              <span>{type}</span>
            </div>
          ))}
        </div>
        {stats && (
          <div className="lineage-stats">
            <div className="stat-item">
              <Database size={14} />
              <span>{stats.upstream} upstream</span>
            </div>
            <div className="stat-item">
              <Database size={14} />
              <span>{stats.downstream} downstream</span>
            </div>
            <div className="stat-item">
              <Code size={14} />
              <span>{stats.edges} transforms</span>
            </div>
          </div>
        )}
      </div>

      {showImpact && (
        <div className="impact-panel">
          {impactLoading ? (
            <div className="impact-loading">
              <Loader2 className="loading-spinner" size={24} />
              <span>Analyzing impact...</span>
            </div>
          ) : impactData ? (
            <>
              <div className="impact-header">
                <div className="impact-title">
                  <Target size={20} />
                  <h3>Impact Analysis</h3>
                  <span
                    className="criticality-badge"
                    style={{ background: CRITICALITY_COLORS[impactData.criticality] }}
                  >
                    {impactData.criticality}
                  </span>
                </div>
                <div className="impact-summary">
                  <div className="impact-stat">
                    <span className="stat-value">{impactData.total_datasets_affected}</span>
                    <span className="stat-label">Datasets</span>
                  </div>
                  <div className="impact-stat">
                    <span className="stat-value">{impactData.total_columns_affected}</span>
                    <span className="stat-label">Columns</span>
                  </div>
                  <div className="impact-stat">
                    <span className="stat-value">{impactData.impact_summary.direct_dependents}</span>
                    <span className="stat-label">Direct</span>
                  </div>
                  <div className="impact-stat">
                    <span className="stat-value">{impactData.impact_summary.transitive_dependents}</span>
                    <span className="stat-label">Transitive</span>
                  </div>
                </div>
              </div>
              <div className="impacted-datasets">
                {impactData.impacted_datasets.map((ds, idx) => (
                  <div key={idx} className="impacted-dataset">
                    <div className="dataset-header">
                      <Database size={16} />
                      <span className="dataset-name">{ds.dataset_name}</span>
                      <span className="dataset-type">{ds.dataset_type || 'table'}</span>
                      <span className="depth-badge">Depth {ds.max_depth}</span>
                    </div>
                    <div className="affected-columns">
                      {ds.columns.map((col, cidx) => (
                        <div key={cidx} className="affected-column">
                          <span className="col-name">{col.column_name}</span>
                          {col.transform_type && (
                            <span
                              className="transform-badge"
                              style={{ background: TRANSFORM_COLORS[col.transform_type] || '#666' }}
                            >
                              {col.transform_type}
                            </span>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
              {impactData.total_datasets_affected === 0 && (
                <div className="no-impact">
                  <AlertTriangle size={24} />
                  <p>No downstream dependencies found for this column.</p>
                </div>
              )}
            </>
          ) : (
            <div className="impact-error">
              <AlertTriangle size={24} />
              <p>Failed to load impact analysis</p>
            </div>
          )}
        </div>
      )}

      {loading ? (
        <LoadingSpinner size={32} message="Loading column lineage..." />
      ) : nodes.length === 0 ? (
        <div className="empty-graph">
          <Network size={48} strokeWidth={1.5} />
          <h3>No column lineage data</h3>
          <p>This column has no tracked upstream or downstream dependencies.</p>
        </div>
      ) : (
        <div className="flow-container">
          <ReactFlow
            nodes={nodes}
            edges={edges}
            edgeTypes={edgeTypes}
            fitView
            fitViewOptions={{ padding: 0.3 }}
            minZoom={0.3}
            maxZoom={1.5}
          >
            <Background color="#c0c4cc" gap={20} size={2} />
            <Controls showInteractive={false} />
            <MiniMap
              nodeColor={(node) => {
                const style = node.style || {};
                if (style.background?.includes('667eea')) return '#667eea';
                return style.borderColor || '#667eea';
              }}
              maskColor="rgba(240, 242, 245, 0.8)"
              style={{ borderRadius: '8px' }}
            />
          </ReactFlow>
        </div>
      )}
    </div>
  );
}

export default ColumnLineage;
