import { useState, useEffect, useMemo, useCallback } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType, getBezierPath, EdgeLabelRenderer } from 'reactflow';
import 'reactflow/dist/style.css';
import { getCachedDatasets, getCachedDatasetColumns, getCachedColumnLineage, getCachedColumnImpact, invalidateDatasetCache } from '../services/cachedApi';
import { apiCache } from '../utils/cache';
import { Network, ChevronDown, Loader2, Database, Code, AlertTriangle, Zap, Target, ArrowRight, Copy, Check, Info, RefreshCw } from 'lucide-react';
import { getLayoutedElements } from '../utils/layoutGraph';
import ColumnLineageNode from '../components/lineage/ColumnLineageNode';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './ColumnLineage.css';

const TRANSFORM_COLORS = {
  DIRECT: '#22c55e',
  EXPRESSION: '#3b82f6',
  AGGREGATE: '#8b5cf6',
  JOIN: '#f59e0b',
  FILTER: '#ef4444',
  CASE: '#06b6d4',
};

const CRITICALITY_COLORS = {
  LOW: '#22c55e',
  MEDIUM: '#f59e0b',
  HIGH: '#ef4444',
  CRITICAL: '#8b5cf6',
};

// Custom edge with hover tooltip for expression
function TooltipEdge({
  id, sourceX, sourceY, targetX, targetY,
  sourcePosition, targetPosition, style, markerEnd, data,
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
      {/* Hover glow path */}
      {hovered && (
        <path
          d={edgePath}
          fill="none"
          stroke={edgeColor}
          strokeWidth={14}
          strokeOpacity={0.1}
          style={{ pointerEvents: 'none' }}
        />
      )}
      {/* Main edge path */}
      <path
        id={id}
        d={edgePath}
        fill="none"
        style={{
          ...style,
          strokeWidth: hovered ? 4 : style?.strokeWidth || 3,
          transition: 'stroke-width 0.2s ease, opacity 0.2s ease',
        }}
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

// CRITICAL: Define at module level (outside component)
const nodeTypes = { columnLineageNode: ColumnLineageNode };
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
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [showImpact, setShowImpact] = useState(false);
  const [impactData, setImpactData] = useState(null);
  const [impactLoading, setImpactLoading] = useState(false);
  const [highlightedNodeId, setHighlightedNodeId] = useState(null);

  // Clear old cached data format (one-time migration)
  useEffect(() => {
    const CACHE_VERSION = 'v2_columns_array';
    const lastVersion = localStorage.getItem('columnLineageCacheVersion');
    if (lastVersion !== CACHE_VERSION) {
      apiCache.invalidate('datasetColumns');
      localStorage.setItem('columnLineageCacheVersion', CACHE_VERSION);
    }
  }, []);

  useEffect(() => {
    loadDatasets();
  }, []);

  const loadDatasets = async () => {
    try {
      const datasetList = await getCachedDatasets();
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
    setColumnsLoading(true);
    setHighlightedNodeId(null);
    try {
      const columnNames = await getCachedDatasetColumns(datasetName);
      const columnList = columnNames.map(col => ({ column_name: col }));

      setColumns(columnList);
      if (columnList.length > 0) {
        const firstColumn = columnList[0].column_name;
        setSelectedColumn(firstColumn);
        await loadLineage(datasetName, firstColumn);
        setColumnsLoading(false);
      } else {
        setSelectedColumn('');
        setLineageData(null);
        setColumnsLoading(false);
      }
    } catch (error) {
      console.error('Failed to load columns:', error);
      setColumns([]);
      setSelectedColumn('');
      setLineageData(null);
      setColumnsLoading(false);
    }
  };

  const loadLineage = async (datasetName, columnName) => {
    if (!datasetName || !columnName) return;
    setLoading(true);
    setLineageData(null);
    setHighlightedNodeId(null);
    try {
      const lineageData = await getCachedColumnLineage(datasetName, columnName);
      setLineageData(lineageData);
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
      const impactData = await getCachedColumnImpact(datasetName, columnName);
      setImpactData(impactData);
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

  const handleRefresh = () => {
    invalidateDatasetCache();
    if (selectedDataset && selectedColumn) {
      loadLineage(selectedDataset, selectedColumn);
      if (showImpact) {
        loadImpact(selectedDataset, selectedColumn);
      }
    }
    if (selectedDataset) {
      loadColumns(selectedDataset);
    }
    loadDatasets();
  };

  // Node click: highlight connected edges
  const handleNodeClick = useCallback((event, node) => {
    setHighlightedNodeId(prev => prev === node.id ? null : node.id);
  }, []);

  const handlePaneClick = useCallback(() => {
    setHighlightedNodeId(null);
  }, []);

  // Lookup map: dataset name → { location, dataset_type }
  const datasetMap = useMemo(() => {
    const map = new Map();
    datasets.forEach(d => map.set(d.name, { location: d.location, dataset_type: d.dataset_type }));
    return map;
  }, [datasets]);

  const { nodes, edges: rawEdges } = useMemo(() => {
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

    // Build node data for custom node component
    const buildNodeData = (datasetName, columnName, info, role, transformColor) => {
      const { bucket, displayName } = parseDatasetName(datasetName);
      return {
        displayName,
        bucket,
        location: info.location,
        dataset_type: info.dataset_type,
        columnName,
        nodeRole: role,
        transformColor,
      };
    };

    // Selected column node
    const selectedKey = `${dataset_name}.${column_name}`;
    const selectedInfo = getDatasetInfo(dataset_name);
    nodeMap.set(selectedKey, {
      id: selectedKey,
      type: 'columnLineageNode',
      data: buildNodeData(dataset_name, column_name, selectedInfo, 'selected', null),
      position: { x: 0, y: 0 },
    });

    // Upstream nodes
    upstream.forEach((col) => {
      const key = `${col.dataset_name}.${col.column_name}`;
      const transformColor = TRANSFORM_COLORS[col.transform_type] || '#22c55e';
      const colInfo = getDatasetInfo(col.dataset_name);
      nodeMap.set(key, {
        id: key,
        type: 'columnLineageNode',
        data: buildNodeData(col.dataset_name, col.column_name, colInfo, 'upstream', transformColor),
        position: { x: 0, y: 0 },
      });
    });

    // Downstream nodes
    downstream.forEach((col) => {
      const key = `${col.dataset_name}.${col.column_name}`;
      const transformColor = TRANSFORM_COLORS[col.transform_type] || '#f59e0b';
      const colInfo = getDatasetInfo(col.dataset_name);
      nodeMap.set(key, {
        id: key,
        type: 'columnLineageNode',
        data: buildNodeData(col.dataset_name, col.column_name, colInfo, 'downstream', transformColor),
        position: { x: 0, y: 0 },
      });
    });

    // Create edges
    const flowEdges = lineageEdges.map((e) => {
      const sourceKey = `${e.source_dataset_name}.${e.source_column}`;
      const targetKey = `${e.target_dataset_name}.${e.target_column}`;
      const edgeColor = TRANSFORM_COLORS[e.transform_type] || '#667eea';
      const edgeId = e.id || `${e.source_dataset_id}-${e.source_column}-${e.target_dataset_id}-${e.target_column}`;

      return {
        id: edgeId,
        source: sourceKey,
        target: targetKey,
        type: 'tooltipEdge',
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
      rawNodes, flowEdges,
      { direction: 'LR', nodeWidth, nodeHeight, rankSep: 300, nodeSep: 80 }
    );

    return { nodes: layoutedNodes, edges: layoutedEdges };
  }, [lineageData, datasetMap]);

  // Apply edge highlighting when a node is clicked
  const edges = useMemo(() => {
    if (!highlightedNodeId) return rawEdges;
    return rawEdges.map(edge => ({
      ...edge,
      style: {
        ...edge.style,
        opacity: (edge.source === highlightedNodeId || edge.target === highlightedNodeId) ? 1 : 0.12,
        strokeWidth: (edge.source === highlightedNodeId || edge.target === highlightedNodeId) ? 4 : 1,
      },
      animated: (edge.source === highlightedNodeId || edge.target === highlightedNodeId),
    }));
  }, [rawEdges, highlightedNodeId]);

  const stats = useMemo(() => {
    if (!lineageData) return null;
    return {
      upstream: lineageData.upstream.length,
      downstream: lineageData.downstream.length,
      edges: lineageData.edges.length,
    };
  }, [lineageData]);

  return (
    <div className="column-lineage">
      <PageHeader
        title="Column Lineage"
        description="Track field-level data flow and transformations"
        icon={Network}
      />

      <div className="column-controls-bar">
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
                disabled={initialLoading}
              >
                {initialLoading ? (
                  <option value="">Loading datasets...</option>
                ) : datasets.length === 0 ? (
                  <option value="">No datasets</option>
                ) : (
                  datasets.map((d) => {
                    const { bucket, displayName } = parseDatasetName(d.name);
                    return (
                      <option key={d.id} value={d.name}>
                        {bucket ? `${displayName} (${bucket})` : displayName}
                      </option>
                    );
                  })
                )}
              </select>
              {initialLoading ? (
                <Loader2 size={18} className="select-icon loading-spinner" />
              ) : (
                <ChevronDown size={18} className="select-icon" />
              )}
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
                disabled={columnsLoading || columns.length === 0 || initialLoading}
              >
                {initialLoading || columnsLoading ? (
                  <option value="">Loading columns...</option>
                ) : columns.length === 0 ? (
                  <option value="">No columns with lineage</option>
                ) : (
                  columns.map((c) => (
                    <option key={c.column_name} value={c.column_name}>
                      {c.column_name}
                    </option>
                  ))
                )}
              </select>
              {columnsLoading || initialLoading ? (
                <Loader2 size={18} className="select-icon loading-spinner" />
              ) : (
                <ChevronDown size={18} className="select-icon" />
              )}
            </div>
          </div>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem' }}>
          <button
            className="refresh-btn"
            onClick={handleRefresh}
            disabled={loading || columnsLoading || initialLoading}
            title="Refresh data from database"
          >
            {loading || columnsLoading ? (
              <Loader2 size={16} className="loading-spinner" />
            ) : (
              <RefreshCw size={16} />
            )}
            <span>Refresh</span>
          </button>
          <button
            className={`impact-btn ${showImpact ? 'active' : ''}`}
            onClick={handleShowImpact}
            disabled={!selectedColumn || initialLoading}
          >
            <Zap size={16} />
            <span>Impact Analysis</span>
          </button>
        </div>
      </div>

      {datasets.length === 0 && !initialLoading ? (
        <div className="empty-state">
          <Network size={48} strokeWidth={1.5} />
          <h3>No datasets found</h3>
          <p>Run a Spark job to capture column lineage.</p>
        </div>
      ) : (
        <>
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
            {stats && !initialLoading && !loading && !columnsLoading && (
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

          {(initialLoading || loading || columnsLoading) ? (
            <div className="col-lineage-skeleton">
              <div className="skeleton-node" />
              <div className="skeleton-edge" />
              <div className="skeleton-node skeleton-node--active" />
              <div className="skeleton-edge" />
              <div className="skeleton-node" />
            </div>
          ) : nodes.length === 0 ? (
            <div className="empty-graph-enhanced">
              <div className="empty-graph-illustration">
                <div className="empty-node-placeholder" />
                <ArrowRight size={20} className="empty-arrow" />
                <div className="empty-node-placeholder empty-node-placeholder--active" />
                <ArrowRight size={20} className="empty-arrow" />
                <div className="empty-node-placeholder" />
              </div>
              <h3>No column lineage data</h3>
              <p>This column has no tracked upstream or downstream dependencies.</p>
            </div>
          ) : (
            <div className="flow-container">
              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                edgeTypes={edgeTypes}
                onNodeClick={handleNodeClick}
                onPaneClick={handlePaneClick}
                fitView
                fitViewOptions={{ padding: 0.3, duration: 800 }}
                minZoom={0.3}
                maxZoom={1.5}
              >
                <Background color="#c0c4cc" gap={20} size={2} />
                <Controls showInteractive={false} />
                <MiniMap
                  nodeColor={(node) => {
                    const role = node.data?.nodeRole;
                    if (role === 'selected') return '#667eea';
                    const tc = node.data?.transformColor;
                    if (tc) return tc;
                    if (role === 'upstream') return '#22c55e';
                    if (role === 'downstream') return '#f59e0b';
                    return '#667eea';
                  }}
                  maskColor="rgba(240, 242, 245, 0.8)"
                  style={{ borderRadius: '8px' }}
                />
              </ReactFlow>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default ColumnLineage;
