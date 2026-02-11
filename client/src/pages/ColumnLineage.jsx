import { useState, useEffect, useMemo } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType } from 'reactflow';
import 'reactflow/dist/style.css';
import { getDatasets, getDatasetColumns, getColumnLineageGraph, getColumnImpact } from '../services/api';
import { Network, ChevronDown, Loader2, Database, Code, AlertTriangle, Zap, Target } from 'lucide-react';
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

      // Extract unique columns for this dataset from the edges
      // Columns where this dataset is the target (has upstream lineage)
      const targetColumns = edges
        .filter(e => e.target_dataset_name === datasetName)
        .map(e => e.target_column);

      // Columns where this dataset is the source (has downstream lineage)
      const sourceColumns = edges
        .filter(e => e.source_dataset_name === datasetName)
        .map(e => e.source_column);

      // Combine and deduplicate
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

  // Lookup map: dataset name → { location, dataset_type }
  const datasetMap = useMemo(() => {
    const map = new Map();
    datasets.forEach(d => map.set(d.name, { location: d.location, dataset_type: d.dataset_type }));
    return map;
  }, [datasets]);

  const { nodes, edges } = useMemo(() => {
    if (!lineageData) return { nodes: [], edges: [] };

    const { dataset_name, column_name, upstream, downstream, edges: lineageEdges } = lineageData;

    // Create a unique key for each column (dataset.column)
    const nodeMap = new Map();

    // Position constants
    const nodeWidth = 200;
    const horizontalGap = 280;
    const verticalGap = 100;
    const centerX = 500;
    const centerY = 250;

    // Helper: get dataset info from the lookup map
    const getDatasetInfo = (name) => datasetMap.get(name) || {};

    // Selected column node
    const selectedKey = `${dataset_name}.${column_name}`;
    const selectedInfo = getDatasetInfo(dataset_name);
    nodeMap.set(selectedKey, {
      id: selectedKey,
      data: {
        label: (
          <div className="column-node selected-node">
            <div className="node-dataset-row">
              {selectedInfo.dataset_type && (
                <span className="node-type-badge">{selectedInfo.dataset_type}</span>
              )}
              <span className="node-dataset">{dataset_name}</span>
            </div>
            {selectedInfo.location && (
              <div className="node-location" title={selectedInfo.location}>{selectedInfo.location}</div>
            )}
            <div className="node-column">{column_name}</div>
          </div>
        ),
      },
      position: { x: centerX, y: centerY },
      style: {
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        padding: '0',
        borderRadius: '10px',
        border: 'none',
        boxShadow: '0 4px 12px rgba(102, 126, 234, 0.4)',
        minWidth: `${nodeWidth}px`,
      },
    });

    // Group upstream by depth
    const upstreamByDepth = new Map();
    upstream.forEach((col) => {
      if (!upstreamByDepth.has(col.depth)) {
        upstreamByDepth.set(col.depth, []);
      }
      upstreamByDepth.get(col.depth).push(col);
    });

    // Group downstream by depth
    const downstreamByDepth = new Map();
    downstream.forEach((col) => {
      if (!downstreamByDepth.has(col.depth)) {
        downstreamByDepth.set(col.depth, []);
      }
      downstreamByDepth.get(col.depth).push(col);
    });

    // Add upstream nodes
    upstreamByDepth.forEach((cols, depth) => {
      const xPos = centerX - depth * horizontalGap;
      const totalHeight = (cols.length - 1) * verticalGap;
      const startY = centerY - totalHeight / 2;

      cols.forEach((col, idx) => {
        const key = `${col.dataset_name}.${col.column_name}`;
        const transformColor = TRANSFORM_COLORS[col.transform_type] || '#4caf50';
        const colInfo = getDatasetInfo(col.dataset_name);
        nodeMap.set(key, {
          id: key,
          data: {
            label: (
              <div className="column-node">
                <div className="node-dataset-row">
                  {colInfo.dataset_type && (
                    <span className="node-type-badge">{colInfo.dataset_type}</span>
                  )}
                  <span className="node-dataset">{col.dataset_name}</span>
                </div>
                {colInfo.location && (
                  <div className="node-location" title={colInfo.location}>{colInfo.location}</div>
                )}
                <div className="node-column">{col.column_name}</div>
                {col.transform_type && (
                  <div className="node-transform" style={{ background: transformColor }}>
                    {col.transform_type}
                  </div>
                )}
              </div>
            ),
          },
          position: { x: xPos, y: startY + idx * verticalGap },
          style: {
            background: '#ffffff',
            color: '#333',
            padding: '0',
            borderRadius: '8px',
            border: `2px solid ${transformColor}`,
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
            minWidth: `${nodeWidth - 20}px`,
          },
        });
      });
    });

    // Add downstream nodes
    downstreamByDepth.forEach((cols, depth) => {
      const xPos = centerX + depth * horizontalGap;
      const totalHeight = (cols.length - 1) * verticalGap;
      const startY = centerY - totalHeight / 2;

      cols.forEach((col, idx) => {
        const key = `${col.dataset_name}.${col.column_name}`;
        const transformColor = TRANSFORM_COLORS[col.transform_type] || '#ff9800';
        const colInfo = getDatasetInfo(col.dataset_name);
        nodeMap.set(key, {
          id: key,
          data: {
            label: (
              <div className="column-node">
                <div className="node-dataset-row">
                  {colInfo.dataset_type && (
                    <span className="node-type-badge">{colInfo.dataset_type}</span>
                  )}
                  <span className="node-dataset">{col.dataset_name}</span>
                </div>
                {colInfo.location && (
                  <div className="node-location" title={colInfo.location}>{colInfo.location}</div>
                )}
                <div className="node-column">{col.column_name}</div>
                {col.transform_type && (
                  <div className="node-transform" style={{ background: transformColor }}>
                    {col.transform_type}
                  </div>
                )}
              </div>
            ),
          },
          position: { x: xPos, y: startY + idx * verticalGap },
          style: {
            background: '#ffffff',
            color: '#333',
            padding: '0',
            borderRadius: '8px',
            border: `2px solid ${transformColor}`,
            boxShadow: '0 2px 8px rgba(0,0,0,0.1)',
            minWidth: `${nodeWidth - 20}px`,
          },
        });
      });
    });

    // Create edges with improved visibility and smart labeling
    const flowEdges = lineageEdges.map((e) => {
      const sourceKey = `${e.source_dataset_name}.${e.source_column}`;
      const targetKey = `${e.target_dataset_name}.${e.target_column}`;
      const edgeColor = TRANSFORM_COLORS[e.transform_type] || '#667eea';

      // Smart label logic: Show transform type for all edges
      // For DIRECT: show subtle indicator
      // For others: show transform type name
      const transformLabel = e.transform_type || 'UNKNOWN';
      const isDirect = transformLabel === 'DIRECT';

      // Create display label with icon/symbol
      const labelText = isDirect ? '→' : transformLabel;

      return {
        id: e.id,
        source: sourceKey,
        target: targetKey,
        type: 'smoothstep',  // Better routing around nodes
        animated: true,
        style: {
          stroke: edgeColor,
          strokeWidth: 3,    // Thicker for better visibility
          opacity: 0.9,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: edgeColor,
          width: 22,         // Larger arrowhead
          height: 22,
        },
        // Always show label (different styles for DIRECT vs others)
        label: labelText,
        labelStyle: {
          fontSize: isDirect ? 14 : 10,  // Larger for direct arrow symbol
          fill: isDirect ? edgeColor : '#1a1a2e',
          fontWeight: isDirect ? '400' : '700',
          fontFamily: 'system-ui, -apple-system, sans-serif',
          letterSpacing: isDirect ? '0' : '0.3px',
        },
        labelBgStyle: {
          fill: isDirect ? 'transparent' : '#ffffff',
          fillOpacity: isDirect ? 0 : 0.95,
          rx: 5,
          ry: 5,
          stroke: isDirect ? 'transparent' : edgeColor,
          strokeWidth: isDirect ? 0 : 1.5,
        },
        labelBgPadding: isDirect ? [0, 0] : [6, 10],
        // Store expression in data for tooltip/hover display
        data: {
          expression: e.expression,
          transformType: e.transform_type,
        },
      };
    });

    return { nodes: Array.from(nodeMap.values()), edges: flowEdges };
  }, [lineageData, datasetMap]);

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
                {datasets.map((d) => (
                  <option key={d.id} value={d.name}>
                    {d.name}
                  </option>
                ))}
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
            fitView
            fitViewOptions={{ padding: 0.3 }}
            minZoom={0.3}
            maxZoom={1.5}
          >
            <Background color="#e0e0e0" gap={20} size={1} />
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
