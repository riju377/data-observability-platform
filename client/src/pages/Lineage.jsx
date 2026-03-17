import { useState, useEffect, useCallback, useMemo } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType, EdgeLabelRenderer, getBezierPath } from 'reactflow';
import 'reactflow/dist/style.css';
import { getCachedDatasets, getCachedLineageGraph, invalidateDatasetCache } from '../services/cachedApi';
import { GitBranch, ChevronDown, ArrowRight, Loader2, Database, Workflow, RefreshCw } from 'lucide-react';
import { getLayoutedElements } from '../utils/layoutGraph';
import LineageNode from '../components/lineage/LineageNode';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './Lineage.css';
import '../styles/buttons.css';

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

// Custom edge with hover tooltip for job info
function LineageEdge({ id, sourceX, sourceY, targetX, targetY, sourcePosition, targetPosition, style, markerEnd, data }) {
  const [hovered, setHovered] = useState(false);

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX, sourceY, targetX, targetY,
    sourcePosition, targetPosition,
  });

  const hasJobInfo = data?.job_name &&
    data.job_name !== 'lineage-capture' &&
    data.job_name.toLowerCase() !== 'unknown' &&
    data.job_name !== 'command';

  const truncatedName = hasJobInfo
    ? (data.job_name.length > 16 ? data.job_name.slice(0, 16) + '...' : data.job_name)
    : null;

  return (
    <>
      {/* Hover glow path */}
      {hovered && (
        <path
          d={edgePath}
          fill="none"
          stroke="#667eea"
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
          strokeWidth: hovered ? 3 : style?.strokeWidth || 2,
          transition: 'stroke-width 0.2s ease, opacity 0.2s ease',
        }}
        markerEnd={markerEnd}
      />
      {hasJobInfo && (
        <EdgeLabelRenderer>
          <div
            style={{
              position: 'absolute',
              transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)`,
              pointerEvents: 'all',
              zIndex: 1000,
            }}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={() => setHovered(false)}
          >
            {/* Pill badge */}
            <div className="edge-job-badge">
              <Workflow size={10} />
              <span>{truncatedName}</span>
            </div>
            {/* Full tooltip on hover */}
            {hovered && (
              <div className="edge-job-tooltip">
                <div className="edge-job-tooltip__header">
                  <Workflow size={12} />
                  <span>Spark Job</span>
                </div>
                <div className="edge-job-tooltip__name">{data.job_name}</div>
              </div>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

// CRITICAL: Define nodeTypes and edgeTypes at module level (outside component)
const nodeTypes = { lineageNode: LineageNode };
const edgeTypes = { lineageEdge: LineageEdge };

function Lineage() {
  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState('');
  const [lineageData, setLineageData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [highlightedNodeId, setHighlightedNodeId] = useState(null);

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      const lastSelected = localStorage.getItem('lastSelectedDataset');

      if (lastSelected) {
        const [datasetList, lineageData] = await Promise.all([
          getCachedDatasets(),
          getCachedLineageGraph(lastSelected).catch(() => null)
        ]);

        setDatasets(datasetList);
        if (datasetList.some(d => d.name === lastSelected)) {
          setSelectedDataset(lastSelected);
          setLineageData(lineageData);
        } else if (datasetList.length > 0) {
          setSelectedDataset(datasetList[0].name);
          loadLineage(datasetList[0].name);
        }
      } else {
        const datasetList = await getCachedDatasets();
        setDatasets(datasetList);
        if (datasetList.length > 0) {
          setSelectedDataset(datasetList[0].name);
          loadLineage(datasetList[0].name);
        }
      }
    } catch (error) {
      console.error('Failed to load data:', error);
      setDatasets([]);
    }
    setInitialLoading(false);
  };

  const loadLineage = async (datasetName) => {
    setLoading(true);
    setLineageData(null);
    setHighlightedNodeId(null);
    try {
      const lineageData = await getCachedLineageGraph(datasetName);
      setLineageData(lineageData);
    } catch (error) {
      console.error('Failed to load lineage:', error);
      setLineageData(null);
    }
    setLoading(false);
  };

  const handleRefresh = () => {
    if (selectedDataset) {
      invalidateDatasetCache();
      loadLineage(selectedDataset);
    }
  };

  // Node click: highlight connected edges
  const handleNodeClick = useCallback((event, node) => {
    setHighlightedNodeId(prev => prev === node.id ? null : node.id);
  }, []);

  const handlePaneClick = useCallback(() => {
    setHighlightedNodeId(null);
  }, []);

  // Build graph with dagre layout
  const { nodes, edges: rawEdges } = useMemo(() => {
    if (!lineageData) return { nodes: [], edges: [] };

    const { dataset, upstream, downstream, edges: lineageEdges } = lineageData;
    const nodeWidth = 280;
    const nodeHeight = 100;

    const buildNodeData = (d, role) => {
      const { bucket, displayName } = parseDatasetName(d.name);
      return {
        displayName,
        bucket,
        location: d.location,
        dataset_type: d.dataset_type,
        nodeRole: role,
      };
    };

    const flowNodes = [];

    // Selected dataset node
    flowNodes.push({
      id: dataset.id,
      type: 'lineageNode',
      data: buildNodeData(dataset, 'selected'),
      position: { x: 0, y: 0 },
    });

    // Upstream nodes
    upstream.forEach((d) => {
      flowNodes.push({
        id: d.id,
        type: 'lineageNode',
        data: buildNodeData(d, 'upstream'),
        position: { x: 0, y: 0 },
      });
    });

    // Downstream nodes
    downstream.forEach((d) => {
      flowNodes.push({
        id: d.id,
        type: 'lineageNode',
        data: buildNodeData(d, 'downstream'),
        position: { x: 0, y: 0 },
      });
    });

    // Build set of valid node IDs to filter orphan edges
    const nodeIdSet = new Set(flowNodes.map((n) => n.id));

    // Deduplicate edges
    const edgeMap = new Map();
    lineageEdges.forEach((e) => {
      if (!nodeIdSet.has(e.source_dataset_id) || !nodeIdSet.has(e.target_dataset_id)) return;
      if (e.source_dataset_id === e.target_dataset_id) return;
      const key = `${e.source_dataset_id}→${e.target_dataset_id}`;
      if (!edgeMap.has(key)) {
        edgeMap.set(key, e);
      }
    });

    const flowEdges = Array.from(edgeMap.values()).map((e) => ({
      id: e.id,
      source: e.source_dataset_id,
      target: e.target_dataset_id,
      type: 'lineageEdge',
      animated: true,
      style: {
        stroke: '#667eea',
        strokeWidth: 2,
        opacity: 0.85,
      },
      markerEnd: {
        type: MarkerType.ArrowClosed,
        color: '#667eea',
        width: 20,
        height: 20,
      },
      data: {
        job_name: e.job_name,
      },
    }));

    // Apply dagre layout
    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      flowNodes, flowEdges,
      { direction: 'LR', nodeWidth, nodeHeight, rankSep: 350, nodeSep: 100 }
    );

    return { nodes: layoutedNodes, edges: layoutedEdges };
  }, [lineageData]);

  // Apply edge highlighting when a node is clicked
  const edges = useMemo(() => {
    if (!highlightedNodeId) return rawEdges;
    return rawEdges.map(edge => ({
      ...edge,
      style: {
        ...edge.style,
        opacity: (edge.source === highlightedNodeId || edge.target === highlightedNodeId) ? 1 : 0.12,
        strokeWidth: (edge.source === highlightedNodeId || edge.target === highlightedNodeId) ? 3 : 1,
      },
      animated: (edge.source === highlightedNodeId || edge.target === highlightedNodeId),
    }));
  }, [rawEdges, highlightedNodeId]);

  // Stats for the info panel
  const stats = useMemo(() => {
    if (!lineageData) return null;
    return {
      upstream: lineageData.upstream.length,
      downstream: lineageData.downstream.length,
      edges: lineageData.edges.length,
    };
  }, [lineageData]);

  return (
    <div className="lineage">
      <PageHeader
        title="Data Lineage"
        description="Visualize data flow and dependencies"
        icon={GitBranch}
      >
        {datasets.length > 0 && (
          <div style={{ display: 'flex', gap: '1.5rem', alignItems: 'flex-end' }}>
            <div className="dataset-selector">
              <label>Select Dataset</label>
              <div className="select-wrapper">
                <select
                  value={selectedDataset}
                  onChange={(e) => {
                    const datasetName = e.target.value;
                    setSelectedDataset(datasetName);
                    localStorage.setItem('lastSelectedDataset', datasetName);
                    loadLineage(datasetName);
                  }}
                  disabled={initialLoading}
                >
                  {initialLoading ? (
                    <option value="">Loading datasets...</option>
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
            <button
              className="refresh-btn"
              onClick={handleRefresh}
              disabled={loading || initialLoading}
              title="Refresh lineage data from database"
            >
              {loading ? (
                <Loader2 size={16} className="loading-spinner" />
              ) : (
                <RefreshCw size={16} />
              )}
              <span>Refresh</span>
            </button>
          </div>
        )}
      </PageHeader>

      {datasets.length === 0 && !initialLoading ? (
        <div className="empty-state">
          <GitBranch size={48} strokeWidth={1.5} />
          <h3>No datasets found</h3>
          <p>Run a Spark job to capture data lineage.</p>
        </div>
      ) : (
        <>
          <div className="lineage-info-bar">
            <div className="lineage-legend">
              <div className="legend-item">
                <span className="legend-dot upstream"></span>
                <span>Upstream</span>
              </div>
              <ArrowRight size={16} className="legend-arrow" />
              <div className="legend-item">
                <span className="legend-dot selected"></span>
                <span>Selected</span>
              </div>
              <ArrowRight size={16} className="legend-arrow" />
              <div className="legend-item">
                <span className="legend-dot downstream"></span>
                <span>Downstream</span>
              </div>
            </div>
            {stats && !initialLoading && !loading && (
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
                  <GitBranch size={14} />
                  <span>{stats.edges} edges</span>
                </div>
              </div>
            )}
          </div>

          {(initialLoading || loading) ? (
            <div className="lineage-skeleton">
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
              <h3>No lineage data</h3>
              <p>This dataset has no upstream or downstream dependencies.</p>
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
                <Background color="#c0c4cc" gap={20} size={1} />
                <Controls showInteractive={false} />
                <MiniMap
                  nodeColor={(node) => {
                    const role = node.data?.nodeRole;
                    if (role === 'selected') return '#667eea';
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

export default Lineage;
