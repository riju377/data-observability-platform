import { useState, useEffect, useCallback, useMemo } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType, BaseEdge, EdgeLabelRenderer, getBezierPath } from 'reactflow';
import 'reactflow/dist/style.css';
import { getCachedDatasets, getCachedLineageGraph } from '../services/cachedApi';
import { GitBranch, ChevronDown, ArrowRight, Loader2, Database, Copy, Check, Workflow } from 'lucide-react';
import { getLayoutedElements } from '../utils/layoutGraph';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './Lineage.css';

// Parse composite dataset name "bucket:dataset_name" into parts
function parseDatasetName(name) {
  if (!name) return { bucket: null, displayName: name || '' };
  const colonIdx = name.indexOf(':');
  // Only split on colon if it doesn't look like a URI scheme (e.g., s3://)
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
    sourceX,
    sourceY,
    targetX,
    targetY,
    sourcePosition,
    targetPosition,
  });

  const hasJobInfo = data?.job_name &&
    data.job_name !== 'lineage-capture' &&
    data.job_name.toLowerCase() !== 'unknown' &&
    data.job_name !== 'command';

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={style}
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
            {/* Small indicator on edge */}
            <div
              style={{
                width: '8px',
                height: '8px',
                borderRadius: '50%',
                background: hovered ? '#667eea' : 'rgba(102, 126, 234, 0.5)',
                border: '1.5px solid white',
                cursor: 'help',
                transition: 'all 0.2s ease',
                boxShadow: hovered ? '0 2px 8px rgba(102, 126, 234, 0.4)' : 'none',
              }}
            />
            {/* Tooltip on hover */}
            {hovered && (
              <div
                style={{
                  position: 'absolute',
                  bottom: '100%',
                  left: '50%',
                  transform: 'translateX(-50%)',
                  marginBottom: '12px',
                  background: 'white',
                  border: '1px solid #e2e8f0',
                  borderRadius: '6px',
                  padding: '8px 12px',
                  boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                  whiteSpace: 'nowrap',
                  fontSize: '12px',
                  color: '#4a5568',
                  zIndex: 10000,
                }}
              >
                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <Workflow size={12} />
                  <span style={{ fontWeight: '500' }}>Job:</span>
                  <span>{data.job_name}</span>
                </div>
              </div>
            )}
          </div>
        </EdgeLabelRenderer>
      )}
    </>
  );
}

const edgeTypes = { lineageEdge: LineageEdge };

function Lineage() {
  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState('');
  const [lineageData, setLineageData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);
  const [copiedPath, setCopiedPath] = useState(null);

  useEffect(() => {
    loadInitialData();
  }, []);

  const loadInitialData = async () => {
    try {
      const lastSelected = localStorage.getItem('lastSelectedDataset');

      if (lastSelected) {
        // Parallel loading with last selected dataset
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
        // Fallback to sequential
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
    try {
      const lineageData = await getCachedLineageGraph(datasetName);
      setLineageData(lineageData);
    } catch (error) {
      console.error('Failed to load lineage:', error);
      setLineageData(null);
    }
    setLoading(false);
  };

  // Copy to clipboard handler
  const handleCopyPath = useCallback((path, event) => {
    event.stopPropagation();
    navigator.clipboard.writeText(path).then(() => {
      setCopiedPath(path);
      setTimeout(() => setCopiedPath(null), 2000);
    });
  }, []);

  // Build graph with dagre layout
  const { nodes, edges } = useMemo(() => {
    if (!lineageData) return { nodes: [], edges: [] };

    const { dataset, upstream, downstream, edges: lineageEdges } = lineageData;

    const nodeWidth = 280;
    const nodeHeight = 100;

    // Helper: build JSX label showing name + type badge + bucket badge + location path
    const buildNodeLabel = (d) => {
      const { bucket, displayName } = parseDatasetName(d.name);
      return (
        <div className="lineage-node">
          <div className="node-header">
            {d.dataset_type && (
              <span className="node-type-badge">{d.dataset_type}</span>
            )}
            {bucket && (
              <span className="node-bucket-badge" title={bucket}>{bucket}</span>
            )}
            <div className="node-name-wrapper">
              <div className="node-name" data-tooltip={displayName}>
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
          {d.location && (
            <div className="node-location-wrapper">
              <div className="node-location" data-tooltip={d.location}>
                <span className="node-text-inner">{d.location}</span>
              </div>
              <div
                className="copy-path-btn"
                role="button"
                tabIndex={0}
                onClick={(e) => handleCopyPath(d.location, e)}
                title="Copy full path"
              >
                {copiedPath === d.location ? (
                  <Check size={12} />
                ) : (
                  <Copy size={12} />
                )}
              </div>
            </div>
          )}
        </div>
      );
    };

    // Build nodes (positions will be set by dagre)
    const flowNodes = [];

    // Selected dataset node
    flowNodes.push({
      id: dataset.id,
      data: { label: buildNodeLabel(dataset) },
      position: { x: 0, y: 0 },
      style: {
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        fontWeight: '700',
        padding: '0',
        borderRadius: '12px',
        border: '3px solid #fff',
        boxShadow: '0 8px 20px rgba(102, 126, 234, 0.5), 0 0 0 4px rgba(102, 126, 234, 0.1)',
        fontSize: '0.9375rem',
        width: `${nodeWidth}px`,
        textAlign: 'center',
      },
    });

    // Upstream nodes
    upstream.forEach((d) => {
      flowNodes.push({
        id: d.id,
        data: { label: buildNodeLabel(d) },
        position: { x: 0, y: 0 },
        style: {
          background: '#4caf50',
          color: 'white',
          fontWeight: '600',
          padding: '0',
          borderRadius: '10px',
          border: '2px solid #fff',
          boxShadow: '0 4px 12px rgba(76, 175, 80, 0.4)',
          fontSize: '0.875rem',
          width: `${nodeWidth}px`,
          textAlign: 'center',
        },
      });
    });

    // Downstream nodes
    downstream.forEach((d) => {
      flowNodes.push({
        id: d.id,
        data: { label: buildNodeLabel(d) },
        position: { x: 0, y: 0 },
        style: {
          background: '#ff9800',
          color: 'white',
          fontWeight: '600',
          padding: '0',
          borderRadius: '10px',
          border: '2px solid #fff',
          boxShadow: '0 4px 12px rgba(255, 152, 0, 0.4)',
          fontSize: '0.875rem',
          width: `${nodeWidth}px`,
          textAlign: 'center',
        },
      });
    });

    // Build set of valid node IDs to filter orphan edges
    const nodeIdSet = new Set(flowNodes.map((n) => n.id));

    // Deduplicate edges: one per source→target pair, drop edges to/from missing nodes
    const edgeMap = new Map();
    lineageEdges.forEach((e) => {
      // Skip edges that reference nodes not in the graph (causes dangling arrows)
      if (!nodeIdSet.has(e.source_dataset_id) || !nodeIdSet.has(e.target_dataset_id)) return;
      // Skip self-loops
      if (e.source_dataset_id === e.target_dataset_id) return;
      const key = `${e.source_dataset_id}→${e.target_dataset_id}`;
      if (!edgeMap.has(key)) {
        edgeMap.set(key, e);
      }
    });

    const flowEdges = Array.from(edgeMap.values()).map((e) => {
      return {
        id: e.id,
        source: e.source_dataset_id,
        target: e.target_dataset_id,
        type: 'lineageEdge',  // Use custom edge with hover tooltip
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
      };
    });

    // Apply dagre layout for automatic positioning
    const { nodes: layoutedNodes, edges: layoutedEdges } = getLayoutedElements(
      flowNodes,
      flowEdges,
      {
        direction: 'LR',
        nodeWidth,
        nodeHeight,
        rankSep: 350,
        nodeSep: 100,
      }
    );

    return { nodes: layoutedNodes, edges: layoutedEdges };
  }, [lineageData, copiedPath, handleCopyPath]);

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
        {!initialLoading && datasets.length > 0 && (
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
        )}
      </PageHeader>

      {initialLoading ? (
        <LoadingSpinner message="Loading datasets..." />
      ) : datasets.length === 0 ? (
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
              <GitBranch size={14} />
              <span>{stats.edges} edges</span>
            </div>
          </div>
        )}
      </div>

      {loading ? (
        <LoadingSpinner size={32} message="Loading lineage graph..." />
      ) : nodes.length === 0 ? (
        <div className="empty-graph">
          <GitBranch size={48} strokeWidth={1.5} />
          <h3>No lineage data</h3>
          <p>This dataset has no upstream or downstream dependencies.</p>
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
            <Background color="#c0c4cc" gap={20} size={1} />
            <Controls showInteractive={false} />
            <MiniMap
              nodeColor={(node) => {
                const bg = node.style?.background || '';
                if (bg.includes('667eea') || bg.includes('764ba2')) return '#667eea';
                if (bg.includes('4caf50') || bg.includes('2e7d32')) return '#4caf50';
                if (bg.includes('ff9800')) return '#ff9800';
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
