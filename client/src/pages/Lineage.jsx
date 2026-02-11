import { useState, useEffect, useCallback, useMemo } from 'react';
import ReactFlow, { Background, Controls, MiniMap, MarkerType } from 'reactflow';
import 'reactflow/dist/style.css';
import { getDatasets, getLineageGraph } from '../services/api';
import { GitBranch, ChevronDown, ArrowRight, Loader2, Database } from 'lucide-react';
import LoadingSpinner from '../components/LoadingSpinner';
import PageHeader from '../components/PageHeader';
import './Lineage.css';

function Lineage() {
  const [datasets, setDatasets] = useState([]);
  const [selectedDataset, setSelectedDataset] = useState('');
  const [lineageData, setLineageData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [initialLoading, setInitialLoading] = useState(true);

  useEffect(() => {
    loadDatasets();
  }, []);

  const loadDatasets = async () => {
    try {
      const res = await getDatasets();
      const datasetList = res.data || [];
      setDatasets(datasetList);
      if (datasetList.length > 0) {
        setSelectedDataset(datasetList[0].name);
        loadLineage(datasetList[0].name);
      }
    } catch (error) {
      console.error('Failed to load datasets:', error);
      setDatasets([]);
    }
    setInitialLoading(false);
  };

  const loadLineage = async (datasetName) => {
    setLoading(true);
    try {
      const res = await getLineageGraph(datasetName);
      setLineageData(res.data);
    } catch (error) {
      console.error('Failed to load lineage:', error);
      setLineageData(null);
    }
    setLoading(false);
  };

  // Compute node positions based on graph structure
  const { nodes, edges } = useMemo(() => {
    if (!lineageData) return { nodes: [], edges: [] };

    const { dataset, upstream, downstream, edges: lineageEdges } = lineageData;

    // Build adjacency map for computing levels
    const edgeMap = new Map();
    lineageEdges.forEach(e => {
      if (!edgeMap.has(e.source_dataset_id)) {
        edgeMap.set(e.source_dataset_id, []);
      }
      edgeMap.get(e.source_dataset_id).push(e.target_dataset_id);
    });

    // Build reverse adjacency for upstream traversal
    const reverseEdgeMap = new Map();
    lineageEdges.forEach(e => {
      if (!reverseEdgeMap.has(e.target_dataset_id)) {
        reverseEdgeMap.set(e.target_dataset_id, []);
      }
      reverseEdgeMap.get(e.target_dataset_id).push(e.source_dataset_id);
    });

    // Compute depth levels for upstream nodes (how many hops from selected)
    const upstreamLevels = new Map();
    const computeUpstreamLevel = (nodeId, level, visited = new Set()) => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);

      const currentLevel = upstreamLevels.get(nodeId);
      if (currentLevel === undefined || level < currentLevel) {
        upstreamLevels.set(nodeId, level);
      }

      const parents = reverseEdgeMap.get(nodeId) || [];
      parents.forEach(parentId => {
        computeUpstreamLevel(parentId, level + 1, visited);
      });
    };
    computeUpstreamLevel(dataset.id, 0);

    // Compute depth levels for downstream nodes
    const downstreamLevels = new Map();
    const computeDownstreamLevel = (nodeId, level, visited = new Set()) => {
      if (visited.has(nodeId)) return;
      visited.add(nodeId);

      const currentLevel = downstreamLevels.get(nodeId);
      if (currentLevel === undefined || level < currentLevel) {
        downstreamLevels.set(nodeId, level);
      }

      const children = edgeMap.get(nodeId) || [];
      children.forEach(childId => {
        computeDownstreamLevel(childId, level + 1, visited);
      });
    };
    computeDownstreamLevel(dataset.id, 0);

    // Group upstream nodes by level
    const upstreamByLevel = new Map();
    upstream.forEach(d => {
      const level = upstreamLevels.get(d.id) || 1;
      if (!upstreamByLevel.has(level)) {
        upstreamByLevel.set(level, []);
      }
      upstreamByLevel.get(level).push(d);
    });

    // Group downstream nodes by level
    const downstreamByLevel = new Map();
    downstream.forEach(d => {
      const level = downstreamLevels.get(d.id) || 1;
      if (!downstreamByLevel.has(level)) {
        downstreamByLevel.set(level, []);
      }
      downstreamByLevel.get(level).push(d);
    });

    // Calculate max upstream level
    const maxUpstreamLevel = Math.max(0, ...Array.from(upstreamByLevel.keys()));

    // Position constants - Improved spacing for better visibility
    const nodeWidth = 200;
    const nodeHeight = 60;
    const horizontalGap = 300;
    const verticalGap = 120;
    const centerX = 500;
    const centerY = 300;

    // Helper: build JSX label showing name + type badge + location path
    const buildNodeLabel = (d) => (
      <div className="lineage-node">
        <div className="node-header">
          {d.dataset_type && (
            <span className="node-type-badge">{d.dataset_type}</span>
          )}
          <span className="node-name">{d.name}</span>
        </div>
        {d.location && (
          <div className="node-location" title={d.location}>{d.location}</div>
        )}
      </div>
    );

    // Create nodes array
    const flowNodes = [];

    // Add selected dataset node at center
    flowNodes.push({
      id: dataset.id,
      data: { label: buildNodeLabel(dataset) },
      position: { x: centerX, y: centerY },
      style: {
        background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
        color: 'white',
        fontWeight: '700',
        padding: '0',
        borderRadius: '12px',
        border: '3px solid #fff',
        boxShadow: '0 8px 20px rgba(102, 126, 234, 0.5), 0 0 0 4px rgba(102, 126, 234, 0.1)',
        fontSize: '0.9375rem',
        minWidth: '10rem',
        width: 'fit-content',
        maxWidth: '25rem',
        textAlign: 'center',
      },
    });

    // Add upstream nodes (positioned to the left, level 1 closest to center)
    upstreamByLevel.forEach((nodesAtLevel, level) => {
      const xPos = centerX - (level * horizontalGap);
      const totalHeight = (nodesAtLevel.length - 1) * verticalGap;
      const startY = centerY - totalHeight / 2;

      nodesAtLevel.forEach((d, idx) => {
        flowNodes.push({
          id: d.id,
          data: { label: buildNodeLabel(d) },
          position: { x: xPos, y: startY + idx * verticalGap },
          style: {
            background: level === maxUpstreamLevel ? '#2e7d32' : '#4caf50',
            color: 'white',
            fontWeight: '600',
            padding: '0',
            borderRadius: '10px',
            border: '2px solid #fff',
            boxShadow: '0 4px 12px rgba(76, 175, 80, 0.4), 0 0 0 3px rgba(76, 175, 80, 0.1)',
            fontSize: '0.875rem',
            minWidth: '10rem',
            width: 'fit-content',
            maxWidth: '25rem',
            textAlign: 'center',
          },
        });
      });
    });

    // Add downstream nodes (positioned to the right, level 1 closest to center)
    downstreamByLevel.forEach((nodesAtLevel, level) => {
      const xPos = centerX + (level * horizontalGap);
      const totalHeight = (nodesAtLevel.length - 1) * verticalGap;
      const startY = centerY - totalHeight / 2;

      nodesAtLevel.forEach((d, idx) => {
        flowNodes.push({
          id: d.id,
          data: { label: buildNodeLabel(d) },
          position: { x: xPos, y: startY + idx * verticalGap },
          style: {
            background: '#ff9800',
            color: 'white',
            fontWeight: '600',
            padding: '0',
            borderRadius: '10px',
            border: '2px solid #fff',
            boxShadow: '0 4px 12px rgba(255, 152, 0, 0.4), 0 0 0 3px rgba(255, 152, 0, 0.1)',
            fontSize: '0.875rem',
            minWidth: '10rem',
            width: 'fit-content',
            maxWidth: '25rem',
            textAlign: 'center',
          },
        });
      });
    });

    // Create edges with improved visibility and smart labeling
    const flowEdges = lineageEdges.map((e) => {
      // Show label with smart formatting
      // For "lineage-capture": show subtle "→" instead
      // For meaningful names: show the full name
      const isDefaultName = !e.job_name ||
        e.job_name === 'lineage-capture' ||
        e.job_name.toLowerCase() === 'unknown';

      const labelText = isDefaultName ? '→' : e.job_name;

      return {
        id: e.id,
        source: e.source_dataset_id,
        target: e.target_dataset_id,
        type: 'smoothstep',
        animated: true,
        style: {
          stroke: '#667eea',
          strokeWidth: 2,  // Reduced from 4 for elegance
          opacity: 0.8,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: '#667eea',
          width: 20,       // Reduced from 28
          height: 20,
        },
        label: labelText,
        labelStyle: {
          fontSize: isDefaultName ? 12 : 10,
          fill: isDefaultName ? '#667eea' : '#4a4a6a',
          fontWeight: isDefaultName ? '400' : '500',
          fontFamily: 'system-ui, -apple-system, sans-serif'
        },
        labelBgStyle: {
          fill: isDefaultName ? 'transparent' : 'rgba(255, 255, 255, 0.9)',
          fillOpacity: isDefaultName ? 0 : 1,
          rx: 4,
          ry: 4,
          stroke: isDefaultName ? 'transparent' : '#e2e8f0',
          strokeWidth: isDefaultName ? 0 : 1,
        },
        labelBgPadding: isDefaultName ? [0, 0] : [4, 6],
        labelBgBorderRadius: 4,
      };
    });

    return { nodes: flowNodes, edges: flowEdges };
  }, [lineageData]);

  // Stats for the info panel
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
      <div className="lineage">
        <div className="page-header">
          <div className="header-content">
            <h2>Data Lineage</h2>
            <p>Visualize data flow and dependencies</p>
          </div>
        </div>
        <div className="empty-state">
          <GitBranch size={48} strokeWidth={1.5} />
          <h3>No datasets found</h3>
          <p>Run a Spark job to capture data lineage.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="lineage">
      <PageHeader
        title="Data Lineage"
        description="Visualize data flow and dependencies"
        icon={GitBranch}
      >
        <div className="dataset-selector">
          <label>Select Dataset</label>
          <div className="select-wrapper">
            <select
              value={selectedDataset}
              onChange={(e) => {
                setSelectedDataset(e.target.value);
                loadLineage(e.target.value);
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
      </PageHeader>

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
            fitView
            fitViewOptions={{ padding: 0.3 }}
            minZoom={0.3}
            maxZoom={1.5}
          >
            <Background color="#e0e0e0" gap={20} size={1} />
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
    </div>
  );
}

export default Lineage;
