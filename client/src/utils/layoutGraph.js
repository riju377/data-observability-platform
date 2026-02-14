import dagre from 'dagre';

/**
 * Compute node positions using dagre's directed graph layout algorithm.
 * Produces clean left-to-right layouts that minimize edge crossings.
 *
 * @param {Array} nodes - ReactFlow nodes (must have id)
 * @param {Array} edges - ReactFlow edges (must have source and target)
 * @param {Object} options - Layout options
 * @returns {{ nodes: Array, edges: Array }}
 */
export function getLayoutedElements(nodes, edges, options = {}) {
  const {
    direction = 'LR',
    nodeWidth = 280,
    nodeHeight = 100,
    rankSep = 300,
    nodeSep = 80,
  } = options;

  const dagreGraph = new dagre.graphlib.Graph();
  dagreGraph.setDefaultEdgeLabel(() => ({}));
  dagreGraph.setGraph({
    rankdir: direction,
    ranksep: rankSep,
    nodesep: nodeSep,
    edgesep: 50,
    marginx: 40,
    marginy: 40,
    ranker: 'network-simplex', // Best for clean layer assignment
    align: 'UL',              // Align nodes to upper-left within rank
  });

  nodes.forEach((node) => {
    dagreGraph.setNode(node.id, {
      width: nodeWidth,
      height: nodeHeight,
    });
  });

  edges.forEach((edge) => {
    dagreGraph.setEdge(edge.source, edge.target);
  });

  dagre.layout(dagreGraph);

  const layoutedNodes = nodes.map((node) => {
    const dagreNode = dagreGraph.node(node.id);
    return {
      ...node,
      // For LR layout, connect edges on left (target) and right (source) handles
      sourcePosition: direction === 'LR' ? 'right' : 'bottom',
      targetPosition: direction === 'LR' ? 'left' : 'top',
      position: {
        x: dagreNode.x - nodeWidth / 2,
        y: dagreNode.y - nodeHeight / 2,
      },
    };
  });

  return { nodes: layoutedNodes, edges };
}
