import { type JSX, useCallback, useMemo, useEffect } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  BackgroundVariant,
  type Node,
  type Edge,
  getIncomers,
} from '@xyflow/react';
import dagre from '@dagrejs/dagre';
import { ExternalNode, TransformationNode, ScheduledNode } from './DagNode';
import '@xyflow/react/dist/style.css';

const dagNodeTypes = {
  external: ExternalNode,
  transformation: TransformationNode,
  scheduled: ScheduledNode,
};

export interface DagData {
  externalModels: Array<{ id: string }>;
  incrementalModels: Array<{ id: string; depends_on?: string[] }>;
  scheduledModels: Array<{ id: string; depends_on?: string[] }>;
}

export interface DagGraphProps {
  data: DagData;
  className?: string;
}

export function DagGraph({ data, className = '' }: DagGraphProps): JSX.Element {
  // Build nodes and edges from the data with Dagre auto-layout
  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Create nodes first (without positions)
    data.externalModels.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'external',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id },
      });
    });

    data.scheduledModels.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'scheduled',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id },
      });
    });

    data.incrementalModels.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'transformation',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id },
      });
    });

    // Create edges from dependencies
    [...data.incrementalModels, ...data.scheduledModels].forEach(model => {
      if (model.depends_on) {
        model.depends_on.forEach(dep => {
          edges.push({
            id: `${dep}-${model.id}`,
            source: dep,
            target: model.id,
            type: 'smoothstep',
            animated: true,
            style: { stroke: '#6366f1', strokeWidth: 2 },
            markerEnd: {
              type: 'arrowclosed' as const,
              color: '#6366f1',
            },
          });
        });
      }
    });

    // Use Dagre to calculate layout
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setDefaultEdgeLabel(() => ({}));

    // Configure the graph layout
    dagreGraph.setGraph({
      rankdir: 'TB', // Top to bottom
      align: 'UL', // Align nodes to upper left
      nodesep: 150, // Horizontal spacing between nodes
      ranksep: 180, // Vertical spacing between ranks
      ranker: 'network-simplex', // Use network-simplex for better layouts
      marginx: 50,
      marginy: 50,
    });

    // Add nodes to dagre graph with dimensions
    const nodeWidth = 220;
    const nodeHeight = 90;
    nodes.forEach(node => {
      dagreGraph.setNode(node.id, { width: nodeWidth, height: nodeHeight });
    });

    // Add edges to dagre graph
    edges.forEach(edge => {
      dagreGraph.setEdge(edge.source, edge.target);
    });

    // Calculate layout
    dagre.layout(dagreGraph);

    // Apply calculated positions to nodes
    nodes.forEach(node => {
      const nodeWithPosition = dagreGraph.node(node.id);
      node.position = {
        x: nodeWithPosition.x - nodeWidth / 2,
        y: nodeWithPosition.y - nodeHeight / 2,
      };
    });

    return { nodes, edges };
  }, [data]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Update nodes and edges when data loads
  useEffect(() => {
    if (initialNodes.length > 0) {
      setNodes(initialNodes);
    }
  }, [initialNodes, setNodes]);

  useEffect(() => {
    if (initialEdges.length > 0) {
      setEdges(initialEdges);
    }
  }, [initialEdges, setEdges]);

  // Get all ancestor nodes (nodes that this node depends on)
  const getAncestors = useCallback(
    (nodeId: string, visited = new Set<string>()): Set<string> => {
      if (visited.has(nodeId)) return new Set();
      visited.add(nodeId);

      const ancestors = new Set<string>();
      const incomingNodes = getIncomers({ id: nodeId } as Node, nodes, edges);

      incomingNodes.forEach(node => {
        ancestors.add(node.id);
        const nodeAncestors = getAncestors(node.id, new Set(visited));
        nodeAncestors.forEach(id => ancestors.add(id));
      });

      return ancestors;
    },
    [nodes, edges]
  );

  // Update node highlighting based on hover
  const onNodeMouseEnter = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      const ancestors = getAncestors(node.id);
      const highlighted = new Set([node.id, ...ancestors]);

      setNodes(nds =>
        nds.map(n => ({
          ...n,
          data: {
            ...n.data,
            isHighlighted: highlighted.has(n.id),
            isDimmed: !highlighted.has(n.id),
          },
        }))
      );
    },
    [getAncestors, setNodes]
  );

  const onNodeMouseLeave = useCallback(() => {
    setNodes(nds =>
      nds.map(n => ({
        ...n,
        data: {
          ...n.data,
          isHighlighted: false,
          isDimmed: false,
        },
      }))
    );
  }, [setNodes]);

  const onNodeClick = useCallback((_event: React.MouseEvent, node: Node) => {
    console.log('Clicked node:', node);
  }, []);

  return (
    <div
      className={`h-[500px] overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-900/40 shadow-xl ring-1 ring-slate-700/50 backdrop-blur-sm sm:h-[600px] lg:h-[700px] ${className}`}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={onNodeClick}
        onNodeMouseEnter={onNodeMouseEnter}
        onNodeMouseLeave={onNodeMouseLeave}
        nodeTypes={dagNodeTypes}
        colorMode="dark"
        fitView
        minZoom={0.1}
        maxZoom={2}
        defaultEdgeOptions={{
          type: 'smoothstep',
          animated: true,
        }}
        className="bg-slate-950"
      >
        <Background variant={BackgroundVariant.Dots} gap={20} size={1} />
        <Controls />
        <MiniMap
          nodeColor={node => {
            if (node.type === 'external') return '#10b981';
            if (node.type === 'scheduled') return '#10b981';
            return '#6366f1';
          }}
        />
      </ReactFlow>
    </div>
  );
}
