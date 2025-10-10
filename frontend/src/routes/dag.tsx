import { type JSX, useCallback, useMemo, useEffect } from 'react';
import { createFileRoute, Link } from '@tanstack/react-router';
import { useQuery } from '@tanstack/react-query';
import {
  listAllModelsOptions,
  listTransformationsOptions,
  listExternalModelsOptions,
} from '@api/@tanstack/react-query.gen';
import { ArrowPathIcon, XCircleIcon, ArrowLeftIcon } from '@heroicons/react/24/outline';
import {
  ReactFlow,
  ReactFlowProvider,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  BackgroundVariant,
  type Node,
  type Edge,
  Handle,
  Position,
  type NodeProps,
  getIncomers,
} from '@xyflow/react';
import dagre from 'dagre';
import '@xyflow/react/dist/style.css';

// Custom node components
function ExternalNode({ data }: NodeProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  return (
    <div className="relative">
      <Handle type="source" position={Position.Bottom} className="!bg-green-500" />
      <Link
        to="/model/$id"
        params={{ id: encodeURIComponent(label) }}
        className={`block rounded-lg border-2 p-4 shadow-lg ring-1 backdrop-blur-sm transition-all hover:scale-105 hover:shadow-xl ${
          isHighlighted
            ? 'border-green-400 bg-gradient-to-br from-green-900/60 to-green-800/60 ring-green-400/50 shadow-green-500/30'
            : isDimmed
              ? 'border-green-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-green-500/10'
              : 'border-green-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-green-500/30 hover:shadow-green-500/20'
        }`}
      >
        <div className="flex items-center gap-2">
          <div className="h-2 w-2 rounded-full bg-green-500" />
          <div className="font-mono text-xs font-bold text-green-300">EXTERNAL</div>
        </div>
        <div className="mt-2 font-mono text-sm font-semibold text-slate-100" title={label}>
          {label}
        </div>
      </Link>
    </div>
  );
}

function TransformationNode({ data }: NodeProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  return (
    <div className="relative">
      <Handle type="target" position={Position.Top} className="!bg-indigo-500" />
      <Handle type="source" position={Position.Bottom} className="!bg-indigo-500" />
      <Link
        to="/model/$id"
        params={{ id: encodeURIComponent(label) }}
        className={`block rounded-lg border-2 p-4 shadow-lg ring-1 backdrop-blur-sm transition-all hover:scale-105 hover:shadow-xl ${
          isHighlighted
            ? 'border-indigo-400 bg-gradient-to-br from-indigo-900/60 to-indigo-800/60 ring-indigo-400/50 shadow-indigo-500/30'
            : isDimmed
              ? 'border-indigo-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-indigo-500/10'
              : 'border-indigo-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-indigo-500/30 hover:shadow-indigo-500/20'
        }`}
      >
        <div className="flex items-center gap-2">
          <div className="h-2 w-2 rounded-full bg-indigo-500" />
          <div className="font-mono text-xs font-bold text-indigo-300">INCREMENTAL</div>
        </div>
        <div className="mt-2 font-mono text-sm font-semibold text-slate-100" title={label}>
          {label}
        </div>
      </Link>
    </div>
  );
}

function ScheduledNode({ data }: NodeProps): JSX.Element {
  const isHighlighted = data.isHighlighted as boolean;
  const isDimmed = data.isDimmed as boolean;
  const label = data.label as string;
  return (
    <div className="relative">
      <Handle type="target" position={Position.Top} className="!bg-emerald-500" />
      <Handle type="source" position={Position.Bottom} className="!bg-emerald-500" />
      <Link
        to="/model/$id"
        params={{ id: encodeURIComponent(label) }}
        className={`block rounded-lg border-2 p-4 shadow-lg ring-1 backdrop-blur-sm transition-all hover:scale-105 hover:shadow-xl ${
          isHighlighted
            ? 'border-emerald-400 bg-gradient-to-br from-emerald-900/60 to-emerald-800/60 ring-emerald-400/50 shadow-emerald-500/30'
            : isDimmed
              ? 'border-emerald-500/20 bg-gradient-to-br from-slate-900/40 to-slate-950/40 opacity-30 ring-emerald-500/10'
              : 'border-emerald-500/50 bg-gradient-to-br from-slate-800 to-slate-900 ring-emerald-500/30 hover:shadow-emerald-500/20'
        }`}
      >
        <div className="flex items-center gap-2">
          <div className="h-2 w-2 rounded-full bg-emerald-500" />
          <div className="font-mono text-xs font-bold text-emerald-300">SCHEDULED</div>
        </div>
        <div className="mt-2 font-mono text-sm font-semibold text-slate-100" title={label}>
          {label}
        </div>
      </Link>
    </div>
  );
}

// Custom node types for different model types
const nodeTypes = {
  external: ExternalNode,
  transformation: TransformationNode,
  scheduled: ScheduledNode,
};

function DagComponent(): JSX.Element {
  // Fetch all model data
  const allModels = useQuery(listAllModelsOptions());
  const transformations = useQuery(listTransformationsOptions());
  const externalModels = useQuery(listExternalModelsOptions());

  // Build nodes and edges from the data with Dagre auto-layout
  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    if (!allModels.data || !transformations.data || !externalModels.data) {
      return { nodes: [], edges: [] };
    }

    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Group models by type
    const externalList = externalModels.data.models;
    const incrementalList = transformations.data.models.filter(m => m.type === 'incremental');
    const scheduledList = transformations.data.models.filter(m => m.type === 'scheduled');

    // Create nodes first (without positions)
    externalList.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'external',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id },
      });
    });

    scheduledList.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'scheduled',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id },
      });
    });

    incrementalList.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'transformation',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id },
      });
    });

    // Create edges from dependencies
    transformations.data.models.forEach(model => {
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
      nodesep: 150, // Horizontal spacing between nodes (increased from 100)
      ranksep: 180, // Vertical spacing between ranks (increased from 150)
      ranker: 'network-simplex', // Use network-simplex for better layouts
      marginx: 50,
      marginy: 50,
    });

    // Add nodes to dagre graph with dimensions (increased to account for actual rendered size)
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

    console.log('Generated nodes:', nodes.length, 'edges:', edges.length);
    return { nodes, edges };
  }, [allModels.data, transformations.data, externalModels.data]);

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

  console.log('Current nodes state:', nodes.length, 'edges state:', edges.length);

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

  if (allModels.isLoading || transformations.isLoading || externalModels.isLoading) {
    return (
      <div className="flex items-center gap-3 text-slate-400">
        <ArrowPathIcon className="h-5 w-5 animate-spin text-indigo-400" />
        <span className="font-medium">Loading DAG...</span>
      </div>
    );
  }

  if (allModels.error || transformations.error || externalModels.error) {
    return (
      <div className="rounded-xl border border-red-500/50 bg-gradient-to-br from-red-950/80 to-red-900/50 p-4 shadow-md">
        <div className="flex items-center gap-2">
          <XCircleIcon className="h-5 w-5 shrink-0 text-red-400" />
          <span className="font-semibold text-red-200">
            Error: {allModels.error?.message || transformations.error?.message || externalModels.error?.message}
          </span>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 top-[120px] flex flex-col px-6 pb-6">
      <div className="mb-6 flex items-center justify-between">
        <div className="flex items-baseline gap-4">
          <h1 className="bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-4xl font-black tracking-tight text-transparent">
            Dependency DAG
          </h1>
          <div className="flex items-center gap-3 text-xs">
            <div className="flex items-center gap-2">
              <div className="h-3 w-3 rounded-sm bg-green-500" />
              <span className="font-medium text-slate-400">External</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="h-3 w-3 rounded-sm bg-emerald-500" />
              <span className="font-medium text-slate-400">Scheduled</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="h-3 w-3 rounded-sm bg-indigo-500" />
              <span className="font-medium text-slate-400">Incremental</span>
            </div>
          </div>
        </div>
        <Link
          to="/"
          className="group inline-flex items-center gap-2 rounded-lg bg-slate-800/60 px-4 py-2 text-sm font-medium text-slate-300 shadow-sm ring-1 ring-slate-700/50 backdrop-blur-sm transition-all hover:bg-slate-800/80 hover:text-indigo-400 hover:shadow-md hover:ring-indigo-500/50"
        >
          <ArrowLeftIcon className="size-4 transition-transform group-hover:-translate-x-0.5" />
          Back to Dashboard
        </Link>
      </div>

      <div className="flex-1 overflow-hidden rounded-2xl border border-indigo-500/30 bg-slate-900/40 shadow-xl ring-1 ring-slate-700/50 backdrop-blur-sm">
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={onNodeClick}
          onNodeMouseEnter={onNodeMouseEnter}
          onNodeMouseLeave={onNodeMouseLeave}
          nodeTypes={nodeTypes}
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
    </div>
  );
}

function DagPage(): JSX.Element {
  return (
    <ReactFlowProvider>
      <DagComponent />
    </ReactFlowProvider>
  );
}

export const Route = createFileRoute('/dag')({
  component: DagPage,
});
