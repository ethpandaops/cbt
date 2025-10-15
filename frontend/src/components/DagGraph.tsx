import { type JSX, useCallback, useMemo, useEffect, useState } from 'react';
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
  useReactFlow,
} from '@xyflow/react';
import { useNavigate } from '@tanstack/react-router';
import { MagnifyingGlassIcon, XMarkIcon } from '@heroicons/react/24/outline';
import dagre from '@dagrejs/dagre';
import { ExternalNode, TransformationNode, ScheduledNode } from './DagNode';
import { useDagLayoutDirection } from '@hooks/useDagLayoutDirection';
import '@xyflow/react/dist/style.css';

const dagNodeTypes = {
  external: ExternalNode,
  transformation: TransformationNode,
  scheduled: ScheduledNode,
};

import type {
  ExternalModel,
  TransformationModel,
  ExternalBounds,
  CoverageSummary,
  IntervalTypeTransformation,
} from '@api/types.gen';

export interface DagData {
  externalModels: Array<ExternalModel>;
  incrementalModels: Array<TransformationModel>;
  scheduledModels: Array<TransformationModel>;
  bounds?: Array<ExternalBounds>;
  coverage?: Array<CoverageSummary>;
  intervalTypes?: Record<string, Array<IntervalTypeTransformation>>;
}

export interface DagGraphProps {
  data: DagData;
  className?: string;
  triggerFitView?: number; // Increment this value to trigger fitView
}

function DagGraphInner({ data, className = '', triggerFitView }: DagGraphProps): JSX.Element {
  const navigate = useNavigate();
  const { fitView } = useReactFlow();
  const { layoutDirection, setLayoutDirection } = useDagLayoutDirection();
  const [searchQuery, setSearchQuery] = useState('');
  const [hoveredNode, setHoveredNode] = useState<string | null>(null);
  const [isSearchFocused, setIsSearchFocused] = useState(false);

  // Build nodes and edges from the data with Dagre auto-layout
  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Create lookup maps for bounds and coverage
    const boundsMap = new Map(data.bounds?.map(b => [b.id, b]) || []);
    const coverageMap = new Map(data.coverage?.map(c => [c.id, c]) || []);

    // Create nodes first (without positions)
    data.externalModels.forEach(model => {
      const modelBounds = boundsMap.get(model.id);
      const intervalType = model.interval?.type;
      const transformation = intervalType && data.intervalTypes?.[intervalType]?.[0];

      nodes.push({
        id: model.id,
        type: 'external',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: {
          label: model.id,
          model: modelBounds ? { ...model, bounds: modelBounds } : model,
          transformation,
        },
      });
    });

    data.scheduledModels.forEach(model => {
      nodes.push({
        id: model.id,
        type: 'scheduled',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: { label: model.id, model },
      });
    });

    data.incrementalModels.forEach(model => {
      const modelCoverage = coverageMap.get(model.id);
      const intervalType = model.interval?.type;
      const transformation = intervalType && data.intervalTypes?.[intervalType]?.[0];

      nodes.push({
        id: model.id,
        type: 'transformation',
        position: { x: 0, y: 0 }, // Will be set by Dagre
        data: {
          label: model.id,
          model: modelCoverage ? { ...model, coverage: modelCoverage.ranges } : model,
          transformation,
        },
      });
    });

    // Create edges from dependencies with OR group tracking
    [...data.incrementalModels, ...data.scheduledModels].forEach(model => {
      if (model.depends_on) {
        model.depends_on.forEach(dep => {
          if (typeof dep === 'string') {
            // Regular AND dependency
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
          } else {
            // OR group dependency
            dep.forEach(orDep => {
              edges.push({
                id: `${orDep}-${model.id}`,
                source: orDep,
                target: model.id,
                type: 'smoothstep',
                animated: true,
                label: 'OR',
                labelStyle: {
                  fill: '#94a3b8',
                  fontSize: 10,
                  fontWeight: 600,
                  fontFamily: 'monospace',
                },
                labelBgStyle: {
                  fill: '#1e293b',
                  fillOpacity: 0.9,
                },
                labelBgPadding: [4, 6] as [number, number],
                labelBgBorderRadius: 4,
                style: { stroke: '#22d3ee', strokeWidth: 2, strokeDasharray: '5,5' },
                markerEnd: {
                  type: 'arrowclosed' as const,
                  color: '#22d3ee',
                },
              });
            });
          }
        });
      }
    });

    // Use Dagre to calculate layout
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setDefaultEdgeLabel(() => ({}));

    // Configure the graph layout
    dagreGraph.setGraph({
      rankdir: layoutDirection,
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
  }, [data, layoutDirection]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Apply search dimming effect to nodes when search is active and not hovering
  useEffect(() => {
    if (searchQuery && !hoveredNode) {
      const lowerQuery = searchQuery.toLowerCase();
      const matchingIds = new Set(nodes.filter(n => n.id.toLowerCase().includes(lowerQuery)).map(n => n.id));

      setNodes(nds =>
        nds.map(n => ({
          ...n,
          data: {
            ...n.data,
            isHighlighted: false,
            isDimmed: !matchingIds.has(n.id),
          },
        }))
      );
    } else if (!searchQuery && !hoveredNode) {
      // Clear all states when search is cleared and nothing is hovered
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
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchQuery, hoveredNode, setNodes]);

  // Calculate filtered count for stats
  const filteredNodes = useMemo(() => {
    if (!searchQuery) return nodes;
    const lowerQuery = searchQuery.toLowerCase();
    return nodes.filter(node => node.id.toLowerCase().includes(lowerQuery));
  }, [nodes, searchQuery]);

  // Calculate stats
  const stats = useMemo(() => {
    const total = nodes.length;
    const visible = filteredNodes.length;
    const external = nodes.filter(n => n.type === 'external').length;
    const scheduled = nodes.filter(n => n.type === 'scheduled').length;
    const incremental = nodes.filter(n => n.type === 'transformation').length;
    return { total, visible, external, scheduled, incremental };
  }, [nodes, filteredNodes]);

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

  // Fit view when layout direction changes
  useEffect(() => {
    // Add small delay to let layout calculate
    const timer = setTimeout(() => {
      fitView({ duration: 400, padding: 0.1 });
    }, 50);
    return () => clearTimeout(timer);
  }, [layoutDirection, fitView]);

  // Fit view when triggerFitView prop changes
  useEffect(() => {
    if (triggerFitView !== undefined && triggerFitView > 0) {
      const timer = setTimeout(() => {
        fitView({ duration: 400, padding: 0.1 });
      }, 50);
      return () => clearTimeout(timer);
    }
  }, [triggerFitView, fitView]);

  // Fit view to filtered nodes when searching (only if search is focused)
  useEffect(() => {
    if (searchQuery && filteredNodes.length > 0 && isSearchFocused) {
      // Fit view to only the filtered nodes
      const timer = setTimeout(() => {
        fitView({
          nodes: filteredNodes.map(n => ({ id: n.id })),
          duration: 400,
          padding: 0.2, // More padding to see context
        });
      }, 100); // Slight delay for smooth UX
      return () => clearTimeout(timer);
    }
  }, [searchQuery, filteredNodes, fitView, isSearchFocused]);

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

  // Update node highlighting based on hover (works with search)
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

      setHoveredNode(node.id);
    },
    [getAncestors, setNodes]
  );

  const onNodeMouseLeave = useCallback(() => {
    // Restore search state if searching, otherwise clear all
    if (searchQuery) {
      const lowerQuery = searchQuery.toLowerCase();
      const matchingIds = new Set(nodes.filter(n => n.id.toLowerCase().includes(lowerQuery)).map(n => n.id));

      setNodes(nds =>
        nds.map(n => ({
          ...n,
          data: {
            ...n.data,
            isHighlighted: false,
            isDimmed: !matchingIds.has(n.id),
          },
        }))
      );
    } else {
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
    }
    setHoveredNode(null);
  }, [setNodes, searchQuery, nodes]);

  const onNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      void navigate({ to: '/model/$id', params: { id: encodeURIComponent(node.id) } });
    },
    [navigate]
  );

  const onPaneMouseEnter = useCallback(() => {
    // Blur search input when entering the graph area
    if (document.activeElement instanceof HTMLInputElement) {
      document.activeElement.blur();
    }
  }, []);

  return (
    <div className={`flex flex-col gap-4 ${className}`}>
      {/* Search and Controls */}
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        {/* Search Bar */}
        <div className="relative w-full sm:flex-1 sm:max-w-md">
          <MagnifyingGlassIcon className="pointer-events-none absolute left-3 top-1/2 size-5 -translate-y-1/2 text-slate-400" />
          <input
            type="text"
            value={searchQuery}
            onChange={e => setSearchQuery(e.target.value)}
            onFocus={() => setIsSearchFocused(true)}
            onBlur={() => setIsSearchFocused(false)}
            placeholder="Search models..."
            className="w-full rounded-lg border border-slate-700/50 bg-slate-800/60 py-2.5 pl-10 pr-10 text-sm text-slate-200 placeholder-slate-400 transition-all focus:border-indigo-500 focus:outline-hidden focus:ring-2 focus:ring-indigo-500/50"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery('')}
              className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 transition-colors hover:text-slate-200"
            >
              <XMarkIcon className="size-4" />
            </button>
          )}
        </div>

        {/* Stats Badge */}
        <div className="flex items-center gap-2 rounded-lg bg-slate-800/60 px-3 py-2 text-xs font-medium text-slate-300 backdrop-blur-sm">
          <span>{searchQuery ? `${stats.visible} of ${stats.total}` : `${stats.total}`} models</span>
          <span className="text-slate-500">·</span>
          <span className="text-green-400">{stats.external} ext</span>
          <span className="text-slate-500">·</span>
          <span className="text-emerald-400">{stats.scheduled} sched</span>
          <span className="text-slate-500">·</span>
          <span className="text-indigo-400">{stats.incremental} incr</span>
        </div>
      </div>

      {/* Layout Direction Toggle */}
      <div className="flex items-center gap-2 rounded-lg bg-slate-800/40 p-1 backdrop-blur-sm w-fit">
        <button
          onClick={() => setLayoutDirection('TB')}
          className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold transition-all ${
            layoutDirection === 'TB'
              ? 'bg-indigo-500/20 text-indigo-300 shadow-sm'
              : 'text-slate-400 hover:bg-slate-700/60 hover:text-slate-300'
          }`}
        >
          <svg className="size-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 14l-7 7m0 0l-7-7m7 7V3" />
          </svg>
          Top → Bottom
        </button>
        <button
          onClick={() => setLayoutDirection('LR')}
          className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold transition-all ${
            layoutDirection === 'LR'
              ? 'bg-indigo-500/20 text-indigo-300 shadow-sm'
              : 'text-slate-400 hover:bg-slate-700/60 hover:text-slate-300'
          }`}
        >
          <svg className="size-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
          </svg>
          Left → Right
        </button>
        <button
          onClick={() => setLayoutDirection('RL')}
          className={`flex items-center gap-1.5 rounded-md px-3 py-1.5 text-xs font-semibold transition-all ${
            layoutDirection === 'RL'
              ? 'bg-indigo-500/20 text-indigo-300 shadow-sm'
              : 'text-slate-400 hover:bg-slate-700/60 hover:text-slate-300'
          }`}
        >
          <svg className="size-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Right → Left
        </button>
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
          onPaneMouseEnter={onPaneMouseEnter}
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
              if (node.type === 'external') return '#10b981'; // green-500
              if (node.type === 'scheduled') return '#10b981'; // emerald-500
              return '#6366f1'; // indigo-500
            }}
          />
        </ReactFlow>
      </div>
    </div>
  );
}

export function DagGraph(props: DagGraphProps): JSX.Element {
  return <DagGraphInner {...props} />;
}
