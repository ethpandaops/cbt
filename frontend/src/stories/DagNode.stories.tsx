import type { Meta, StoryObj } from '@storybook/react-vite';
import { ReactFlow, ReactFlowProvider } from '@xyflow/react';
import { ExternalNode, TransformationNode, ScheduledNode } from '@/components/DagNode';
import '@xyflow/react/dist/style.css';

const dagNodeTypes = {
  external: ExternalNode,
  transformation: TransformationNode,
  scheduled: ScheduledNode,
};

const meta = {
  title: 'Components/DagNode',
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
} satisfies Meta;

export default meta;
type Story = StoryObj<typeof meta>;

const createNodeStory = (
  nodeType: 'external' | 'transformation' | 'scheduled',
  label: string,
  isHighlighted = false,
  isDimmed = false
): Story => ({
  render: () => {
    const nodes = [
      {
        id: '1',
        type: nodeType,
        position: { x: 250, y: 100 },
        data: {
          label,
          isHighlighted,
          isDimmed,
        },
      },
    ];

    return (
      <ReactFlowProvider>
        <div className="h-[300px] w-[600px] bg-slate-950">
          <ReactFlow
            nodes={nodes}
            nodeTypes={dagNodeTypes}
            colorMode="dark"
            fitView
            panOnDrag={false}
            zoomOnScroll={false}
            nodesDraggable={false}
          />
        </div>
      </ReactFlowProvider>
    );
  },
});

export const ExternalDefault: Story = createNodeStory('external', 'beacon_api.external_source');

export const ExternalHighlighted: Story = createNodeStory('external', 'beacon_api.external_source', true);

export const ExternalDimmed: Story = createNodeStory('external', 'beacon_api.external_source', false, true);

export const TransformationDefault: Story = createNodeStory('transformation', 'beacon_api.incremental_model');

export const TransformationHighlighted: Story = createNodeStory('transformation', 'beacon_api.incremental_model', true);

export const TransformationDimmed: Story = createNodeStory(
  'transformation',
  'beacon_api.incremental_model',
  false,
  true
);

export const ScheduledDefault: Story = createNodeStory('scheduled', 'beacon_api.scheduled_job');

export const ScheduledHighlighted: Story = createNodeStory('scheduled', 'beacon_api.scheduled_job', true);

export const ScheduledDimmed: Story = createNodeStory('scheduled', 'beacon_api.scheduled_job', false, true);

export const LongName: Story = createNodeStory(
  'transformation',
  'beacon_api.very_long_model_name_that_might_need_to_wrap_or_truncate'
);
