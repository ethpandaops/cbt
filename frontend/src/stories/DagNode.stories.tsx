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
  isDimmed = false,
  model?: unknown,
  transformation?: unknown
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
          model,
          transformation,
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

// Stories with model data showing enhanced information
export const ExternalWithData: Story = createNodeStory(
  'external',
  'beacon_api.blocks',
  false,
  false,
  {
    id: 'beacon_api.blocks',
    type: 'external',
    interval: { type: 'slot_number' },
    bounds: { min: 9800000, max: 10500000 },
  },
  { name: 'Slot Number', expression: 'value' }
);

export const ExternalWithDateTransform: Story = createNodeStory(
  'external',
  'beacon_api.validators',
  false,
  false,
  {
    id: 'beacon_api.validators',
    type: 'external',
    interval: { type: 'slot_number' },
    bounds: { min: 10000000, max: 10500000 },
  },
  { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' }
);

export const IncrementalWithSQL: Story = createNodeStory(
  'transformation',
  'beacon_api.slot_coverage',
  false,
  false,
  {
    id: 'beacon_api.slot_coverage',
    type: 'incremental',
    content_type: 'sql',
    interval: { type: 'slot_number' },
    coverage: [
      { position: 10000000, interval: 500000 },
      { position: 10600000, interval: 400000 },
    ],
  },
  { name: 'Epoch', expression: 'value / 32' }
);

export const IncrementalWithBackfill: Story = createNodeStory(
  'transformation',
  'beacon_api.attestations',
  false,
  false,
  {
    id: 'beacon_api.attestations',
    type: 'incremental',
    content_type: 'sql',
    interval: { type: 'slot_number' },
    schedules: { backfill: '0 2 * * * *', forwardfill: '*/5 * * * *' },
    coverage: [{ position: 10000000, interval: 1000000 }],
  },
  { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' }
);

export const IncrementalWithExec: Story = createNodeStory(
  'transformation',
  'beacon_api.custom_processor',
  false,
  false,
  {
    id: 'beacon_api.custom_processor',
    type: 'incremental',
    content_type: 'exec',
    interval: { type: 'slot_number' },
    coverage: [{ position: 10000000, interval: 500000 }],
  }
);

export const ScheduledWithSchedule: Story = createNodeStory('scheduled', 'beacon_api.daily_summary', false, false, {
  id: 'beacon_api.daily_summary',
  type: 'scheduled',
  content_type: 'sql',
  schedule: '0 0 * * *',
});

export const ScheduledCronExpression: Story = createNodeStory('scheduled', 'beacon_api.hourly_report', false, false, {
  id: 'beacon_api.hourly_report',
  type: 'scheduled',
  content_type: 'sql',
  schedule: '0 * * * *',
});

// Highlighted/Dimmed variants with data
export const IncrementalHighlightedWithData: Story = createNodeStory(
  'transformation',
  'beacon_api.slot_coverage',
  true,
  false,
  {
    id: 'beacon_api.slot_coverage',
    type: 'incremental',
    content_type: 'sql',
    interval: { type: 'slot_number' },
    schedules: { backfill: '0 2 * * *' },
    coverage: [{ position: 10000000, interval: 1000000 }],
  },
  { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' }
);

export const IncrementalDimmedWithData: Story = createNodeStory(
  'transformation',
  'beacon_api.slot_coverage',
  false,
  true,
  {
    id: 'beacon_api.slot_coverage',
    type: 'incremental',
    content_type: 'sql',
    interval: { type: 'slot_number' },
    coverage: [{ position: 10000000, interval: 1000000 }],
  }
);
