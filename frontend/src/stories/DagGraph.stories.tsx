import type { Meta, StoryObj } from '@storybook/react-vite';
import { ReactFlowProvider } from '@xyflow/react';
import { DagGraph, type DagData } from '@/components/DagGraph';

const meta = {
  title: 'Components/DagGraph',
  component: DagGraph,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <ReactFlowProvider>
        <div className="flex h-screen flex-col bg-slate-950 p-8">
          <Story />
        </div>
      </ReactFlowProvider>
    ),
  ],
} satisfies Meta<typeof DagGraph>;

export default meta;
type Story = StoryObj<typeof meta>;

const simpleData: DagData = {
  externalModels: [{ id: 'external.source_data' }],
  incrementalModels: [
    { id: 'transform.step1', depends_on: ['external.source_data'] },
    { id: 'transform.step2', depends_on: ['transform.step1'] },
  ],
  scheduledModels: [],
};

const complexData: DagData = {
  externalModels: [{ id: 'beacon_api.blocks' }, { id: 'beacon_api.attestations' }],
  incrementalModels: [
    { id: 'beacon_api.validators', depends_on: ['beacon_api.blocks'] },
    { id: 'beacon_api.committees', depends_on: ['beacon_api.blocks', 'beacon_api.attestations'] },
    { id: 'beacon_api.aggregated_attestations', depends_on: ['beacon_api.committees'] },
  ],
  scheduledModels: [{ id: 'beacon_api.daily_summary', depends_on: ['beacon_api.validators', 'beacon_api.committees'] }],
};

const largeData: DagData = {
  externalModels: [
    { id: 'external.source_a' },
    { id: 'external.source_b' },
    { id: 'external.source_c' },
    { id: 'external.source_d' },
  ],
  incrementalModels: [
    { id: 'transform.layer1_a', depends_on: ['external.source_a'] },
    { id: 'transform.layer1_b', depends_on: ['external.source_b'] },
    { id: 'transform.layer1_c', depends_on: ['external.source_c', 'external.source_d'] },
    { id: 'transform.layer2_a', depends_on: ['transform.layer1_a', 'transform.layer1_b'] },
    { id: 'transform.layer2_b', depends_on: ['transform.layer1_c'] },
    { id: 'transform.layer3', depends_on: ['transform.layer2_a', 'transform.layer2_b'] },
  ],
  scheduledModels: [
    { id: 'scheduled.daily_report', depends_on: ['transform.layer3'] },
    { id: 'scheduled.weekly_summary', depends_on: ['transform.layer3'] },
  ],
};

const emptyData: DagData = {
  externalModels: [],
  incrementalModels: [],
  scheduledModels: [],
};

export const Simple: Story = {
  args: {
    data: simpleData,
    className: 'flex-1',
    showLinks: false,
  },
};

export const Complex: Story = {
  args: {
    data: complexData,
    className: 'flex-1',
    showLinks: false,
  },
};

export const Large: Story = {
  args: {
    data: largeData,
    className: 'flex-1',
    showLinks: false,
  },
};

export const Empty: Story = {
  args: {
    data: emptyData,
    className: 'flex-1',
    showLinks: false,
  },
};

export const OnlyExternal: Story = {
  args: {
    data: {
      externalModels: [{ id: 'external.data1' }, { id: 'external.data2' }, { id: 'external.data3' }],
      incrementalModels: [],
      scheduledModels: [],
    },
    className: 'flex-1',
    showLinks: false,
  },
};

export const OnlyScheduled: Story = {
  args: {
    data: {
      externalModels: [],
      incrementalModels: [],
      scheduledModels: [{ id: 'scheduled.job1' }, { id: 'scheduled.job2' }],
    },
    className: 'flex-1',
    showLinks: false,
  },
};
