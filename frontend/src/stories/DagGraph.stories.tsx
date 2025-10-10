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
  externalModels: [
    {
      id: 'external.source_data',
      database: 'external',
      table: 'source_data',
    },
  ],
  incrementalModels: [
    {
      id: 'transform.step1',
      database: 'transform',
      table: 'step1',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM external.source_data',
      depends_on: ['external.source_data'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
    {
      id: 'transform.step2',
      database: 'transform',
      table: 'step2',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM transform.step1',
      depends_on: ['transform.step1'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
  ],
  scheduledModels: [],
};

const complexData: DagData = {
  externalModels: [
    { id: 'beacon_api.blocks', database: 'beacon_api', table: 'blocks' },
    { id: 'beacon_api.attestations', database: 'beacon_api', table: 'attestations' },
  ],
  incrementalModels: [
    {
      id: 'beacon_api.validators',
      database: 'beacon_api',
      table: 'validators',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM beacon_api.blocks',
      depends_on: ['beacon_api.blocks'],
      interval: { min: 1, max: 32, type: 'epoch' },
      schedules: { forwardfill: '*/5 * * * *', backfill: '0 */6 * * *' },
    },
    {
      id: 'beacon_api.committees',
      database: 'beacon_api',
      table: 'committees',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM beacon_api.blocks JOIN beacon_api.attestations',
      depends_on: ['beacon_api.blocks', 'beacon_api.attestations'],
      interval: { min: 1, max: 32, type: 'epoch' },
    },
    {
      id: 'beacon_api.aggregated_attestations',
      database: 'beacon_api',
      table: 'aggregated_attestations',
      type: 'incremental',
      content_type: 'exec',
      content: '/usr/bin/aggregate_attestations.sh',
      depends_on: ['beacon_api.committees'],
      interval: { min: 1, max: 32, type: 'epoch' },
    },
  ],
  scheduledModels: [
    {
      id: 'beacon_api.daily_summary',
      database: 'beacon_api',
      table: 'daily_summary',
      type: 'scheduled',
      content_type: 'sql',
      content: 'SELECT * FROM beacon_api.validators JOIN beacon_api.committees',
      depends_on: ['beacon_api.validators', 'beacon_api.committees'],
      schedule: '0 0 * * *',
    },
  ],
};

const largeData: DagData = {
  externalModels: [
    { id: 'external.source_a', database: 'external', table: 'source_a' },
    { id: 'external.source_b', database: 'external', table: 'source_b' },
    { id: 'external.source_c', database: 'external', table: 'source_c' },
    { id: 'external.source_d', database: 'external', table: 'source_d' },
  ],
  incrementalModels: [
    {
      id: 'transform.layer1_a',
      database: 'transform',
      table: 'layer1_a',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM external.source_a',
      depends_on: ['external.source_a'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
    {
      id: 'transform.layer1_b',
      database: 'transform',
      table: 'layer1_b',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM external.source_b',
      depends_on: ['external.source_b'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
    {
      id: 'transform.layer1_c',
      database: 'transform',
      table: 'layer1_c',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM external.source_c JOIN external.source_d',
      depends_on: ['external.source_c', 'external.source_d'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
    {
      id: 'transform.layer2_a',
      database: 'transform',
      table: 'layer2_a',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM transform.layer1_a JOIN transform.layer1_b',
      depends_on: ['transform.layer1_a', 'transform.layer1_b'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
    {
      id: 'transform.layer2_b',
      database: 'transform',
      table: 'layer2_b',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM transform.layer1_c',
      depends_on: ['transform.layer1_c'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
    {
      id: 'transform.layer3',
      database: 'transform',
      table: 'layer3',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM transform.layer2_a JOIN transform.layer2_b',
      depends_on: ['transform.layer2_a', 'transform.layer2_b'],
      interval: { min: 1, max: 100, type: 'slot' },
    },
  ],
  scheduledModels: [
    {
      id: 'scheduled.daily_report',
      database: 'scheduled',
      table: 'daily_report',
      type: 'scheduled',
      content_type: 'sql',
      content: 'SELECT * FROM transform.layer3',
      depends_on: ['transform.layer3'],
      schedule: '0 0 * * *',
    },
    {
      id: 'scheduled.weekly_summary',
      database: 'scheduled',
      table: 'weekly_summary',
      type: 'scheduled',
      content_type: 'sql',
      content: 'SELECT * FROM transform.layer3',
      depends_on: ['transform.layer3'],
      schedule: '0 0 * * 0',
    },
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
  },
};

export const Complex: Story = {
  args: {
    data: complexData,
    className: 'flex-1',
  },
};

export const Large: Story = {
  args: {
    data: largeData,
    className: 'flex-1',
  },
};

export const Empty: Story = {
  args: {
    data: emptyData,
    className: 'flex-1',
  },
};

export const OnlyExternal: Story = {
  args: {
    data: {
      externalModels: [
        { id: 'external.data1', database: 'external', table: 'data1' },
        { id: 'external.data2', database: 'external', table: 'data2' },
        { id: 'external.data3', database: 'external', table: 'data3' },
      ],
      incrementalModels: [],
      scheduledModels: [],
    },
    className: 'flex-1',
  },
};

export const OnlyScheduled: Story = {
  args: {
    data: {
      externalModels: [],
      incrementalModels: [],
      scheduledModels: [
        {
          id: 'scheduled.job1',
          database: 'scheduled',
          table: 'job1',
          type: 'scheduled',
          content_type: 'sql',
          content: 'SELECT 1',
          schedule: '0 * * * *',
        },
        {
          id: 'scheduled.job2',
          database: 'scheduled',
          table: 'job2',
          type: 'scheduled',
          content_type: 'sql',
          content: 'SELECT 2',
          schedule: '0 0 * * *',
        },
      ],
    },
    className: 'flex-1',
  },
};
