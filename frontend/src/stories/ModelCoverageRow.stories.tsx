import type { Meta, StoryObj } from '@storybook/react-vite';
import { ModelCoverageRow } from '@/components/ModelCoverageRow';
import { Tooltip } from 'react-tooltip';
import type { IncrementalModelItem } from '@/types';

const meta = {
  title: 'Components/ModelCoverageRow',
  component: ModelCoverageRow,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
        <Tooltip id="chunk-tooltip" className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100" />
      </div>
    ),
  ],
} satisfies Meta<typeof ModelCoverageRow>;

export default meta;
type Story = StoryObj<typeof meta>;

const transformationModel: IncrementalModelItem = {
  id: 'beacon_api.beacon_api_eth_v1_beacon_states_state_id_validators',
  type: 'transformation',
  intervalType: 'slot_number',
  data: {
    coverage: [
      { position: 100, interval: 200 },
      { position: 400, interval: 150 },
      { position: 700, interval: 250 },
    ],
  },
};

const externalModel: IncrementalModelItem = {
  id: 'beacon_api.beacon_api_eth_v1_beacon_blob_sidecars',
  type: 'external',
  intervalType: 'slot_number',
  data: {
    bounds: { min: 200, max: 800 },
  },
};

const scheduledModel: IncrementalModelItem = {
  id: 'beacon_api.scheduled_transformation',
  type: 'transformation',
  intervalType: 'none',
  data: {},
};

export const TransformationDefault: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
  },
};

export const TransformationHighlighted: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
    isHighlighted: true,
  },
};

export const TransformationDimmed: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
    isDimmed: true,
  },
};

export const ExternalDefault: Story = {
  args: {
    model: externalModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
  },
};

export const Scheduled: Story = {
  args: {
    model: scheduledModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
  },
};

export const WithoutLink: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
  },
};

export const CustomNameWidth: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    globalMin: 0,
    globalMax: 1000,
    nameWidth: 'w-96',
  },
};

export const ZoomedIn: Story = {
  args: {
    model: transformationModel,
    zoomStart: 350,
    zoomEnd: 600,
    globalMin: 0,
    globalMax: 1000,
  },
};
