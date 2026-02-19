import type { Meta, StoryObj } from '@storybook/react-vite';
import { ModelCoverageRow } from './ModelCoverageRow';
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
      <div className="bg-background p-8">
        <Story />
        <Tooltip id="chunk-tooltip" className="!rounded !bg-gray-900 !px-2 !py-1 !text-xs !text-white !opacity-100" />
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
  hasOverride: false,
  isDisabled: false,
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
  hasOverride: false,
  isDisabled: false,
  data: {
    bounds: { min: 200, max: 800 },
  },
};

const scheduledModel: IncrementalModelItem = {
  id: 'beacon_api.scheduled_transformation',
  type: 'transformation',
  intervalType: 'none',
  hasOverride: false,
  isDisabled: false,
  data: {},
};

export const TransformationDefault: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const TransformationHighlighted: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    isHighlighted: true,
  },
};

export const TransformationDimmed: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
    isDimmed: true,
  },
};

export const ExternalDefault: Story = {
  args: {
    model: externalModel,
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const Scheduled: Story = {
  args: {
    model: scheduledModel,
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const WithoutLink: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const CustomNameWidth: Story = {
  args: {
    model: transformationModel,
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const ZoomedIn: Story = {
  args: {
    model: transformationModel,
    zoomStart: 350,
    zoomEnd: 600,
  },
};

export const WithOverrideIcon: Story = {
  args: {
    model: {
      ...transformationModel,
      hasOverride: true,
      isDisabled: false,
    },
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const DisabledIcon: Story = {
  args: {
    model: {
      ...transformationModel,
      hasOverride: true,
      isDisabled: true,
    },
    zoomStart: 0,
    zoomEnd: 1000,
  },
};
