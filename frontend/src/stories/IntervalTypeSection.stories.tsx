import type { Meta, StoryObj } from '@storybook/react-vite';
import { IntervalTypeSection } from '@/components/IntervalTypeSection';
import { Tooltip } from 'react-tooltip';
import type { IncrementalModelItem } from '@/types';

const meta = {
  title: 'Components/IntervalTypeSection',
  component: IntervalTypeSection,
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
} satisfies Meta<typeof IntervalTypeSection>;

export default meta;
type Story = StoryObj<typeof meta>;

const sampleModels: IncrementalModelItem[] = [
  {
    id: 'beacon_api.beacon_api_eth_v1_beacon_states_state_id_validators',
    type: 'transformation',
    intervalType: 'slot_number',
    data: {
      coverage: [
        { position: 100, interval: 200 },
        { position: 400, interval: 150 },
      ],
    },
  },
  {
    id: 'beacon_api.beacon_api_eth_v1_beacon_blob_sidecars',
    type: 'external',
    intervalType: 'slot_number',
    data: {
      bounds: { min: 200, max: 800 },
    },
  },
  {
    id: 'beacon_api.another_transformation',
    type: 'transformation',
    intervalType: 'slot_number',
    data: {
      coverage: [
        { position: 300, interval: 100 },
        { position: 600, interval: 200 },
      ],
    },
  },
];

export const Default: Story = {
  args: {
    intervalType: 'slot_number',
    models: sampleModels,
    zoomRange: { start: 0, end: 1000 },
    globalMin: 0,
    globalMax: 1000,
    transformations: [
      {
        name: 'Slot Number',
        expression: 'x',
      },
    ],
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
    onResetZoom: () => console.log('Reset zoom'),
    showLinks: false,
  },
};

export const WithMultipleTransformations: Story = {
  args: {
    intervalType: 'slot_number',
    models: sampleModels,
    zoomRange: { start: 0, end: 1000 },
    globalMin: 0,
    globalMax: 1000,
    transformations: [
      {
        name: 'Slot Number',
        expression: 'x',
      },
      {
        name: 'Date',
        expression: '(x * 12) + 1606824000',
        format: 'date',
      },
      {
        name: 'Epoch',
        expression: 'x / 32',
      },
    ],
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
    onResetZoom: () => console.log('Reset zoom'),
    showLinks: false,
  },
};

export const ZoomedIn: Story = {
  args: {
    intervalType: 'slot_number',
    models: sampleModels,
    zoomRange: { start: 300, end: 700 },
    globalMin: 0,
    globalMax: 1000,
    transformations: [
      {
        name: 'Slot Number',
        expression: 'x',
      },
    ],
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
    onResetZoom: () => console.log('Reset zoom'),
    showLinks: false,
  },
};

export const SingleModel: Story = {
  args: {
    intervalType: 'slot_number',
    models: [sampleModels[0]],
    zoomRange: { start: 0, end: 1000 },
    globalMin: 0,
    globalMax: 1000,
    transformations: [
      {
        name: 'Slot Number',
        expression: 'x',
      },
    ],
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
    onResetZoom: () => console.log('Reset zoom'),
    showLinks: false,
  },
};

// Story with complex dependencies and gaps to showcase multi-tooltip feature
const modelsWithDependencies: IncrementalModelItem[] = [
  {
    id: 'base_model',
    type: 'external',
    intervalType: 'slot_number',
    data: {
      bounds: { min: 0, max: 1000 },
    },
  },
  {
    id: 'model_a',
    type: 'transformation',
    intervalType: 'slot_number',
    depends_on: ['base_model'],
    data: {
      coverage: [
        { position: 100, interval: 200 },
        { position: 400, interval: 300 },
        { position: 800, interval: 150 },
      ],
    },
  },
  {
    id: 'model_b',
    type: 'transformation',
    intervalType: 'slot_number',
    depends_on: ['model_a'],
    data: {
      coverage: [
        { position: 150, interval: 100 },
        { position: 450, interval: 200 },
        // Gap from 650-800
        { position: 850, interval: 50 },
      ],
    },
  },
  {
    id: 'model_c',
    type: 'transformation',
    intervalType: 'slot_number',
    depends_on: ['model_a'],
    data: {
      coverage: [
        // Gap from 0-200
        { position: 200, interval: 150 },
        { position: 500, interval: 100 },
        { position: 700, interval: 200 },
      ],
    },
  },
  {
    id: 'model_d_long_name_for_testing',
    type: 'transformation',
    intervalType: 'slot_number',
    depends_on: ['model_b', 'model_c'],
    data: {
      coverage: [
        { position: 250, interval: 50 },
        { position: 550, interval: 100 },
        // Large gap from 650-900
        { position: 900, interval: 50 },
      ],
    },
  },
  {
    id: 'model_e',
    type: 'transformation',
    intervalType: 'slot_number',
    depends_on: ['model_d_long_name_for_testing'],
    data: {
      coverage: [
        // Sparse coverage to show lots of gaps
        { position: 260, interval: 30 },
        { position: 920, interval: 20 },
      ],
    },
  },
];

export const WithDependenciesAndGaps: Story = {
  args: {
    intervalType: 'slot_number',
    models: modelsWithDependencies,
    zoomRange: { start: 0, end: 1000 },
    globalMin: 0,
    globalMax: 1000,
    transformations: [
      {
        name: 'Slot Number',
        expression: 'x',
      },
    ],
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
    onResetZoom: () => console.log('Reset zoom'),
    showLinks: false,
  },
};

// Interactive story - removed since Storybook 9 requires args for all stories
