import type { Meta, StoryObj } from '@storybook/react-vite';
import { useRef } from 'react';
import { CoverageTooltip } from '@/components/CoverageTooltip';
import type { IncrementalModelItem } from '@/types';

const meta = {
  title: 'Components/CoverageTooltip',
  component: CoverageTooltip,
  parameters: {
    layout: 'fullscreen',
    docs: {
      story: {
        inline: false,
        iframeHeight: 400,
      },
    },
  },
  tags: ['autodocs'],
  decorators: [
    (Story, context) => {
      const containerRef = useRef<HTMLDivElement>(null);

      // Create mock model elements in the DOM for the tooltip to position against
      const models = context.args.allModels || [];

      return (
        <div ref={containerRef} className="relative min-h-screen bg-slate-950 p-8">
          <div className="space-y-4">
            {models.map((model: IncrementalModelItem) => (
              <div
                key={model.id}
                data-model-id={model.id}
                className="rounded-lg border border-slate-700 bg-slate-800 p-4"
              >
                <div className="mb-2 font-mono text-sm text-slate-300">{model.id}</div>
                <div style={{ height: '24px' }} className="rounded bg-slate-700" />
              </div>
            ))}
          </div>
          <Story args={{ ...context.args, containerRef }} />
        </div>
      );
    },
  ],
} satisfies Meta<typeof CoverageTooltip>;

export default meta;
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Story = StoryObj<any>;

const sampleModels: IncrementalModelItem[] = [
  {
    id: 'beacon_api.blocks',
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
    id: 'beacon_api.attestations',
    type: 'transformation',
    intervalType: 'slot_number',
    depends_on: ['beacon_api.blocks'],
    data: {
      coverage: [
        { position: 120, interval: 180 },
        { position: 420, interval: 100 },
      ],
    },
  },
  {
    id: 'beacon_api.validators',
    type: 'external',
    intervalType: 'slot_number',
    data: {
      bounds: { min: 50, max: 600 },
    },
  },
];

export const WithCoverage: Story = {
  args: {
    hoveredPosition: 150,
    hoveredModelId: 'beacon_api.blocks',
    mouseX: 300,
    allModels: sampleModels,
    dependencyIds: new Set(['beacon_api.attestations', 'beacon_api.validators']),
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const WithTransformation: Story = {
  args: {
    hoveredPosition: 10461696,
    hoveredModelId: 'beacon_api.blocks',
    mouseX: 350,
    allModels: [
      {
        id: 'beacon_api.blocks',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 10461696, interval: 7200 }],
        },
      },
      {
        id: 'beacon_api.attestations',
        type: 'transformation',
        intervalType: 'slot_number',
        depends_on: ['beacon_api.blocks'],
        data: {
          coverage: [{ position: 10461696, interval: 3600 }],
        },
      },
    ],
    dependencyIds: new Set(['beacon_api.attestations']),
    transformation: {
      name: 'Date',
      expression: '(value * 12) + 1606824000',
      format: 'date',
    },
    zoomStart: 10450000,
    zoomEnd: 10500000,
  },
};

export const MissingCoverage: Story = {
  args: {
    hoveredPosition: 350,
    hoveredModelId: 'beacon_api.blocks',
    mouseX: 400,
    allModels: [
      {
        id: 'beacon_api.blocks',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [
            { position: 100, interval: 200 },
            { position: 500, interval: 200 },
          ],
        },
      },
      {
        id: 'beacon_api.attestations',
        type: 'transformation',
        intervalType: 'slot_number',
        depends_on: ['beacon_api.blocks'],
        data: {
          coverage: [
            { position: 120, interval: 150 },
            { position: 520, interval: 150 },
          ],
        },
      },
    ],
    dependencyIds: new Set(['beacon_api.attestations']),
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const MultipleDependencies: Story = {
  args: {
    hoveredPosition: 250,
    hoveredModelId: 'beacon_api.complex_model',
    mouseX: 450,
    allModels: [
      {
        id: 'beacon_api.complex_model',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 200, interval: 300 }],
        },
      },
      {
        id: 'beacon_api.blocks',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 100, interval: 400 }],
        },
      },
      {
        id: 'beacon_api.attestations',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 150, interval: 300 }],
        },
      },
      {
        id: 'beacon_api.validators',
        type: 'external',
        intervalType: 'slot_number',
        data: {
          bounds: { min: 50, max: 600 },
        },
      },
      {
        id: 'beacon_api.proposers',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 180, interval: 250 }],
        },
      },
    ],
    dependencyIds: new Set([
      'beacon_api.blocks',
      'beacon_api.attestations',
      'beacon_api.validators',
      'beacon_api.proposers',
    ]),
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const ExternalModel: Story = {
  args: {
    hoveredPosition: 400,
    hoveredModelId: 'beacon_api.validators',
    mouseX: 500,
    allModels: [
      {
        id: 'beacon_api.validators',
        type: 'external',
        intervalType: 'slot_number',
        data: {
          bounds: { min: 200, max: 800 },
        },
      },
      {
        id: 'beacon_api.blocks',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 250, interval: 500 }],
        },
      },
    ],
    dependencyIds: new Set(['beacon_api.blocks']),
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const SingleModel: Story = {
  args: {
    hoveredPosition: 300,
    hoveredModelId: 'beacon_api.blocks',
    mouseX: 250,
    allModels: [
      {
        id: 'beacon_api.blocks',
        type: 'transformation',
        intervalType: 'slot_number',
        data: {
          coverage: [{ position: 200, interval: 400 }],
        },
      },
    ],
    dependencyIds: new Set(),
    zoomStart: 0,
    zoomEnd: 1000,
  },
};

export const ZoomedIn: Story = {
  args: {
    hoveredPosition: 450,
    hoveredModelId: 'beacon_api.blocks',
    mouseX: 350,
    allModels: sampleModels,
    dependencyIds: new Set(['beacon_api.attestations']),
    zoomStart: 350,
    zoomEnd: 600,
  },
  parameters: {
    docs: {
      description: {
        story: 'Tooltip shown in a zoomed-in view of the coverage range',
      },
    },
  },
};
