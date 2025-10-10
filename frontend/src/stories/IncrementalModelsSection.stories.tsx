import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn } from 'storybook/test';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { IncrementalModelsSection } from '@/components/IncrementalModelsSection';
import {
  listTransformationsQueryKey,
  listTransformationCoverageQueryKey,
  listExternalModelsQueryKey,
  listExternalBoundsQueryKey,
  getIntervalTypesQueryKey,
} from '@api/@tanstack/react-query.gen';
import type {
  ListTransformationsResponse,
  ListTransformationCoverageResponse,
  ListExternalModelsResponse,
  ListExternalBoundsResponse,
  GetIntervalTypesResponse,
} from '@api/types.gen';

const meta = {
  title: 'Components/IncrementalModelsSection',
  component: IncrementalModelsSection,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  argTypes: {
    zoomRanges: {
      control: 'object',
      description: 'Zoom ranges for each interval type',
    },
  },
  decorators: [
    (Story, context) => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: {
            retry: false,
            staleTime: Infinity,
          },
        },
      });

      // Get mock data from story parameters (not args, to avoid type conflicts)
      const storyParams = context.parameters as {
        mockData?: {
          transformations?: ListTransformationsResponse;
          coverage?: ListTransformationCoverageResponse;
          externalModels?: ListExternalModelsResponse;
          bounds?: ListExternalBoundsResponse;
          intervalTypes?: GetIntervalTypesResponse;
        };
      };

      const mockData = storyParams.mockData || {};

      // Pre-populate the QueryClient cache using the actual query key generators
      if (mockData.transformations) {
        queryClient.setQueryData(listTransformationsQueryKey(), mockData.transformations);
      }
      if (mockData.coverage) {
        queryClient.setQueryData(listTransformationCoverageQueryKey(), mockData.coverage);
      }
      if (mockData.externalModels) {
        queryClient.setQueryData(listExternalModelsQueryKey(), mockData.externalModels);
      }
      if (mockData.bounds) {
        queryClient.setQueryData(listExternalBoundsQueryKey(), mockData.bounds);
      }
      if (mockData.intervalTypes) {
        queryClient.setQueryData(getIntervalTypesQueryKey(), mockData.intervalTypes);
      }

      const storyArgs = context.args as React.ComponentProps<typeof IncrementalModelsSection>;

      return (
        <QueryClientProvider client={queryClient}>
          <div className="bg-slate-950 p-8">
            <IncrementalModelsSection {...storyArgs} />
          </div>
        </QueryClientProvider>
      );
    },
  ],
} satisfies Meta<typeof IncrementalModelsSection>;

export default meta;
type Story = StoryObj<typeof meta>;

const mockTransformations: ListTransformationsResponse = {
  models: [
    {
      id: 'beacon_api.blocks',
      database: 'beacon_api',
      table: 'blocks',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM blocks',
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
    {
      id: 'beacon_api.attestations',
      database: 'beacon_api',
      table: 'attestations',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM attestations',
      depends_on: ['beacon_api.blocks'],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
    {
      id: 'beacon_api.proposers',
      database: 'beacon_api',
      table: 'proposers',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM proposers',
      depends_on: ['beacon_api.blocks'],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  ],
  total: 3,
};

const mockCoverage: ListTransformationCoverageResponse = {
  coverage: [
    {
      id: 'beacon_api.blocks',
      ranges: [
        { position: 10000000, interval: 500000 },
        { position: 10600000, interval: 400000 },
      ],
    },
    {
      id: 'beacon_api.attestations',
      ranges: [
        { position: 10050000, interval: 450000 },
        { position: 10650000, interval: 300000 },
      ],
    },
    {
      id: 'beacon_api.proposers',
      ranges: [
        { position: 10100000, interval: 400000 },
        { position: 10700000, interval: 250000 },
      ],
    },
  ],
  total: 3,
};

const mockExternalModels: ListExternalModelsResponse = {
  models: [
    {
      id: 'beacon_api.validators',
      database: 'beacon_api',
      table: 'validators',
      interval: {
        type: 'slot_number',
      },
    },
  ],
  total: 1,
};

const mockBounds: ListExternalBoundsResponse = {
  bounds: [
    {
      id: 'beacon_api.validators',
      min: 9800000,
      max: 11000000,
    },
  ],
  total: 1,
};

const mockIntervalTypes: GetIntervalTypesResponse = {
  interval_types: {
    slot_number: [
      { name: 'Slot Number', expression: 'value' },
      { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
      { name: 'Epoch', expression: 'value / 32' },
    ],
  },
};

export const Default: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: mockTransformations,
      coverage: mockCoverage,
      externalModels: mockExternalModels,
      bounds: mockBounds,
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const WithZoom: Story = {
  args: {
    zoomRanges: {
      slot_number: { start: 10200000, end: 10800000 },
    },
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: mockTransformations,
      coverage: mockCoverage,
      externalModels: mockExternalModels,
      bounds: mockBounds,
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const MultipleIntervalTypes: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: {
        models: [
          ...mockTransformations.models,
          {
            id: 'beacon_api.epochs',
            database: 'beacon_api',
            table: 'epochs',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM epochs',
            interval: {
              type: 'epoch_number',
              min: 1,
              max: 225,
            },
          },
          {
            id: 'beacon_api.validators_by_epoch',
            database: 'beacon_api',
            table: 'validators_by_epoch',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM validators',
            depends_on: ['beacon_api.epochs'],
            interval: {
              type: 'epoch_number',
              min: 1,
              max: 225,
            },
          },
        ],
        total: 5,
      },
      coverage: {
        coverage: [
          ...mockCoverage.coverage,
          {
            id: 'beacon_api.epochs',
            ranges: [
              { position: 300000, interval: 10000 },
              { position: 315000, interval: 5000 },
            ],
          },
          {
            id: 'beacon_api.validators_by_epoch',
            ranges: [{ position: 301000, interval: 8000 }],
          },
        ],
        total: 5,
      },
      externalModels: mockExternalModels,
      bounds: mockBounds,
      intervalTypes: {
        interval_types: {
          slot_number: [
            { name: 'Slot Number', expression: 'value' },
            { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
          ],
          epoch_number: [
            { name: 'Epoch', expression: 'value' },
            { name: 'Day', expression: 'value / 225' },
          ],
        },
      },
    },
  },
};

export const ManyModels: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: {
        models: [
          {
            id: 'beacon_api.blocks',
            database: 'beacon_api',
            table: 'blocks',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM blocks',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.attestations',
            database: 'beacon_api',
            table: 'attestations',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM attestations',
            depends_on: ['beacon_api.blocks'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.proposers',
            database: 'beacon_api',
            table: 'proposers',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM proposers',
            depends_on: ['beacon_api.blocks'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.deposits',
            database: 'beacon_api',
            table: 'deposits',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM deposits',
            depends_on: ['beacon_api.blocks'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.withdrawals',
            database: 'beacon_api',
            table: 'withdrawals',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM withdrawals',
            depends_on: ['beacon_api.blocks'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.slashings',
            database: 'beacon_api',
            table: 'slashings',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM slashings',
            depends_on: ['beacon_api.attestations'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.aggregations',
            database: 'beacon_api',
            table: 'aggregations',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM aggregations',
            depends_on: ['beacon_api.attestations', 'beacon_api.proposers'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.rewards',
            database: 'beacon_api',
            table: 'rewards',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM rewards',
            depends_on: ['beacon_api.attestations', 'beacon_api.proposers'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 8,
      },
      coverage: {
        coverage: [
          { id: 'beacon_api.blocks', ranges: [{ position: 10000000, interval: 1000000 }] },
          { id: 'beacon_api.attestations', ranges: [{ position: 10050000, interval: 900000 }] },
          { id: 'beacon_api.proposers', ranges: [{ position: 10100000, interval: 850000 }] },
          { id: 'beacon_api.deposits', ranges: [{ position: 10000000, interval: 800000 }] },
          { id: 'beacon_api.withdrawals', ranges: [{ position: 10200000, interval: 700000 }] },
          { id: 'beacon_api.slashings', ranges: [{ position: 10300000, interval: 600000 }] },
          { id: 'beacon_api.aggregations', ranges: [{ position: 10150000, interval: 750000 }] },
          { id: 'beacon_api.rewards', ranges: [{ position: 10250000, interval: 650000 }] },
        ],
        total: 8,
      },
      externalModels: {
        models: [
          {
            id: 'beacon_api.validators',
            database: 'beacon_api',
            table: 'validators',
            interval: { type: 'slot_number' },
          },
          { id: 'beacon_api.config', database: 'beacon_api', table: 'config', interval: { type: 'slot_number' } },
        ],
        total: 2,
      },
      bounds: {
        bounds: [
          { id: 'beacon_api.validators', min: 9800000, max: 11000000 },
          { id: 'beacon_api.config', min: 9500000, max: 11200000 },
        ],
        total: 2,
      },
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const SparseCoverage: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: mockTransformations,
      coverage: {
        coverage: [
          {
            id: 'beacon_api.blocks',
            ranges: [
              { position: 10000000, interval: 100000 },
              { position: 10200000, interval: 100000 },
              { position: 10400000, interval: 100000 },
              { position: 10600000, interval: 100000 },
              { position: 10800000, interval: 100000 },
            ],
          },
          {
            id: 'beacon_api.attestations',
            ranges: [
              { position: 10050000, interval: 80000 },
              { position: 10250000, interval: 80000 },
              { position: 10650000, interval: 80000 },
            ],
          },
          {
            id: 'beacon_api.proposers',
            ranges: [
              { position: 10100000, interval: 50000 },
              { position: 10300000, interval: 50000 },
              { position: 10700000, interval: 50000 },
            ],
          },
        ],
        total: 3,
      },
      externalModels: mockExternalModels,
      bounds: mockBounds,
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const NoExternalModels: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: mockTransformations,
      coverage: mockCoverage,
      externalModels: { models: [], total: 0 },
      bounds: { bounds: [], total: 0 },
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const OnlyExternalModels: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: { models: [], total: 0 },
      coverage: { coverage: [], total: 0 },
      externalModels: {
        models: [
          {
            id: 'beacon_api.validators',
            database: 'beacon_api',
            table: 'validators',
            interval: { type: 'slot_number' },
          },
          { id: 'beacon_api.config', database: 'beacon_api', table: 'config', interval: { type: 'slot_number' } },
          { id: 'beacon_api.genesis', database: 'beacon_api', table: 'genesis', interval: { type: 'slot_number' } },
        ],
        total: 3,
      },
      bounds: {
        bounds: [
          { id: 'beacon_api.validators', min: 9800000, max: 11000000 },
          { id: 'beacon_api.config', min: 9500000, max: 11200000 },
          { id: 'beacon_api.genesis', min: 0, max: 11500000 },
        ],
        total: 3,
      },
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const ComplexDependencies: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: {
        models: [
          {
            id: 'beacon_api.level1',
            database: 'beacon_api',
            table: 'level1',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM level1',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.level2a',
            database: 'beacon_api',
            table: 'level2a',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM level2a',
            depends_on: ['beacon_api.level1'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.level2b',
            database: 'beacon_api',
            table: 'level2b',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM level2b',
            depends_on: ['beacon_api.level1'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.level3',
            database: 'beacon_api',
            table: 'level3',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM level3',
            depends_on: ['beacon_api.level2a', 'beacon_api.level2b'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 4,
      },
      coverage: {
        coverage: [
          { id: 'beacon_api.level1', ranges: [{ position: 10000000, interval: 1000000 }] },
          { id: 'beacon_api.level2a', ranges: [{ position: 10100000, interval: 800000 }] },
          { id: 'beacon_api.level2b', ranges: [{ position: 10050000, interval: 850000 }] },
          { id: 'beacon_api.level3', ranges: [{ position: 10200000, interval: 600000 }] },
        ],
        total: 4,
      },
      externalModels: { models: [], total: 0 },
      bounds: { bounds: [], total: 0 },
      intervalTypes: mockIntervalTypes,
    },
  },
};

export const WithMultipleTransformations: Story = {
  args: {
    zoomRanges: {},
    onZoomChange: fn(),
    onResetZoom: fn(),
  },
  parameters: {
    mockData: {
      transformations: mockTransformations,
      coverage: mockCoverage,
      externalModels: mockExternalModels,
      bounds: mockBounds,
      intervalTypes: {
        interval_types: {
          slot_number: [
            { name: 'Raw Slot', expression: 'value' },
            { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
            { name: 'Epoch', expression: 'value / 32' },
            { name: 'Day', expression: 'value / 7200' },
            { name: 'Hour', expression: 'value / 300' },
          ],
        },
      },
    },
    docs: {
      description: {
        story: 'Shows incremental models with multiple transformation options',
      },
    },
  },
};
