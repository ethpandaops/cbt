import type { Meta, StoryObj } from '@storybook/react-vite';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { UseQueryResult } from '@tanstack/react-query';
import { ModelDetailView } from '@/components/ModelDetailView';
import type {
  TransformationModel,
  ListTransformationCoverageResponse,
  ListExternalBoundsResponse,
  ListTransformationsResponse,
  GetIntervalTypesResponse,
} from '@api/types.gen';

// Story args type that omits the query result props
type StoryArgs = Omit<
  React.ComponentProps<typeof ModelDetailView>,
  'coverage' | 'allBounds' | 'allTransformations' | 'intervalTypes'
>;

const meta = {
  title: 'Components/ModelDetailView',
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  decorators: [
    (_Story, context) => {
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: {
            retry: false,
            staleTime: Infinity,
          },
        },
      });

      // Get mock data from story parameters
      const storyParams = context.parameters as {
        mockData?: {
          coverage?: ListTransformationCoverageResponse;
          bounds?: ListExternalBoundsResponse;
          transformations?: ListTransformationsResponse;
          intervalTypes?: GetIntervalTypesResponse;
        };
      };

      const mockData = storyParams.mockData || {};

      // Simple mock that satisfies UseQueryResult interface
      const asMockQuery = <T,>(data: T | undefined): UseQueryResult<T, Error> =>
        ({
          data,
          isSuccess: true,
          isError: false,
          error: null,
        }) as UseQueryResult<T, Error>;

      const storyArgs = context.args as StoryArgs;

      return (
        <QueryClientProvider client={queryClient}>
          <div className="bg-slate-950 p-8">
            <ModelDetailView
              {...storyArgs}
              coverage={asMockQuery(mockData.coverage)}
              allBounds={asMockQuery(mockData.bounds)}
              allTransformations={asMockQuery(mockData.transformations)}
              intervalTypes={asMockQuery(mockData.intervalTypes)}
            />
          </div>
        </QueryClientProvider>
      );
    },
  ],
} satisfies Meta<StoryArgs>;

export default meta;
type Story = StoryObj<StoryArgs>;

const mockTransformation: TransformationModel = {
  id: 'beacon_api.slot_coverage',
  database: 'beacon_api',
  table: 'slot_coverage',
  type: 'incremental',
  content_type: 'sql',
  content: `SELECT
  slot_number,
  block_hash,
  proposer_index
FROM beacon_blocks
WHERE slot_number >= :min_interval
  AND slot_number < :max_interval
ORDER BY slot_number`,
  description: 'Coverage tracking for beacon chain slots',
  tags: ['beacon', 'coverage', 'slots'],
  interval: {
    type: 'slot_number',
    min: 1,
    max: 7200,
  },
  depends_on: ['beacon_api.blocks', 'beacon_api.validators'],
};

const mockCoverageData: ListTransformationCoverageResponse = {
  coverage: [
    {
      id: 'beacon_api.slot_coverage',
      ranges: [
        { position: 10000000, interval: 500000 },
        { position: 10600000, interval: 300000 },
      ],
    },
    {
      id: 'beacon_api.blocks',
      ranges: [
        { position: 9900000, interval: 800000 },
        { position: 10800000, interval: 200000 },
      ],
    },
    {
      id: 'beacon_api.validators',
      ranges: [{ position: 9800000, interval: 1200000 }],
    },
  ],
  total: 3,
};

const mockBoundsData: ListExternalBoundsResponse = {
  bounds: [],
  total: 0,
};

const mockTransformationsData: ListTransformationsResponse = {
  models: [
    mockTransformation,
    {
      id: 'beacon_api.blocks',
      database: 'beacon_api',
      table: 'blocks',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM blocks',
      depends_on: [],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
    {
      id: 'beacon_api.validators',
      database: 'beacon_api',
      table: 'validators',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM validators',
      depends_on: [],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  ],
  total: 3,
};

const mockIntervalTypesData: GetIntervalTypesResponse = {
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
    decodedId: 'beacon_api.slot_coverage',
    transformation: mockTransformation,
  },
  parameters: {
    mockData: {
      coverage: mockCoverageData,
      bounds: mockBoundsData,
      transformations: mockTransformationsData,
      intervalTypes: mockIntervalTypesData,
    },
  },
};

export const WithSchedules: Story = {
  args: {
    decodedId: 'beacon_api.slot_coverage',
    transformation: {
      ...mockTransformation,
      schedules: {
        forwardfill: '*/5 * * * *',
        backfill: '0 2 * * *',
      },
    },
  },
  parameters: {
    mockData: {
      coverage: mockCoverageData,
      bounds: mockBoundsData,
      transformations: mockTransformationsData,
      intervalTypes: mockIntervalTypesData,
    },
  },
};

export const WithExternalDependency: Story = {
  args: {
    decodedId: 'beacon_api.slot_coverage',
    transformation: {
      ...mockTransformation,
      depends_on: ['beacon_api.blocks', 'beacon_api.external_source'],
    },
  },
  parameters: {
    mockData: {
      coverage: mockCoverageData,
      bounds: {
        bounds: [
          {
            id: 'beacon_api.external_source',
            min: 9500000,
            max: 11000000,
          },
        ],
        total: 1,
      },
      transformations: {
        models: [
          ...mockTransformationsData.models,
          {
            id: 'beacon_api.external_source',
            database: 'beacon_api',
            table: 'external_source',
            type: 'external',
            content_type: 'sql',
            content: '',
            interval: {
              type: 'slot_number',
              min: 1,
              max: 7200,
            },
          },
        ],
        total: 4,
      },
      intervalTypes: mockIntervalTypesData,
    },
  },
};

export const LongSQL: Story = {
  args: {
    decodedId: 'beacon_api.complex_analysis',
    transformation: {
      id: 'beacon_api.complex_analysis',
      database: 'beacon_api',
      table: 'complex_analysis',
      type: 'incremental',
      content_type: 'sql',
      content: `WITH slot_range AS (
  SELECT generate_series(:min_interval, :max_interval) AS slot_number
),
blocks_with_gaps AS (
  SELECT
    sr.slot_number,
    bb.block_hash,
    bb.proposer_index,
    bb.timestamp,
    CASE WHEN bb.slot_number IS NULL THEN 1 ELSE 0 END as is_missed
  FROM slot_range sr
  LEFT JOIN beacon_blocks bb ON sr.slot_number = bb.slot_number
),
gap_groups AS (
  SELECT
    slot_number,
    block_hash,
    proposer_index,
    timestamp,
    is_missed,
    SUM(is_missed) OVER (ORDER BY slot_number) as gap_group
  FROM blocks_with_gaps
),
coverage_ranges AS (
  SELECT
    MIN(slot_number) as range_start,
    MAX(slot_number) as range_end,
    COUNT(*) as slot_count,
    COUNT(block_hash) as block_count
  FROM gap_groups
  WHERE is_missed = 0
  GROUP BY gap_group
  HAVING COUNT(block_hash) > 0
)
SELECT * FROM coverage_ranges ORDER BY range_start`,
      description: 'Complex coverage analysis with CTEs',
      tags: ['beacon', 'coverage', 'analysis', 'complex'],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
      depends_on: ['beacon_api.blocks'],
    },
  },
  parameters: {
    mockData: {
      coverage: mockCoverageData,
      bounds: mockBoundsData,
      transformations: mockTransformationsData,
      intervalTypes: mockIntervalTypesData,
    },
  },
};

export const ExecContentType: Story = {
  args: {
    decodedId: 'beacon_api.custom_script',
    transformation: {
      id: 'beacon_api.custom_script',
      database: 'beacon_api',
      table: 'custom_script',
      type: 'incremental',
      content_type: 'exec',
      content: './scripts/process_beacon_data.sh --min ${MIN_INTERVAL} --max ${MAX_INTERVAL}',
      description: 'Custom shell script for data processing',
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
      depends_on: ['beacon_api.blocks'],
    },
  },
  parameters: {
    mockData: {
      coverage: mockCoverageData,
      bounds: mockBoundsData,
      transformations: mockTransformationsData,
      intervalTypes: mockIntervalTypesData,
    },
  },
};

export const NoDependencies: Story = {
  args: {
    decodedId: 'beacon_api.standalone',
    transformation: {
      id: 'beacon_api.standalone',
      database: 'beacon_api',
      table: 'standalone',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM external_source',
      description: 'Standalone transformation with no dependencies',
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
      depends_on: [],
    },
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.standalone',
            ranges: [{ position: 10000000, interval: 1000000 }],
          },
        ],
        total: 1,
      },
      bounds: mockBoundsData,
      transformations: {
        models: [
          {
            id: 'beacon_api.standalone',
            database: 'beacon_api',
            table: 'standalone',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM external_source',
            depends_on: [],
            interval: {
              type: 'slot_number',
              min: 1,
              max: 7200,
            },
          },
        ],
        total: 1,
      },
      intervalTypes: mockIntervalTypesData,
    },
  },
};

export const MultipleTransformations: Story = {
  args: {
    decodedId: 'beacon_api.slot_coverage',
    transformation: mockTransformation,
  },
  parameters: {
    mockData: {
      coverage: mockCoverageData,
      bounds: mockBoundsData,
      transformations: mockTransformationsData,
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
        story: 'Model detail view with multiple transformation options for the interval type',
      },
    },
  },
};

export const SparseCoverage: Story = {
  args: {
    decodedId: 'beacon_api.slot_coverage',
    transformation: mockTransformation,
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.slot_coverage',
            ranges: [
              { position: 10000000, interval: 50000 },
              { position: 10100000, interval: 50000 },
              { position: 10200000, interval: 50000 },
              { position: 10400000, interval: 50000 },
              { position: 10600000, interval: 50000 },
            ],
          },
          {
            id: 'beacon_api.blocks',
            ranges: [
              { position: 9950000, interval: 100000 },
              { position: 10150000, interval: 100000 },
              { position: 10450000, interval: 100000 },
            ],
          },
        ],
        total: 2,
      },
      bounds: mockBoundsData,
      transformations: mockTransformationsData,
      intervalTypes: mockIntervalTypesData,
    },
    docs: {
      description: {
        story: 'Model with sparse coverage showing multiple gaps',
      },
    },
  },
};
