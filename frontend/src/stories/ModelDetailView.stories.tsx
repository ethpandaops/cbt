import type { Meta, StoryObj } from '@storybook/react-vite';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { UseQueryResult } from '@tanstack/react-query';
import { ModelDetailView } from '@/components/ModelDetailView';
import { debugCoverageAtPositionQueryKey } from '@api/@tanstack/react-query.gen';
import type {
  TransformationModel,
  ListTransformationCoverageResponse,
  ListExternalBoundsResponse,
  ListTransformationsResponse,
  GetIntervalTypesResponse,
  CoverageDebug,
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
          coverageDebug?: Record<string, CoverageDebug>; // Position -> Debug data
        };
      };

      const mockData = storyParams.mockData || {};

      // Set mock coverage debug data if provided
      if (mockData.coverageDebug) {
        Object.entries(mockData.coverageDebug).forEach(([positionKey, debugData]) => {
          const [modelId, position] = positionKey.split(':');
          queryClient.setQueryData(
            debugCoverageAtPositionQueryKey({
              path: {
                id: modelId,
                position: parseInt(position, 10),
              },
            }),
            debugData
          );
        });
      }

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
      coverageDebug: {
        // Mock debug data for clicking on position 10250000 (in a gap)
        'beacon_api.slot_coverage:10250000': {
          model_id: 'beacon_api.slot_coverage',
          position: 10250000,
          interval: 50000,
          end_position: 10300000,
          can_process: false,
          model_coverage: {
            has_data: true,
            first_position: 10000000,
            last_end_position: 10650000,
            ranges_in_window: [],
            gaps_in_window: [{ start: 10250000, end: 10400000, size: 150000, overlaps_request: true }],
          },
          dependencies: [
            {
              id: 'beacon_api.blocks',
              type: 'required',
              node_type: 'transformation',
              is_incremental: true,
              bounds: {
                has_data: true,
                min: 9950000,
                max: 10550000,
              },
              gaps: [{ start: 10250000, end: 10450000, size: 200000, overlaps_request: true }],
              coverage_status: 'has_gaps',
              blocking: true,
            },
          ],
          validation: {
            in_bounds: true,
            has_dependency_gaps: true,
            valid_range: {
              min: 9950000,
              max: 10550000,
            },
            blocking_gaps: [
              {
                dependency_id: 'beacon_api.blocks',
                gap: {
                  start: 10250000,
                  end: 10450000,
                  size: 200000,
                },
              },
            ],
            next_valid_position: 10450000,
            reasons: [
              'Dependency beacon_api.blocks has gap from 10250000 to 10450000',
              'Model has no coverage for position 10250000',
            ],
          },
        },
        // Mock debug data for clicking on position 10100000 (covered area)
        'beacon_api.slot_coverage:10100000': {
          model_id: 'beacon_api.slot_coverage',
          position: 10100000,
          interval: 50000,
          end_position: 10150000,
          can_process: true,
          model_coverage: {
            has_data: true,
            first_position: 10000000,
            last_end_position: 10650000,
            ranges_in_window: [{ position: 10100000, interval: 50000 }],
          },
          dependencies: [
            {
              id: 'beacon_api.blocks',
              type: 'required',
              node_type: 'transformation',
              is_incremental: true,
              bounds: {
                has_data: true,
                min: 9950000,
                max: 10550000,
              },
              coverage_status: 'full_coverage',
              blocking: false,
            },
          ],
          validation: {
            in_bounds: true,
            has_dependency_gaps: false,
            valid_range: {
              min: 9950000,
              max: 10550000,
            },
            reasons: [],
          },
        },
      },
    },
    docs: {
      description: {
        story:
          'Model with sparse coverage showing multiple gaps. Click on coverage bars to see debug information - try clicking on gaps vs covered areas!',
      },
    },
  },
};

export const OrGroupDependencies: Story = {
  args: {
    decodedId: 'beacon_api.consumer',
    transformation: {
      id: 'beacon_api.consumer',
      database: 'beacon_api',
      table: 'consumer',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM consumer WHERE slot >= :min_interval AND slot < :max_interval',
      description: 'Consumer with OR group dependencies',
      depends_on: [['beacon_api.source_a', 'beacon_api.source_b']],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.consumer',
            ranges: [{ position: 10200000, interval: 700000 }],
          },
          {
            id: 'beacon_api.source_a',
            ranges: [{ position: 10000000, interval: 900000 }],
          },
          {
            id: 'beacon_api.source_b',
            ranges: [{ position: 10100000, interval: 800000 }],
          },
        ],
        total: 3,
      },
      bounds: mockBoundsData,
      transformations: {
        models: [
          {
            id: 'beacon_api.consumer',
            database: 'beacon_api',
            table: 'consumer',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM consumer',
            depends_on: [['beacon_api.source_a', 'beacon_api.source_b']],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_a',
            database: 'beacon_api',
            table: 'source_a',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_a',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_b',
            database: 'beacon_api',
            table: 'source_b',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_b',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 3,
      },
      intervalTypes: mockIntervalTypesData,
    },
    docs: {
      description: {
        story:
          'Model with OR group dependencies: requires either source_a OR source_b. Both dependencies show OR #1 badge.',
      },
    },
  },
};

export const OrGroupMixed: Story = {
  args: {
    decodedId: 'beacon_api.aggregator',
    transformation: {
      id: 'beacon_api.aggregator',
      database: 'beacon_api',
      table: 'aggregator',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM aggregator WHERE slot >= :min_interval AND slot < :max_interval',
      description: 'Aggregator with mixed AND/OR dependencies',
      depends_on: ['beacon_api.blocks', ['beacon_api.attestations_v1', 'beacon_api.attestations_v2']],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.aggregator',
            ranges: [{ position: 10300000, interval: 600000 }],
          },
          {
            id: 'beacon_api.blocks',
            ranges: [{ position: 10000000, interval: 1000000 }],
          },
          {
            id: 'beacon_api.attestations_v1',
            ranges: [{ position: 10100000, interval: 800000 }],
          },
          {
            id: 'beacon_api.attestations_v2',
            ranges: [{ position: 10150000, interval: 750000 }],
          },
        ],
        total: 4,
      },
      bounds: mockBoundsData,
      transformations: {
        models: [
          {
            id: 'beacon_api.aggregator',
            database: 'beacon_api',
            table: 'aggregator',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM aggregator',
            depends_on: ['beacon_api.blocks', ['beacon_api.attestations_v1', 'beacon_api.attestations_v2']],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
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
            id: 'beacon_api.attestations_v1',
            database: 'beacon_api',
            table: 'attestations_v1',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM attestations_v1',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.attestations_v2',
            database: 'beacon_api',
            table: 'attestations_v2',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM attestations_v2',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 4,
      },
      intervalTypes: mockIntervalTypesData,
    },
    docs: {
      description: {
        story:
          'Mixed AND/OR dependencies: requires blocks (no badge) AND (attestations_v1 OR attestations_v2 with OR #1 badges). Hover over OR badges to see related dependencies.',
      },
    },
  },
};

export const OrGroupMultiple: Story = {
  args: {
    decodedId: 'beacon_api.multi_consumer',
    transformation: {
      id: 'beacon_api.multi_consumer',
      database: 'beacon_api',
      table: 'multi_consumer',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM multi_consumer WHERE slot >= :min_interval AND slot < :max_interval',
      description: 'Consumer with multiple OR groups',
      depends_on: [
        ['beacon_api.source_a1', 'beacon_api.source_a2'],
        ['beacon_api.source_b1', 'beacon_api.source_b2'],
      ],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.multi_consumer',
            ranges: [{ position: 10400000, interval: 500000 }],
          },
          {
            id: 'beacon_api.source_a1',
            ranges: [{ position: 10000000, interval: 900000 }],
          },
          {
            id: 'beacon_api.source_a2',
            ranges: [{ position: 10050000, interval: 850000 }],
          },
          {
            id: 'beacon_api.source_b1',
            ranges: [{ position: 10100000, interval: 800000 }],
          },
          {
            id: 'beacon_api.source_b2',
            ranges: [{ position: 10150000, interval: 750000 }],
          },
        ],
        total: 5,
      },
      bounds: mockBoundsData,
      transformations: {
        models: [
          {
            id: 'beacon_api.multi_consumer',
            database: 'beacon_api',
            table: 'multi_consumer',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM multi_consumer',
            depends_on: [
              ['beacon_api.source_a1', 'beacon_api.source_a2'],
              ['beacon_api.source_b1', 'beacon_api.source_b2'],
            ],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_a1',
            database: 'beacon_api',
            table: 'source_a1',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_a1',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_a2',
            database: 'beacon_api',
            table: 'source_a2',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_a2',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_b1',
            database: 'beacon_api',
            table: 'source_b1',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_b1',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_b2',
            database: 'beacon_api',
            table: 'source_b2',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_b2',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 5,
      },
      intervalTypes: mockIntervalTypesData,
    },
    docs: {
      description: {
        story:
          'Multiple OR groups: requires (source_a1 OR source_a2) AND (source_b1 OR source_b2). OR #1 badges on a-sources, OR #2 badges on b-sources. Hover to highlight related dependencies.',
      },
    },
  },
};

export const OrGroupTransitive: Story = {
  args: {
    decodedId: 'beacon_api.final',
    transformation: {
      id: 'beacon_api.final',
      database: 'beacon_api',
      table: 'final',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM final WHERE slot >= :min_interval AND slot < :max_interval',
      description: 'Final model with transitive OR dependencies',
      depends_on: ['beacon_api.middle'],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.final',
            ranges: [{ position: 10400000, interval: 500000 }],
          },
          {
            id: 'beacon_api.middle',
            ranges: [{ position: 10200000, interval: 700000 }],
          },
          {
            id: 'beacon_api.source_x',
            ranges: [{ position: 10000000, interval: 900000 }],
          },
          {
            id: 'beacon_api.source_y',
            ranges: [{ position: 10100000, interval: 800000 }],
          },
        ],
        total: 4,
      },
      bounds: mockBoundsData,
      transformations: {
        models: [
          {
            id: 'beacon_api.final',
            database: 'beacon_api',
            table: 'final',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM final',
            depends_on: ['beacon_api.middle'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.middle',
            database: 'beacon_api',
            table: 'middle',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM middle',
            depends_on: [['beacon_api.source_x', 'beacon_api.source_y']],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_x',
            database: 'beacon_api',
            table: 'source_x',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_x',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.source_y',
            database: 'beacon_api',
            table: 'source_y',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM source_y',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 4,
      },
      intervalTypes: mockIntervalTypesData,
    },
    docs: {
      description: {
        story:
          'Transitive OR dependencies: final depends on middle (no badge), but middle has OR dependencies on (source_x OR source_y with OR #1 badges). Tests recursive OR group resolution.',
      },
    },
  },
};

export const OrGroupSharedDependency: Story = {
  args: {
    decodedId: 'beacon_api.model_a',
    transformation: {
      id: 'beacon_api.model_a',
      database: 'beacon_api',
      table: 'model_a',
      type: 'incremental',
      content_type: 'sql',
      content: 'SELECT * FROM model_a WHERE slot >= :min_interval AND slot < :max_interval',
      description: 'Model A with Model D appearing in multiple OR groups',
      depends_on: [['beacon_api.model_b', 'beacon_api.model_c'], 'beacon_api.model_d'],
      interval: {
        type: 'slot_number',
        min: 1,
        max: 7200,
      },
    },
  },
  parameters: {
    mockData: {
      coverage: {
        coverage: [
          {
            id: 'beacon_api.model_a',
            ranges: [{ position: 10400000, interval: 500000 }],
          },
          {
            id: 'beacon_api.model_b',
            ranges: [{ position: 10200000, interval: 700000 }],
          },
          {
            id: 'beacon_api.model_c',
            ranges: [{ position: 10250000, interval: 650000 }],
          },
          {
            id: 'beacon_api.model_d',
            ranges: [{ position: 10000000, interval: 900000 }],
          },
          {
            id: 'beacon_api.model_e',
            ranges: [{ position: 10050000, interval: 850000 }],
          },
        ],
        total: 5,
      },
      bounds: mockBoundsData,
      transformations: {
        models: [
          {
            id: 'beacon_api.model_a',
            database: 'beacon_api',
            table: 'model_a',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM model_a',
            depends_on: [['beacon_api.model_b', 'beacon_api.model_c'], 'beacon_api.model_d'],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.model_b',
            database: 'beacon_api',
            table: 'model_b',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM model_b',
            depends_on: ['beacon_api.model_d', ['beacon_api.model_c', 'beacon_api.model_e']],
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.model_c',
            database: 'beacon_api',
            table: 'model_c',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM model_c',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.model_d',
            database: 'beacon_api',
            table: 'model_d',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM model_d',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
          {
            id: 'beacon_api.model_e',
            database: 'beacon_api',
            table: 'model_e',
            type: 'incremental',
            content_type: 'sql',
            content: 'SELECT * FROM model_e',
            interval: { type: 'slot_number', min: 1, max: 7200 },
          },
        ],
        total: 5,
      },
      intervalTypes: mockIntervalTypesData,
    },
    docs: {
      description: {
        story:
          "Model with dependencies appearing in multiple OR groups: Model A depends on [(B OR C) AND D]. Model B depends on [D AND (C OR E)]. This means Model D appears TWICE (once as direct dep with no badge, once transitively through Model B with OR #2 badge from B's OR group). Model C appears TWICE (once with OR #1 badge from A's OR group, once with OR #2 badge from B's OR group). Look for Model C and Model D showing multiple OR badges on the same row.",
      },
    },
  },
};
