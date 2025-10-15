import type { Meta, StoryObj } from '@storybook/react-vite';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { CoverageDebugDialog } from '@/components/CoverageDebugDialog';
import { debugCoverageAtPositionQueryKey, getIntervalTypesQueryKey } from '@api/@tanstack/react-query.gen';
import type { CoverageDebug, GetIntervalTypesResponse } from '@api/types.gen';

const meta = {
  title: 'Components/CoverageDebugDialog',
  component: CoverageDebugDialog,
  parameters: {
    layout: 'fullscreen',
  },
  tags: ['autodocs'],
  argTypes: {
    isOpen: {
      control: 'boolean',
      description: 'Controls dialog visibility',
    },
    modelId: {
      control: 'text',
      description: 'Model ID to debug',
    },
    position: {
      control: 'number',
      description: 'Position to debug',
    },
  },
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
        mockDebugData?: CoverageDebug;
      };

      const storyArgs = context.args as React.ComponentProps<typeof CoverageDebugDialog>;

      // Set the mock data in query cache if provided
      if (storyParams.mockDebugData) {
        queryClient.setQueryData(
          debugCoverageAtPositionQueryKey({
            path: {
              id: storyArgs.modelId,
              position: storyArgs.position,
            },
          }),
          storyParams.mockDebugData
        );
      }

      // Set mock interval types data
      const mockIntervalTypes: GetIntervalTypesResponse = {
        interval_types: {
          slot: [
            {
              name: 'Slot',
            },
            {
              name: 'Date',
              format: 'datetime',
              expression: 'value * 12 + 1606824023',
            },
          ],
        },
      };

      queryClient.setQueryData(getIntervalTypesQueryKey(), mockIntervalTypes);

      return (
        <QueryClientProvider client={queryClient}>
          <div className="min-h-screen bg-slate-950 p-8">
            <CoverageDebugDialog {...storyArgs} />
          </div>
        </QueryClientProvider>
      );
    },
  ],
} satisfies Meta<typeof CoverageDebugDialog>;

export default meta;
type Story = StoryObj<typeof CoverageDebugDialog>;

// Mock debug data for stories
const mockDebugDataCannotProcess: CoverageDebug = {
  model_id: 'analytics.entity_network_effects_or_test',
  position: 1760421480,
  interval: 60,
  end_position: 1760421540,
  can_process: false,
  model_coverage: {
    has_data: true,
    first_position: 1760399556,
    last_end_position: 1760415038,
    ranges_in_window: [
      { position: 1760421300, interval: 60 },
      { position: 1760421360, interval: 60 },
    ],
    gaps_in_window: [
      { start: 1760421420, end: 1760421480, size: 60, overlaps_request: true },
      { start: 1760421540, end: 1760421600, size: 60, overlaps_request: false },
    ],
  },
  dependencies: [
    {
      id: 'analytics.block_entity',
      type: 'required',
      node_type: 'transformation',
      is_incremental: true,
      bounds: {
        has_data: true,
        min: 1760414066,
        max: 1760421746,
      },
      gaps: [{ start: 1760421480, end: 1760421540, size: 60, overlaps_request: true }],
      coverage_status: 'has_gaps',
      blocking: true,
      child_dependencies: [
        {
          id: 'ethereum.beacon_blocks',
          type: 'required',
          node_type: 'external',
          bounds: {
            has_data: true,
            min: 1760406816,
            max: 1760421758,
            lag_applied: 10,
          },
          coverage_status: 'full_coverage',
          blocking: false,
        },
      ],
    },
    {
      id: 'analytics.block_propagation',
      type: 'required',
      node_type: 'transformation',
      is_incremental: true,
      bounds: {
        has_data: true,
        min: 1760406816,
        max: 1760421746,
      },
      coverage_status: 'full_coverage',
      blocking: false,
    },
  ],
  validation: {
    in_bounds: false,
    has_dependency_gaps: true,
    valid_range: {
      min: 1760406816,
      max: 1760421746,
    },
    blocking_gaps: [
      {
        dependency_id: 'analytics.block_entity',
        gap: {
          start: 1760421480,
          end: 1760421540,
          size: 60,
        },
      },
    ],
    next_valid_position: 1760421540,
    reasons: [
      'Dependency analytics.block_entity has gap from 1760421480 to 1760421540',
      'Position is beyond current forward fill boundary',
    ],
  },
};

const mockDebugDataCanProcess: CoverageDebug = {
  model_id: 'analytics.successful_model',
  position: 1760400000,
  interval: 60,
  end_position: 1760400060,
  can_process: true,
  model_coverage: {
    has_data: true,
    first_position: 1760399556,
    last_end_position: 1760415038,
    ranges_in_window: [{ position: 1760399900, interval: 200 }],
    gaps_in_window: [],
  },
  dependencies: [
    {
      id: 'analytics.source_model',
      type: 'required',
      node_type: 'transformation',
      is_incremental: true,
      bounds: {
        has_data: true,
        min: 1760390000,
        max: 1760420000,
      },
      gaps: [],
      coverage_status: 'full_coverage',
      blocking: false,
    },
  ],
  validation: {
    in_bounds: true,
    has_dependency_gaps: false,
    valid_range: {
      min: 1760390000,
      max: 1760420000,
    },
    reasons: [],
  },
};

const mockDebugDataWithOrGroups: CoverageDebug = {
  model_id: 'analytics.or_group_model',
  position: 1760410000,
  interval: 60,
  end_position: 1760410060,
  can_process: true,
  model_coverage: {
    has_data: true,
    first_position: 1760399556,
    last_end_position: 1760415038,
  },
  dependencies: [
    {
      id: 'or_group',
      type: 'or_group',
      node_type: 'transformation',
      coverage_status: 'has_gaps',
      blocking: false,
      bounds: {
        has_data: true,
        min: 1760400000,
        max: 1760420000,
      },
      or_group_members: [
        {
          id: 'source1.data',
          type: 'required',
          node_type: 'external',
          bounds: {
            has_data: false,
            min: 0,
            max: 0,
          },
          coverage_status: 'not_initialized',
          blocking: true,
        },
        {
          id: 'source2.data',
          type: 'required',
          node_type: 'external',
          bounds: {
            has_data: true,
            min: 1760400000,
            max: 1760420000,
          },
          coverage_status: 'full_coverage',
          blocking: false,
        },
      ],
    },
  ],
  validation: {
    in_bounds: true,
    has_dependency_gaps: false,
    valid_range: {
      min: 1760400000,
      max: 1760420000,
    },
    reasons: [],
  },
};

const mockDebugDataNoData: CoverageDebug = {
  model_id: 'analytics.empty_model',
  position: 1760300000,
  interval: 60,
  end_position: 1760300060,
  can_process: false,
  model_coverage: {
    has_data: false,
    first_position: 0,
    last_end_position: 0,
  },
  dependencies: [],
  validation: {
    in_bounds: false,
    has_dependency_gaps: false,
    reasons: ['Model has no processed data'],
  },
};

export const CannotProcess: Story = {
  args: {
    isOpen: true,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.entity_network_effects_or_test',
    position: 1760421480,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataCannotProcess,
  },
};

export const CanProcess: Story = {
  args: {
    isOpen: true,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.successful_model',
    position: 1760400000,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataCanProcess,
  },
};

export const WithOrGroups: Story = {
  args: {
    isOpen: true,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.or_group_model',
    position: 1760410000,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataWithOrGroups,
  },
};

export const NoData: Story = {
  args: {
    isOpen: true,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.empty_model',
    position: 1760300000,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataNoData,
  },
};

export const Closed: Story = {
  args: {
    isOpen: false,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.test_model',
    position: 1760400000,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataCanProcess,
  },
};

export const MobileView: Story = {
  args: {
    isOpen: true,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.entity_network_effects_or_test',
    position: 1760421480,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataCannotProcess,
    viewport: {
      defaultViewport: 'mobile1',
    },
  },
};

const mockDebugDataManyRanges: CoverageDebug = {
  model_id: 'analytics.many_ranges_model',
  position: 1760400000,
  interval: 60,
  end_position: 1760400060,
  can_process: true,
  model_coverage: {
    has_data: true,
    first_position: 1760399556,
    last_end_position: 1760415038,
    ranges_in_window: [
      { position: 1760399900, interval: 60 },
      { position: 1760399960, interval: 60 },
      { position: 1760400020, interval: 60 },
      { position: 1760400080, interval: 60 },
      { position: 1760400140, interval: 60 },
      { position: 1760400200, interval: 60 },
      { position: 1760400260, interval: 60 },
      { position: 1760400320, interval: 60 },
      { position: 1760400380, interval: 60 },
      { position: 1760400440, interval: 60 },
    ],
    gaps_in_window: [],
  },
  dependencies: [
    {
      id: 'analytics.source_model',
      type: 'required',
      node_type: 'transformation',
      is_incremental: true,
      bounds: {
        has_data: true,
        min: 1760390000,
        max: 1760420000,
      },
      gaps: [],
      coverage_status: 'full_coverage',
      blocking: false,
    },
  ],
  validation: {
    in_bounds: true,
    has_dependency_gaps: false,
    valid_range: {
      min: 1760390000,
      max: 1760420000,
    },
    reasons: [],
  },
};

export const ManyRanges: Story = {
  args: {
    isOpen: true,
    onClose: () => console.log('Close dialog'),
    modelId: 'analytics.many_ranges_model',
    position: 1760400000,
    intervalType: 'slot',
  },
  parameters: {
    mockDebugData: mockDebugDataManyRanges,
  },
};
