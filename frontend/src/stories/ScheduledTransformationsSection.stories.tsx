import type { Meta, StoryObj } from '@storybook/react-vite';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ScheduledTransformationsSection } from '@/components/ScheduledTransformationsSection';
import { listTransformationsQueryKey, listScheduledRunsQueryKey } from '@api/@tanstack/react-query.gen';
import type { ListTransformationsResponse, ListScheduledRunsResponse } from '@api/types.gen';

const meta = {
  title: 'Components/ScheduledTransformationsSection',
  component: ScheduledTransformationsSection,
  parameters: {
    layout: 'padded',
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
          transformations?: ListTransformationsResponse;
          runs?: ListScheduledRunsResponse;
        };
      };

      const mockData = storyParams.mockData || {};
      const transformationsData = mockData.transformations;
      const runsData = mockData.runs;

      // Pre-populate the QueryClient cache using the actual query key generators
      if (transformationsData) {
        queryClient.setQueryData(listTransformationsQueryKey(), transformationsData);
      }
      if (runsData) {
        queryClient.setQueryData(listScheduledRunsQueryKey(), runsData);
      }

      return (
        <QueryClientProvider client={queryClient}>
          <div className="bg-slate-950 p-8">
            <ScheduledTransformationsSection />
          </div>
        </QueryClientProvider>
      );
    },
  ],
} satisfies Meta<typeof ScheduledTransformationsSection>;

export default meta;

const mockTransformations: ListTransformationsResponse = {
  models: [
    {
      id: 'beacon_api.daily_summary',
      database: 'beacon_api',
      table: 'daily_summary',
      type: 'scheduled',
      content_type: 'sql',
      content: 'SELECT * FROM beacon_blocks WHERE date = CURRENT_DATE',
      schedule: '0 0 * * *', // Daily at midnight
    },
    {
      id: 'beacon_api.weekly_report',
      database: 'beacon_api',
      table: 'weekly_report',
      type: 'scheduled',
      content_type: 'sql',
      content: 'SELECT * FROM beacon_blocks WHERE week = CURRENT_WEEK',
      schedule: '0 0 * * 1', // Weekly on Monday at midnight
    },
    {
      id: 'beacon_api.monthly_stats',
      database: 'beacon_api',
      table: 'monthly_stats',
      type: 'scheduled',
      content_type: 'sql',
      content: 'SELECT * FROM beacon_blocks WHERE month = CURRENT_MONTH',
      schedule: '0 0 1 * *', // Monthly on 1st at midnight
    },
  ],
  total: 3,
};

const mockRuns: ListScheduledRunsResponse = {
  runs: [
    {
      id: 'beacon_api.daily_summary',
      last_run: '2024-01-15T10:30:00Z',
    },
    {
      id: 'beacon_api.weekly_report',
      last_run: '2024-01-14T08:00:00Z',
    },
    {
      id: 'beacon_api.monthly_stats',
      last_run: '2024-01-01T00:00:00Z',
    },
  ],
  total: 3,
};

export const Default: StoryObj<typeof meta> = {
  parameters: {
    mockData: {
      transformations: mockTransformations,
      runs: mockRuns,
    },
  },
};

export const ManyModels: StoryObj<typeof meta> = {
  parameters: {
    mockData: {
      transformations: {
        models: [
          {
            id: 'beacon_api.hourly_summary',
            database: 'beacon_api',
            table: 'hourly_summary',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE hour = CURRENT_HOUR',
            schedule: '0 * * * *', // Every hour
          },
          {
            id: 'beacon_api.daily_summary',
            database: 'beacon_api',
            table: 'daily_summary',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE date = CURRENT_DATE',
            schedule: '0 0 * * *', // Daily at midnight
          },
          {
            id: 'beacon_api.weekly_report',
            database: 'beacon_api',
            table: 'weekly_report',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE week = CURRENT_WEEK',
            schedule: '0 0 * * 1', // Weekly on Monday
          },
          {
            id: 'beacon_api.monthly_stats',
            database: 'beacon_api',
            table: 'monthly_stats',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE month = CURRENT_MONTH',
            schedule: '0 0 1 * *', // Monthly on 1st
          },
          {
            id: 'beacon_api.quarterly_analysis',
            database: 'beacon_api',
            table: 'quarterly_analysis',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE quarter = CURRENT_QUARTER',
            schedule: '0 0 1 */3 *', // Quarterly
          },
          {
            id: 'beacon_api.yearly_metrics',
            database: 'beacon_api',
            table: 'yearly_metrics',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE year = CURRENT_YEAR',
            schedule: '0 0 1 1 *', // Yearly on Jan 1st
          },
          {
            id: 'beacon_api.realtime_stats',
            database: 'beacon_api',
            table: 'realtime_stats',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE timestamp > NOW() - INTERVAL 5 MINUTE',
            schedule: '*/5 * * * *', // Every 5 minutes
          },
          {
            id: 'beacon_api.validator_performance',
            database: 'beacon_api',
            table: 'validator_performance',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM validators WHERE active = true',
            schedule: '@every 15m', // Every 15 minutes
          },
        ],
        total: 8,
      },
      runs: {
        runs: [
          {
            id: 'beacon_api.hourly_summary',
            last_run: '2024-01-15T14:00:00Z',
          },
          {
            id: 'beacon_api.daily_summary',
            last_run: '2024-01-15T10:30:00Z',
          },
          {
            id: 'beacon_api.weekly_report',
            last_run: '2024-01-14T08:00:00Z',
          },
          {
            id: 'beacon_api.monthly_stats',
            last_run: '2024-01-01T00:00:00Z',
          },
          {
            id: 'beacon_api.quarterly_analysis',
            last_run: '2024-01-01T00:00:00Z',
          },
          {
            id: 'beacon_api.yearly_metrics',
            last_run: '2024-01-01T00:00:00Z',
          },
          {
            id: 'beacon_api.realtime_stats',
            last_run: '2024-01-15T14:55:00Z',
          },
          {
            id: 'beacon_api.validator_performance',
            last_run: '2024-01-15T12:00:00Z',
          },
        ],
        total: 8,
      },
    },
  },
};

export const SingleModel: StoryObj<typeof meta> = {
  parameters: {
    mockData: {
      transformations: {
        models: [
          {
            id: 'beacon_api.daily_summary',
            database: 'beacon_api',
            table: 'daily_summary',
            type: 'scheduled',
            content_type: 'sql',
            content: 'SELECT * FROM beacon_blocks WHERE date = CURRENT_DATE',
            schedule: '0 0 * * *', // Daily at midnight
          },
        ],
        total: 1,
      },
      runs: {
        runs: [
          {
            id: 'beacon_api.daily_summary',
            last_run: '2024-01-15T10:30:00Z',
          },
        ],
        total: 1,
      },
    },
  },
};

export const NoRuns: StoryObj<typeof meta> = {
  parameters: {
    mockData: {
      transformations: mockTransformations,
      runs: {
        runs: [],
        total: 0,
      },
    },
    docs: {
      description: {
        story: 'Shows scheduled transformations that have never been run',
      },
    },
  },
};

export const MixedRunTimes: StoryObj<typeof meta> = {
  parameters: {
    mockData: {
      transformations: mockTransformations,
      runs: {
        runs: [
          {
            id: 'beacon_api.daily_summary',
            last_run: new Date(Date.now() - 1000 * 60 * 30).toISOString(), // 30 minutes ago
          },
          {
            id: 'beacon_api.weekly_report',
            last_run: new Date(Date.now() - 1000 * 60 * 60 * 24 * 3).toISOString(), // 3 days ago
          },
          {
            id: 'beacon_api.monthly_stats',
            last_run: new Date(Date.now() - 1000 * 60 * 60 * 24 * 30).toISOString(), // 30 days ago
          },
        ],
        total: 3,
      },
    },
    docs: {
      description: {
        story: 'Shows scheduled transformations with various last run times',
      },
    },
  },
};

export const Empty: StoryObj<typeof meta> = {
  parameters: {
    mockData: {
      transformations: {
        models: [],
        total: 0,
      },
      runs: {
        runs: [],
        total: 0,
      },
    },
    docs: {
      description: {
        story: 'Shows empty state when no scheduled transformations exist',
      },
    },
  },
};
