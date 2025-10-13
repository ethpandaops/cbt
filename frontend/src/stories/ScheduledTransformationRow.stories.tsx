import type { Meta, StoryObj } from '@storybook/react-vite';
import { ScheduledTransformationRow } from '@/components/ScheduledTransformationRow';
import type { TransformationModel } from '@api/types.gen';

const meta = {
  title: 'Components/ScheduledTransformationRow',
  component: ScheduledTransformationRow,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <div className="space-y-2">
          <Story />
        </div>
      </div>
    ),
  ],
} satisfies Meta<typeof ScheduledTransformationRow>;

export default meta;
type Story = StoryObj<typeof meta>;

const baseModel: TransformationModel = {
  id: 'beacon_api.scheduled_transformation',
  database: 'beacon_api',
  table: 'scheduled_transformation',
  type: 'scheduled',
  content_type: 'sql',
  content: 'SELECT * FROM beacon_blocks',
  schedule: '@every 30m',
};

export const Default: Story = {
  args: {
    model: baseModel,
    lastRun: new Date(Date.now() - 15 * 60 * 1000).toISOString(), // 15 minutes ago
  },
};

export const WithDescription: Story = {
  args: {
    model: {
      ...baseModel,
      description: 'Aggregates beacon block data every 30 minutes for reporting dashboard',
    },
    lastRun: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
  },
};

export const WithSuccessStatus: Story = {
  args: {
    model: {
      ...baseModel,
      metadata: {
        last_run_status: 'success',
        last_run_at: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
      },
    },
    lastRun: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
  },
};

export const WithFailedStatus: Story = {
  args: {
    model: {
      ...baseModel,
      metadata: {
        last_run_status: 'failed',
        last_run_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      },
    },
    lastRun: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
  },
};

export const WithRunningStatus: Story = {
  args: {
    model: {
      ...baseModel,
      metadata: {
        last_run_status: 'running',
        last_run_at: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
      },
    },
    lastRun: new Date(Date.now() - 5 * 60 * 1000).toISOString(),
  },
};

export const WithPendingStatus: Story = {
  args: {
    model: {
      ...baseModel,
      metadata: {
        last_run_status: 'pending',
      },
    },
  },
};

export const CronSchedule: Story = {
  args: {
    model: {
      ...baseModel,
      schedule: '0 * * * *', // Every hour at minute 0
    },
    lastRun: new Date(Date.now() - 25 * 60 * 1000).toISOString(), // 25 minutes ago
  },
};

export const DailyCron: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.daily_report',
      schedule: '0 0 * * *', // Daily at midnight
    },
    lastRun: new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString(), // 12 hours ago
  },
};

export const Overdue: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.overdue_job',
      schedule: '@every 30m',
      metadata: {
        last_run_status: 'failed',
        last_run_at: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(),
      },
    },
    lastRun: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(), // 2 hours ago (should have run every 30m)
  },
};

export const VeryOverdue: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.stuck_job',
      schedule: '@every 1h',
      metadata: {
        last_run_status: 'failed',
        last_run_at: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(),
      },
    },
    lastRun: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(), // 3 days ago
  },
};

export const NoLastRun: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.never_run_job',
      schedule: '@every 15m',
    },
  },
};

export const LongName: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.very_long_transformation_name_that_should_truncate_properly_in_the_row_component',
      description: 'This is a very long description that should also truncate properly when displayed in the UI',
    },
    lastRun: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
  },
};

export const WithOrGroups: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.dependent_transformation',
      depends_on: [['beacon_api.source_a', 'beacon_api.source_b'], 'beacon_api.source_c'],
    },
    lastRun: new Date(Date.now() - 10 * 60 * 1000).toISOString(),
    orGroups: [0, 1],
  },
};

export const Highlighted: Story = {
  args: {
    model: baseModel,
    lastRun: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
    isHighlighted: true,
  },
};

export const Dimmed: Story = {
  args: {
    model: baseModel,
    lastRun: new Date(Date.now() - 15 * 60 * 1000).toISOString(),
    isDimmed: true,
  },
};

export const ComplexCron: Story = {
  args: {
    model: {
      ...baseModel,
      id: 'beacon_api.business_hours_job',
      schedule: '0 9-17 * * 1-5', // Every hour from 9-5, Mon-Fri
      description: 'Runs during business hours only, weekdays',
    },
    lastRun: new Date(Date.now() - 30 * 60 * 1000).toISOString(),
  },
};
