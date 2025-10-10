import type { Meta, StoryObj } from '@storybook/react-vite';
import { ScheduledModelCard } from '@/components/ScheduledModelCard';

const meta = {
  title: 'Components/ScheduledModelCard',
  component: ScheduledModelCard,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <div className="max-w-sm">
          <Story />
        </div>
      </div>
    ),
  ],
} satisfies Meta<typeof ScheduledModelCard>;

export default meta;
type Story = StoryObj<typeof meta>;

export const EverySchedule: Story = {
  args: {
    id: 'beacon_api.scheduled_transformation',
    lastRun: new Date(Date.now() - 15 * 60 * 1000).toISOString(), // 15 minutes ago
    schedule: '@every 30m',
  },
};

export const CronSchedule: Story = {
  args: {
    id: 'beacon_api.hourly_job',
    lastRun: new Date(Date.now() - 25 * 60 * 1000).toISOString(), // 25 minutes ago
    schedule: '0 * * * *', // Every hour at minute 0
  },
};

export const FrequentCron: Story = {
  args: {
    id: 'beacon_api.frequent_job',
    lastRun: new Date(Date.now() - 2 * 60 * 1000).toISOString(), // 2 minutes ago
    schedule: '*/5 * * * *', // Every 5 minutes
  },
};

export const DailyCron: Story = {
  args: {
    id: 'beacon_api.daily_report',
    lastRun: new Date(Date.now() - 12 * 60 * 60 * 1000).toISOString(), // 12 hours ago
    schedule: '0 0 * * *', // Daily at midnight
  },
};

export const LateRunning: Story = {
  args: {
    id: 'beacon_api.late_job',
    lastRun: new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString(), // 2 hours ago (should have run every 30m)
    schedule: '@every 30m',
  },
};

export const VeryLateRunning: Story = {
  args: {
    id: 'beacon_api.stuck_job',
    lastRun: new Date(Date.now() - 3 * 24 * 60 * 60 * 1000).toISOString(), // 3 days ago
    schedule: '@every 1h',
  },
};

export const NoLastRun: Story = {
  args: {
    id: 'beacon_api.never_run_job',
    schedule: '@every 15m',
  },
};

export const LongName: Story = {
  args: {
    id: 'beacon_api.very_long_model_name_that_should_be_truncated_properly_in_the_card',
    lastRun: new Date(Date.now() - 10 * 60 * 1000).toISOString(), // 10 minutes ago
    schedule: '*/30 * * * *',
  },
};

export const ComplexCron: Story = {
  args: {
    id: 'beacon_api.business_hours_job',
    lastRun: new Date(Date.now() - 30 * 60 * 1000).toISOString(), // 30 minutes ago
    schedule: '0 9-17 * * 1-5', // Every hour from 9-5, Mon-Fri
  },
};
