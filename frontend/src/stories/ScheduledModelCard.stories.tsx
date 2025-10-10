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

export const Default: Story = {
  args: {
    id: 'beacon_api.scheduled_transformation',
    lastRun: '2025-10-10T14:30:00Z',
    showLink: false,
  },
};

export const RecentRun: Story = {
  args: {
    id: 'beacon_api.recent_scheduled_job',
    lastRun: new Date(Date.now() - 5 * 60 * 1000).toISOString(), // 5 minutes ago
    showLink: false,
  },
};

export const OldRun: Story = {
  args: {
    id: 'beacon_api.old_scheduled_job',
    lastRun: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(), // 7 days ago
    showLink: false,
  },
};

export const NoLastRun: Story = {
  args: {
    id: 'beacon_api.never_run_job',
    showLink: false,
  },
};

export const LongName: Story = {
  args: {
    id: 'beacon_api.very_long_model_name_that_should_be_truncated_properly_in_the_card',
    lastRun: '2025-10-10T10:00:00Z',
    showLink: false,
  },
};
