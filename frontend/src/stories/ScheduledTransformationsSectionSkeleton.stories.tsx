import type { Meta, StoryObj } from '@storybook/react-vite';
import { ScheduledTransformationsSectionSkeleton } from '@components/ScheduledTransformationsSectionSkeleton';

const meta = {
  title: 'Components/ScheduledTransformationsSectionSkeleton',
  component: ScheduledTransformationsSectionSkeleton,
  parameters: {
    layout: 'padded',
    backgrounds: {
      default: 'dark',
      values: [
        {
          name: 'dark',
          value: '#0f172a',
        },
      ],
    },
  },
  tags: ['autodocs'],
} satisfies Meta<typeof ScheduledTransformationsSectionSkeleton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => (
    <div className="min-h-screen bg-slate-900 p-6">
      <ScheduledTransformationsSectionSkeleton />
    </div>
  ),
};

export const InContainer: Story = {
  render: () => (
    <div className="min-h-screen bg-slate-900 p-6">
      <div className="mx-auto max-w-4xl">
        <h2 className="mb-4 text-xl font-bold text-white">Scheduled Transformations</h2>
        <ScheduledTransformationsSectionSkeleton />
      </div>
    </div>
  ),
};
