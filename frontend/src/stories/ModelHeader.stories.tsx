import type { Meta, StoryObj } from '@storybook/react-vite';
import { ModelHeader } from '@/components/ModelHeader';

const meta = {
  title: 'Components/ModelHeader',
  component: ModelHeader,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ModelHeader>;

export default meta;
type Story = StoryObj<typeof meta>;

export const External: Story = {
  args: {
    modelId: 'beacon_api.external_source_data',
    modelType: 'external',
  },
};

export const Scheduled: Story = {
  args: {
    modelId: 'beacon_api.scheduled_daily_report',
    modelType: 'scheduled',
  },
};

export const Incremental: Story = {
  args: {
    modelId: 'beacon_api.incremental_transformation',
    modelType: 'incremental',
  },
};

export const LongName: Story = {
  args: {
    modelId: 'beacon_api.very_long_model_name_that_shows_truncation_behavior',
    modelType: 'incremental',
  },
};
