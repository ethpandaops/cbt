import type { Meta, StoryObj } from '@storybook/react-vite';
import { ModelInfoCard } from './ModelInfoCard';

const meta = {
  title: 'Components/ModelInfoCard',
  component: ModelInfoCard,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  decorators: [
    Story => (
      <div className="bg-background p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ModelInfoCard>;

export default meta;
type Story = StoryObj<typeof meta>;

export const ExternalModel: Story = {
  args: {
    title: 'Model Information',
    borderColor: 'border-external/30',
    fields: [
      { label: 'Database', value: 'clickhouse' },
      { label: 'Table', value: 'beacon_api_blocks' },
      { label: 'Interval Type', value: 'slot_number' },
      { label: 'Min Position', value: '0', variant: 'highlight', highlightColor: 'external' },
      { label: 'Max Position', value: '8,500,000', variant: 'highlight', highlightColor: 'external' },
    ],
  },
};

export const ScheduledModel: Story = {
  args: {
    title: 'Transformation Details',
    borderColor: 'border-scheduled/30',
    fields: [
      { label: 'Database', value: 'clickhouse' },
      { label: 'Table', value: 'daily_summary' },
      { label: 'Content Type', value: 'parquet' },
      { label: 'Schedule', value: '0 0 * * *', variant: 'highlight', highlightColor: 'scheduled' },
      { label: 'Last Run', value: '2 hours ago' },
      { label: 'Status', value: 'success' },
    ],
  },
};

export const IncrementalModel: Story = {
  args: {
    title: 'Model Information',
    borderColor: 'border-incremental/30',
    columns: 4,
    fields: [
      { label: 'Database', value: 'clickhouse' },
      { label: 'Table', value: 'transformed_data' },
      { label: 'Type', value: 'incremental', variant: 'highlight', highlightColor: 'incremental' },
      { label: 'Content Type', value: 'parquet' },
    ],
  },
};

export const TwoColumnLayout: Story = {
  args: {
    title: 'Basic Information',
    columns: 2,
    fields: [
      { label: 'Database', value: 'postgres' },
      { label: 'Table', value: 'users' },
      { label: 'Rows', value: '1,234,567' },
      { label: 'Size', value: '256 MB' },
    ],
  },
};

export const WithMixedFields: Story = {
  args: {
    title: 'Model Details',
    fields: [
      { label: 'Database', value: 'clickhouse' },
      { label: 'Table', value: 'analytics_data' },
      { label: 'Status', value: 'Active', variant: 'highlight', highlightColor: 'external' },
      { label: 'Priority', value: 'High', variant: 'highlight', highlightColor: 'incremental' },
      { label: 'Created', value: '2024-01-15' },
      { label: 'Updated', value: '2 hours ago' },
    ],
  },
};
