import type { Meta, StoryObj } from '@storybook/react-vite';
import { ConfigOverrideDialog } from './ConfigOverrideDialog';

const meta: Meta<typeof ConfigOverrideDialog> = {
  title: 'Components/Overlays/ConfigOverrideDialog',
  component: ConfigOverrideDialog,
  decorators: [
    Story => (
      <div className="bg-background p-8">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof ConfigOverrideDialog>;

export const IncrementalNew: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.incremental_table',
    modelType: 'incremental',
    currentOverride: null,
  },
};

export const IncrementalWithOverride: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.incremental_table',
    modelType: 'incremental',
    currentOverride: {
      model_id: 'db.incremental_table',
      model_type: 'transformation',
      enabled: true,
      override: { interval: { max: 100 }, schedules: { forwardfill: '@every 30s' } },
      updated_at: '2025-01-15T10:30:00Z',
    },
  },
};

export const ScheduledNew: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.scheduled_table',
    modelType: 'scheduled',
    currentOverride: null,
  },
};

export const ExternalNew: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.external_table',
    modelType: 'external',
    currentOverride: null,
  },
};

export const ExternalWithOverride: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.external_table',
    modelType: 'external',
    currentOverride: {
      model_id: 'db.external_table',
      model_type: 'external',
      override: { lag: 50, cache: { incremental_scan_interval: '5m' } },
      updated_at: '2025-01-15T10:30:00Z',
    },
  },
};

export const Saving: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.table',
    modelType: 'incremental',
    isSaving: true,
  },
};

export const Deleting: Story = {
  args: {
    open: true,
    onClose: () => {},
    onSave: () => {},
    onDelete: () => {},
    modelId: 'db.table',
    modelType: 'incremental',
    currentOverride: {
      model_id: 'db.table',
      model_type: 'transformation',
      enabled: false,
      override: null,
      updated_at: '2025-01-15T10:30:00Z',
    },
    isDeleting: true,
  },
};
