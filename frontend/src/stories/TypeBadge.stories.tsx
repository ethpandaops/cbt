import type { Meta, StoryObj } from '@storybook/react-vite';
import { TypeBadge } from '@/components/shared/TypeBadge';

const meta = {
  title: 'Components/Shared/TypeBadge',
  component: TypeBadge,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  argTypes: {
    type: {
      control: 'select',
      options: ['external', 'scheduled', 'incremental'],
      description: 'Type of model badge',
    },
    compact: {
      control: 'boolean',
      description: 'Whether to show compact version',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof TypeBadge>;

export default meta;
type Story = StoryObj<typeof meta>;

export const External: Story = {
  args: {
    type: 'external',
    compact: false,
  },
};

export const ExternalCompact: Story = {
  args: {
    type: 'external',
    compact: true,
  },
};

export const Scheduled: Story = {
  args: {
    type: 'scheduled',
    compact: false,
  },
};

export const ScheduledCompact: Story = {
  args: {
    type: 'scheduled',
    compact: true,
  },
};

export const Incremental: Story = {
  args: {
    type: 'incremental',
    compact: false,
  },
};

export const IncrementalCompact: Story = {
  args: {
    type: 'incremental',
    compact: true,
  },
};

export const AllTypesDefault: Story = {
  args: {
    type: 'external',
    compact: false,
  },
  render: () => (
    <div className="flex flex-col gap-4">
      <TypeBadge type="external" />
      <TypeBadge type="scheduled" />
      <TypeBadge type="incremental" />
    </div>
  ),
};

export const AllTypesCompact: Story = {
  args: {
    type: 'external',
    compact: true,
  },
  render: () => (
    <div className="flex gap-2">
      <TypeBadge type="external" compact />
      <TypeBadge type="scheduled" compact />
      <TypeBadge type="incremental" compact />
    </div>
  ),
};
