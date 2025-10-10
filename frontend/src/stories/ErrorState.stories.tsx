import type { Meta, StoryObj } from '@storybook/react-vite';
import { ErrorState } from '@/components/shared/ErrorState';

const meta = {
  title: 'Components/Shared/ErrorState',
  component: ErrorState,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  argTypes: {
    variant: {
      control: 'select',
      options: ['default', 'compact'],
      description: 'Visual variant of the error message',
    },
    message: {
      control: 'text',
      description: 'Error message to display',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ErrorState>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    message: 'Failed to load data',
  },
};

export const Compact: Story = {
  args: {
    message: 'Failed to load data',
    variant: 'compact',
  },
};

export const NetworkError: Story = {
  args: {
    message: 'Network request failed',
    variant: 'default',
  },
};

export const NetworkErrorCompact: Story = {
  args: {
    message: 'Network request failed',
    variant: 'compact',
  },
};

export const LongErrorMessage: Story = {
  args: {
    message:
      'Unable to fetch transformation coverage data from the server. Please check your connection and try again.',
    variant: 'default',
  },
};

export const LongErrorMessageCompact: Story = {
  args: {
    message:
      'Unable to fetch transformation coverage data from the server. Please check your connection and try again.',
    variant: 'compact',
  },
};

export const DatabaseError: Story = {
  args: {
    message: 'Database connection timeout',
    variant: 'default',
  },
};

export const APIError: Story = {
  args: {
    message: 'API returned status 500',
    variant: 'compact',
  },
};
