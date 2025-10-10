import type { Meta, StoryObj } from '@storybook/react-vite';
import { LoadingState } from '@/components/shared/LoadingState';

const meta = {
  title: 'Components/Shared/LoadingState',
  component: LoadingState,
  parameters: {
    layout: 'centered',
  },
  tags: ['autodocs'],
  argTypes: {
    size: {
      control: 'select',
      options: ['sm', 'md', 'lg'],
      description: 'Size of the loading indicator',
    },
    message: {
      control: 'text',
      description: 'Loading message to display',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof LoadingState>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {},
};

export const Small: Story = {
  args: {
    size: 'sm',
    message: 'Loading...',
  },
};

export const Medium: Story = {
  args: {
    size: 'md',
    message: 'Loading data...',
  },
};

export const Large: Story = {
  args: {
    size: 'lg',
    message: 'Loading transformations...',
  },
};

export const CustomMessage: Story = {
  args: {
    size: 'md',
    message: 'Fetching coverage data from the server...',
  },
};

export const ShortMessage: Story = {
  args: {
    size: 'md',
    message: 'Wait...',
  },
};

export const LongMessage: Story = {
  args: {
    size: 'lg',
    message: 'Loading all transformation models and calculating coverage statistics...',
  },
};
