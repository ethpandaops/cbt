import type { Meta, StoryObj } from '@storybook/react-vite';
import { BackToDashboardButton } from '@/components/BackToDashboardButton';

const meta = {
  title: 'Components/BackToDashboardButton',
  component: BackToDashboardButton,
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
} satisfies Meta<typeof BackToDashboardButton>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    showLink: false,
  },
};
