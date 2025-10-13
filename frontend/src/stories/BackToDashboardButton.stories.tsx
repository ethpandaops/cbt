import type { Meta, StoryObj } from '@storybook/react-vite';
import { expect, within } from 'storybook/test';
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
  args: {},
};

/**
 * Interaction test: Verify button renders and has correct link
 */
export const ButtonInteraction: Story = {
  args: {},
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Verify the link exists and has correct text
    const link = canvas.getByRole('link', { name: /back to dashboard/i });
    await expect(link).toBeInTheDocument();

    // Verify it links to the root path
    await expect(link).toHaveAttribute('href', '/');

    // Verify the arrow icon is present
    const arrow = canvasElement.querySelector('svg');
    await expect(arrow).toBeTruthy();
  },
};
