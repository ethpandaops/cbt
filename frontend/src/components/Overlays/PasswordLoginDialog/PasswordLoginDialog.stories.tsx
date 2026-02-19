import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn } from 'storybook/test';
import { PasswordLoginDialog } from './PasswordLoginDialog';

const meta: Meta<typeof PasswordLoginDialog> = {
  title: 'Components/Overlays/PasswordLoginDialog',
  component: PasswordLoginDialog,
  decorators: [
    Story => (
      <div className="bg-background p-8">
        <Story />
      </div>
    ),
  ],
  args: {
    onClose: fn(),
    onSubmit: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof PasswordLoginDialog>;

export const Default: Story = {
  args: {
    open: true,
  },
};

export const WithError: Story = {
  args: {
    open: true,
    error: 'Incorrect password',
  },
};

export const Submitting: Story = {
  args: {
    open: true,
    isSubmitting: true,
  },
};
