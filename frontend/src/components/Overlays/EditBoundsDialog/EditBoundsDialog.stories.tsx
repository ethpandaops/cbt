import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn } from 'storybook/test';
import { EditBoundsDialog } from './EditBoundsDialog';

const meta: Meta<typeof EditBoundsDialog> = {
  title: 'Components/Overlays/EditBoundsDialog',
  component: EditBoundsDialog,
  decorators: [
    Story => (
      <div className="bg-background p-8">
        <Story />
      </div>
    ),
  ],
  args: {
    onClose: fn(),
    onSave: fn(),
    onDelete: fn(),
  },
};

export default meta;
type Story = StoryObj<typeof EditBoundsDialog>;

export const Default: Story = {
  args: {
    open: true,
    currentMin: 100000,
    currentMax: 999999,
  },
};

export const Empty: Story = {
  args: {
    open: true,
  },
};

export const Saving: Story = {
  args: {
    open: true,
    currentMin: 100000,
    currentMax: 999999,
    isSaving: true,
  },
};
