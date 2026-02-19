import type { Meta, StoryObj } from '@storybook/react-vite';
import type { IntervalTypeTransformation } from '@/api/types.gen';
import { DeletePeriodDialog } from './DeletePeriodDialog';

const sampleTransformations: IntervalTypeTransformation[] = [
  { name: 'Slot', expression: 'math.floor((value - 1606824023) / 12)' },
  { name: 'Datetime', expression: 'value * 1000', format: 'datetime' },
  { name: 'Timestamp' },
];

const meta: Meta<typeof DeletePeriodDialog> = {
  title: 'Components/Overlays/DeletePeriodDialog',
  component: DeletePeriodDialog,
  decorators: [
    Story => (
      <div className="bg-background p-8">
        <Story />
      </div>
    ),
  ],
};

export default meta;
type Story = StoryObj<typeof DeletePeriodDialog>;

export const Default: Story = {
  args: {
    open: true,
    onClose: () => {},
    onDelete: () => {},
  },
};

export const Deleting: Story = {
  args: {
    open: true,
    onClose: () => {},
    onDelete: () => {},
    isDeleting: true,
  },
};

export const WithTransformations: Story = {
  args: {
    open: true,
    onClose: () => {},
    onDelete: () => {},
    transformations: sampleTransformations,
  },
};

export const WithDatetimeSelected: Story = {
  args: {
    open: true,
    onClose: () => {},
    onDelete: () => {},
    transformations: [
      { name: 'Datetime', expression: 'value * 1000', format: 'datetime' },
      { name: 'Slot', expression: 'math.floor((value - 1606824023) / 12)' },
      { name: 'Timestamp' },
    ],
  },
};
