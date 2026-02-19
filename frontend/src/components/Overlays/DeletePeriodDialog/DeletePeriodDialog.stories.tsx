import type { Meta, StoryObj } from '@storybook/react-vite';
import type { IntervalTypeTransformation, Range } from '@/api/types.gen';
import { DeletePeriodDialog } from './DeletePeriodDialog';

const sampleTransformations: IntervalTypeTransformation[] = [
  { name: 'Slot', expression: 'math.floor((value - 1606824023) / 12)' },
  { name: 'Datetime', expression: 'value * 1000', format: 'datetime' },
  { name: 'Timestamp' },
];

const sampleCoverageRanges: Range[] = [
  { position: 100, interval: 200 },
  { position: 400, interval: 150 },
  { position: 700, interval: 300 },
];

const largeCoverageRanges: Range[] = [
  { position: 1606824023, interval: 2592000 },
  { position: 1609416023, interval: 2592000 },
  { position: 1614600023, interval: 5184000 },
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

export const WithCoverageRanges: Story = {
  args: {
    open: true,
    onClose: () => {},
    onDelete: () => {},
    coverageRanges: sampleCoverageRanges,
  },
};

export const WithCoverageAndTransformations: Story = {
  args: {
    open: true,
    onClose: () => {},
    onDelete: () => {},
    transformations: sampleTransformations,
    coverageRanges: largeCoverageRanges,
  },
};
