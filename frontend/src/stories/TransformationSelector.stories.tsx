import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn } from 'storybook/test';
import { TransformationSelector } from '@/components/shared/TransformationSelector';

const meta = {
  title: 'Components/Shared/TransformationSelector',
  component: TransformationSelector,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  argTypes: {
    selectedIndex: {
      control: { type: 'number', min: 0 },
      description: 'Index of the currently selected transformation',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof TransformationSelector>;

export default meta;
type Story = StoryObj<typeof meta>;

export const TwoTransformations: Story = {
  args: {
    transformations: [
      { name: 'Slot Number', expression: 'value' },
      { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
    ],
    selectedIndex: 0,
    onSelect: fn(),
  },
};

export const TwoTransformationsSecondSelected: Story = {
  args: {
    transformations: [
      { name: 'Slot Number', expression: 'value' },
      { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
    ],
    selectedIndex: 1,
    onSelect: fn(),
  },
};

export const ThreeTransformations: Story = {
  args: {
    transformations: [
      { name: 'Slot Number', expression: 'value' },
      { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
      { name: 'Epoch', expression: 'value / 32' },
    ],
    selectedIndex: 0,
    onSelect: fn(),
  },
};

export const ManyTransformations: Story = {
  args: {
    transformations: [
      { name: 'Raw Value', expression: 'value' },
      { name: 'Date', expression: '(value * 12) + 1606824000', format: 'date' },
      { name: 'Epoch', expression: 'value / 32' },
      { name: 'Day', expression: 'value / 7200' },
      { name: 'Hour', expression: 'value / 300' },
      { name: 'Block', expression: 'value * 1000' },
    ],
    selectedIndex: 2,
    onSelect: fn(),
  },
};

export const LongNames: Story = {
  args: {
    transformations: [
      { name: 'Raw Slot Number Value', expression: 'value' },
      { name: 'Formatted Date with Timezone', expression: '(value * 12) + 1606824000', format: 'date' },
      { name: 'Beacon Chain Epoch Number', expression: 'value / 32' },
    ],
    selectedIndex: 1,
    onSelect: fn(),
  },
};

export const SingleTransformation: Story = {
  args: {
    transformations: [{ name: 'Slot Number', expression: 'value' }],
    selectedIndex: 0,
    onSelect: fn(),
  },
  parameters: {
    docs: {
      description: {
        story: 'Component returns null when there is only one transformation (nothing to select between)',
      },
    },
  },
};

export const NoTransformations: Story = {
  args: {
    transformations: [],
    selectedIndex: 0,
    onSelect: fn(),
  },
  parameters: {
    docs: {
      description: {
        story: 'Component returns null when there are no transformations',
      },
    },
  },
};
