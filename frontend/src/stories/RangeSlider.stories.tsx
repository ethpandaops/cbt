import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn } from 'storybook/test';
import { RangeSlider } from '@/components/RangeSlider';

const meta = {
  title: 'Components/RangeSlider',
  component: RangeSlider,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  argTypes: {
    globalMin: {
      control: { type: 'number' },
      description: 'Minimum value of the slider',
    },
    globalMax: {
      control: { type: 'number' },
      description: 'Maximum value of the slider',
    },
    zoomStart: {
      control: { type: 'number' },
      description: 'Current start value of the range',
    },
    zoomEnd: {
      control: { type: 'number' },
      description: 'Current end value of the range',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof RangeSlider>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 200,
    zoomEnd: 800,
    onZoomChange: fn(),
  },
};

export const FullRange: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 0,
    zoomEnd: 1000,
    onZoomChange: fn(),
  },
};

export const NarrowRange: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 450,
    zoomEnd: 550,
    onZoomChange: fn(),
  },
};

export const LargeNumbers: Story = {
  args: {
    globalMin: 10000000,
    globalMax: 20000000,
    zoomStart: 12000000,
    zoomEnd: 18000000,
    onZoomChange: fn(),
  },
};

export const SmallNumbers: Story = {
  args: {
    globalMin: 0,
    globalMax: 100,
    zoomStart: 20,
    zoomEnd: 80,
    onZoomChange: fn(),
  },
};

export const EdgeRangeLeft: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 0,
    zoomEnd: 300,
    onZoomChange: fn(),
  },
};

export const EdgeRangeRight: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 700,
    zoomEnd: 1000,
    onZoomChange: fn(),
  },
};
