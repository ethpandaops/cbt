import type { Meta, StoryObj } from '@storybook/react-vite';
import { CoverageBar } from '@/components/CoverageBar';
import { Tooltip } from 'react-tooltip';

const meta = {
  title: 'Components/CoverageBar',
  component: CoverageBar,
  parameters: {
    layout: 'padded',
  },
  tags: ['autodocs'],
  argTypes: {
    type: {
      control: 'select',
      options: ['transformation', 'external', 'scheduled'],
      description: 'Type of model to display',
    },
    height: {
      control: { type: 'number', min: 12, max: 200 },
      description: 'Height of the bar in pixels',
    },
    zoomStart: {
      control: { type: 'number' },
      description: 'Start of zoom range',
    },
    zoomEnd: {
      control: { type: 'number' },
      description: 'End of zoom range',
    },
  },
  decorators: [
    Story => (
      <div className="bg-slate-950 p-8">
        <Story />
        <Tooltip
          id="coverage-tooltip"
          className="!bg-gray-900 !text-white !text-xs !px-2 !py-1 !rounded !opacity-100"
        />
      </div>
    ),
  ],
} satisfies Meta<typeof CoverageBar>;

export default meta;
type Story = StoryObj<typeof meta>;

// Transformation model with single range
export const TransformationSingle: Story = {
  args: {
    type: 'transformation',
    ranges: [{ position: 100, interval: 500 }],
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// Transformation model with multiple ranges
export const TransformationMultiple: Story = {
  args: {
    type: 'transformation',
    ranges: [
      { position: 100, interval: 200 },
      { position: 400, interval: 150 },
      { position: 700, interval: 250 },
    ],
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// Transformation model with overlapping ranges (should merge)
export const TransformationOverlapping: Story = {
  args: {
    type: 'transformation',
    ranges: [
      { position: 100, interval: 200 },
      { position: 250, interval: 150 },
      { position: 350, interval: 100 },
    ],
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// Transformation model with adjacent ranges (should merge)
export const TransformationAdjacent: Story = {
  args: {
    type: 'transformation',
    ranges: [
      { position: 100, interval: 200 },
      { position: 300, interval: 200 },
      { position: 500, interval: 200 },
    ],
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// External model
export const External: Story = {
  args: {
    type: 'external',
    bounds: { min: 200, max: 800 },
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// External model partially visible (left)
export const ExternalPartialLeft: Story = {
  args: {
    type: 'external',
    bounds: { min: 0, max: 300 },
    zoomStart: 200,
    zoomEnd: 1000,
    height: 24,
  },
};

// External model partially visible (right)
export const ExternalPartialRight: Story = {
  args: {
    type: 'external',
    bounds: { min: 700, max: 1200 },
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// Scheduled transformation
export const Scheduled: Story = {
  args: {
    type: 'scheduled',
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// Tall bar for detail view
export const TallTransformation: Story = {
  args: {
    type: 'transformation',
    ranges: [
      { position: 100, interval: 200 },
      { position: 400, interval: 150 },
      { position: 700, interval: 250 },
    ],
    zoomStart: 0,
    zoomEnd: 1000,
    height: 96,
  },
};

// Zoomed in view
export const ZoomedIn: Story = {
  args: {
    type: 'transformation',
    ranges: [
      { position: 100, interval: 200 },
      { position: 400, interval: 150 },
      { position: 700, interval: 250 },
    ],
    zoomStart: 350,
    zoomEnd: 600,
    height: 24,
  },
};

// Empty transformation (no coverage)
export const Empty: Story = {
  args: {
    type: 'transformation',
    ranges: [],
    zoomStart: 0,
    zoomEnd: 1000,
    height: 24,
  },
};

// With transformation formatting (slot_number -> date)
export const WithTransformation: Story = {
  args: {
    type: 'transformation',
    ranges: [
      { position: 10461696, interval: 7200 },
      { position: 10475520, interval: 7200 },
    ],
    zoomStart: 10450000,
    zoomEnd: 10500000,
    height: 24,
    transformation: {
      name: 'Date',
      expression: '(value * 12) + 1606824000',
      format: 'date',
    },
  },
};
