import type { Meta, StoryObj } from '@storybook/react-vite';
import { ZoomControls } from '@/components/ZoomControls';

const meta = {
  title: 'Components/ZoomControls',
  component: ZoomControls,
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
} satisfies Meta<typeof ZoomControls>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 0,
    zoomEnd: 1000,
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
  },
};

export const ZoomedIn: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 300,
    zoomEnd: 700,
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
  },
};

export const WithTransformation: Story = {
  args: {
    globalMin: 10450000,
    globalMax: 10500000,
    zoomStart: 10460000,
    zoomEnd: 10480000,
    transformation: {
      name: 'Date',
      expression: '(value * 12) + 1606824000',
      format: 'date',
    },
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
  },
};

export const WithoutResetButton: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 300,
    zoomEnd: 700,
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
  },
};

export const CustomTransformationName: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 300,
    zoomEnd: 700,
    transformationName: 'Custom Range',
    onZoomChange: (start, end) => console.log('Zoom changed:', start, end),
  },
};

// Interactive story - removed since Storybook 9 requires args for all stories
