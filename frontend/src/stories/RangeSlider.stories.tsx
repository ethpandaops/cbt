import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn, expect, userEvent } from 'storybook/test';
import { useState, type ReactElement } from 'react';
import { RangeSlider } from '@/components/RangeSlider';

// Wrapper component to handle local state for smooth dragging
function RangeSliderWrapper(props: React.ComponentProps<typeof RangeSlider>): ReactElement {
  const [localZoom, setLocalZoom] = useState({ start: props.zoomStart, end: props.zoomEnd });

  const handleZoomChange = (start: number, end: number): void => {
    // Update local state immediately for smooth dragging without Storybook re-render
    setLocalZoom({ start, end });
    // Still call the action/mock for the Actions panel
    props.onZoomChange?.(start, end);
  };

  return <RangeSlider {...props} zoomStart={localZoom.start} zoomEnd={localZoom.end} onZoomChange={handleZoomChange} />;
}

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
  render: args => <RangeSliderWrapper {...args} />,
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

/**
 * Interaction test: Click and drag slider to show smooth interaction
 */
export const SliderInteraction: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 300,
    zoomEnd: 700,
    onZoomChange: fn(),
  },
  play: async ({ canvasElement }) => {
    // Verify the slider elements exist with correct roles
    const sliders = canvasElement.querySelectorAll('[role="slider"]');
    await expect(sliders.length).toBe(2);

    // Get the lower thumb (left knob)
    const lowerThumb = sliders[0] as HTMLElement;
    const initialValue = lowerThumb.getAttribute('aria-valuenow');
    await expect(initialValue).toBeTruthy();

    // Get the position of the thumb
    const thumbRect = lowerThumb.getBoundingClientRect();
    const startX = thumbRect.left + thumbRect.width / 2;
    const startY = thumbRect.top + thumbRect.height / 2;

    // Simulate a click and drag to the right
    await userEvent.pointer([{ keys: '[MouseLeft>]', target: lowerThumb, coords: { x: startX, y: startY } }]);
    await new Promise(resolve => setTimeout(resolve, 100));

    // Move right slowly (simulating drag)
    await userEvent.pointer({ coords: { x: startX + 20, y: startY } });
    await new Promise(resolve => setTimeout(resolve, 100));

    await userEvent.pointer({ coords: { x: startX + 40, y: startY } });
    await new Promise(resolve => setTimeout(resolve, 100));

    await userEvent.pointer({ coords: { x: startX + 60, y: startY } });
    await new Promise(resolve => setTimeout(resolve, 100));

    // Release the mouse button
    await userEvent.pointer({ keys: '[/MouseLeft]' });
    await userEvent.pointer({ keys: '[MouseLeft]', coords: { x: startX + 60, y: startY } });
    await new Promise(resolve => setTimeout(resolve, 100));

    // Verify the value changed after dragging (proving drag works)
    const finalValue = lowerThumb.getAttribute('aria-valuenow');
    await expect(finalValue).not.toBe(initialValue);

    // Verify both sliders are still present
    await expect(sliders[0]).toBeInTheDocument();
    await expect(sliders[1]).toBeInTheDocument();
  },
};
