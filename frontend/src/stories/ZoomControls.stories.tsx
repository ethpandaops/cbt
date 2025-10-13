import type { Meta, StoryObj } from '@storybook/react-vite';
import { fn, expect, userEvent, within } from 'storybook/test';
import { useState, type ReactElement } from 'react';
import { ZoomControls } from '@/components/ZoomControls';

// Wrapper component to handle local state for smooth dragging
function ZoomControlsWrapper(props: React.ComponentProps<typeof ZoomControls>): ReactElement {
  const [localZoom, setLocalZoom] = useState({ start: props.zoomStart, end: props.zoomEnd });

  const handleZoomChange = (start: number, end: number): void => {
    // Update local state immediately for smooth dragging without Storybook re-render
    setLocalZoom({ start, end });
    // Still call the action/mock for the Actions panel
    props.onZoomChange?.(start, end);
  };

  return (
    <ZoomControls {...props} zoomStart={localZoom.start} zoomEnd={localZoom.end} onZoomChange={handleZoomChange} />
  );
}

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
  render: args => <ZoomControlsWrapper {...args} />,
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

/**
 * Interaction test: Verify zoom controls render
 */
export const ZoomControlsInteraction: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 300,
    zoomEnd: 700,
    onZoomChange: fn(),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Verify the range slider exists
    const sliders = canvasElement.querySelectorAll('[role="slider"]');
    await expect(sliders.length).toBe(2);

    // Verify the min/max display shows globalMin and globalMax
    const minMaxDisplay = canvas.getByText(/min:.*0.*max:.*1,000/);
    await expect(minMaxDisplay).toBeInTheDocument();

    // Verify Range label is present
    const rangeLabel = canvas.getByText('Range');
    await expect(rangeLabel).toBeInTheDocument();
  },
};

/**
 * Interaction test: Click and drag slider to show smooth interaction
 */
export const ZoomControlsDragInteraction: Story = {
  args: {
    globalMin: 0,
    globalMax: 1000,
    zoomStart: 300,
    zoomEnd: 700,
    onZoomChange: fn(),
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Verify the slider elements exist with correct roles
    const sliders = canvasElement.querySelectorAll('[role="slider"]');
    await expect(sliders.length).toBe(2);

    // Get the lower thumb (left knob)
    const lowerThumb = sliders[0] as HTMLElement;
    const initialValue = lowerThumb.getAttribute('aria-valuenow');
    await expect(initialValue).toBeTruthy();

    // Verify initial range text is present
    const initialRangeText = canvas.getByText(/Range/);
    await expect(initialRangeText).toBeInTheDocument();

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

    // Verify the range text still displays after drag
    const finalRangeText = canvas.getByText(/Range/);
    await expect(finalRangeText).toBeInTheDocument();

    // Verify the min/max display is still present
    const minMaxDisplay = canvas.getByText(/min:.*0.*max:.*1,000/);
    await expect(minMaxDisplay).toBeInTheDocument();
  },
};
