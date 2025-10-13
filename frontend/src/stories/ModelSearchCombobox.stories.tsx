import type { Meta, StoryObj } from '@storybook/react-vite';
import { expect, userEvent, within } from 'storybook/test';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ModelSearchCombobox } from '@/components/ModelSearchCombobox';

// Create a query client for stories
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
    },
  },
});

const meta = {
  title: 'Components/ModelSearchCombobox',
  component: ModelSearchCombobox,
  parameters: {
    layout: 'centered',
  },
  decorators: [
    Story => (
      <QueryClientProvider client={queryClient}>
        <div className="min-h-96 w-full bg-slate-900 p-8">
          <Story />
        </div>
      </QueryClientProvider>
    ),
  ],
  tags: ['autodocs', 'test'],
} satisfies Meta<typeof ModelSearchCombobox>;

export default meta;
type Story = StoryObj<typeof meta>;

/**
 * Full search bar variant (desktop)
 */
export const FullVariant: Story = {
  args: {
    variant: 'full',
  },
};

/**
 * Icon variant for mobile
 */
export const IconVariant: Story = {
  args: {
    variant: 'icon',
  },
};

/**
 * Responsive demonstration
 */
export const Responsive: Story = {
  render: () => (
    <div className="space-y-8">
      <div>
        <h3 className="mb-4 text-sm/6 font-semibold text-slate-300">Desktop (Full)</h3>
        <div className="hidden md:block">
          <ModelSearchCombobox variant="full" />
        </div>
      </div>
      <div>
        <h3 className="mb-4 text-sm/6 font-semibold text-slate-300">Mobile (Icon)</h3>
        <div className="md:hidden">
          <ModelSearchCombobox variant="icon" />
        </div>
      </div>
    </div>
  ),
};

/**
 * Interaction test: Typing in the search input
 */
export const SearchInteraction: Story = {
  args: {
    variant: 'full',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Find the search input
    const searchInput = canvas.getByPlaceholderText('Search models...');
    await expect(searchInput).toBeInTheDocument();

    // Simulate typing in the search
    await userEvent.type(searchInput, 'test');

    // Assert the input has the typed value
    await expect(searchInput).toHaveValue('test');
  },
};

/**
 * Interaction test: Clicking the clear button
 */
export const ClearSearchInteraction: Story = {
  args: {
    variant: 'full',
  },
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);

    // Find and type in the search input
    const searchInput = canvas.getByPlaceholderText('Search models...');
    await userEvent.type(searchInput, 'beacon');
    await expect(searchInput).toHaveValue('beacon');

    // Wait a moment for the clear button to appear
    await new Promise(resolve => setTimeout(resolve, 100));

    // Find and click the clear button - it's a button without a role, so use a class selector
    const clearButton = canvasElement.querySelector('button') as HTMLButtonElement;
    await expect(clearButton).toBeTruthy();
    await userEvent.click(clearButton);

    // Assert the input is cleared
    await expect(searchInput).toHaveValue('');
  },
};
