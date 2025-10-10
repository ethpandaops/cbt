import type { Meta, StoryObj } from '@storybook/react-vite';
import { ModelSearchCombobox } from '@/components/ModelSearchCombobox';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { createMemoryHistory, createRootRoute, createRouter, RouterProvider } from '@tanstack/react-router';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: false,
      staleTime: Infinity,
    },
  },
});

// Create a root route for the router
const rootRoute = createRootRoute();

// Create router with memory history for Storybook
const memoryHistory = createMemoryHistory({
  initialEntries: ['/'],
});

const router = createRouter({
  routeTree: rootRoute,
  history: memoryHistory,
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
        <RouterProvider router={router}>
          <div className="min-h-96 w-full bg-slate-900 p-8">
            <Story />
          </div>
        </RouterProvider>
      </QueryClientProvider>
    ),
  ],
  tags: ['autodocs'],
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
