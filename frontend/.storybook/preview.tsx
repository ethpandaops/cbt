import type { Preview, ReactRenderer } from '@storybook/react-vite';
import { INITIAL_VIEWPORTS } from 'storybook/viewport';
import { RouterProvider, createMemoryHistory, createRootRoute, createRouter } from '@tanstack/react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import '../src/index.css';

const preview: Preview = {
  decorators: [
    Story => {
      // Create QueryClient inside decorator for proper isolation per story
      const queryClient = new QueryClient({
        defaultOptions: {
          queries: {
            retry: false,
            staleTime: Infinity,
          },
        },
      });

      // Create a simple root route that just renders the Story
      const rootRoute = createRootRoute({
        component: () => <Story />,
      });

      // Create a router with memory history (no browser URL changes)
      const router = createRouter({
        routeTree: rootRoute,
        history: createMemoryHistory({
          initialEntries: ['/'],
        }),
      });

      return (
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      );
    },
  ] as ReactRenderer['decorators'],
  parameters: {
    viewport: {
      viewports: INITIAL_VIEWPORTS,
    },
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
    options: {
      storySort: {
        order: [
          'Components',
          [
            'BackToDashboardButton',
            'ModelHeader',
            'ModelInfoCard',
            'ScheduledModelCard',
            'CoverageBar',
            'ModelCoverageRow',
            'ZoomControls',
            'DagNode',
            'DagGraph',
            'DependencyRow',
            'ModelSearchCombobox',
          ],
          '*',
        ],
      },
    },
  },
};

export default preview;
