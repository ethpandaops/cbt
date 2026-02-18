import type { Preview, ReactRenderer } from '@storybook/react-vite';
import { INITIAL_VIEWPORTS } from 'storybook/viewport';
import { RouterProvider, createMemoryHistory, createRootRoute, createRouter } from '@tanstack/react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ThemeProvider } from '../src/providers/ThemeProvider';
import { initialize, mswLoader } from 'msw-storybook-addon';
import { withThemeByClassName } from '@storybook/addon-themes';
import '../src/index.css';

// Initialize MSW
initialize({
  onUnhandledRequest: 'bypass',
  quiet: true,
});

const preview: Preview = {
  loaders: [mswLoader],
  decorators: [
    withThemeByClassName<ReactRenderer>({
      themes: {
        light: '',
        dark: 'dark',
      },
      defaultTheme: 'dark',
    }),
    (Story, context) => {
      const queryOptions = context.parameters.tanstackQuery?.queries || {};

      const queryClient = new QueryClient({
        defaultOptions: {
          queries: {
            retry: 0,
            staleTime: Infinity,
            ...queryOptions,
          },
        },
      });

      const routerConfig = context.parameters.router || {};
      const initialUrl = routerConfig.initialUrl || '/';
      const initialSearch = routerConfig.initialSearch || {};

      const rootRoute = createRootRoute({
        component: () => <Story />,
        validateSearch: () => initialSearch,
      });

      const searchString =
        Object.keys(initialSearch).length > 0
          ? '?' + new URLSearchParams(initialSearch as Record<string, string>).toString()
          : '';

      const router = createRouter({
        routeTree: rootRoute,
        history: createMemoryHistory({
          initialEntries: [initialUrl + searchString],
        }),
        defaultPendingMinMs: 0,
      });

      return (
        <QueryClientProvider client={queryClient} key={context.id}>
          <ThemeProvider>
            <RouterProvider router={router} />
          </ThemeProvider>
        </QueryClientProvider>
      );
    },
  ] as ReactRenderer['decorators'],
  parameters: {
    msw: {
      handlers: [],
    },
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
        method: 'alphabetical',
      },
    },
  },
};

export default preview;
