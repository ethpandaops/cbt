import type { JSX } from 'react';
import { createRootRoute, Outlet, Link, HeadContent, useRouterState, useNavigate } from '@tanstack/react-router';
import { QueryClient, QueryClientProvider, useQuery } from '@tanstack/react-query';
import { Tab, TabGroup, TabList } from '@headlessui/react';
import {
  listAllModelsOptions,
  listExternalModelsOptions,
  listTransformationsOptions,
  listExternalBoundsOptions,
  listTransformationCoverageOptions,
  getIntervalTypesOptions,
  listScheduledRunsOptions,
} from '@/api/@tanstack/react-query.gen';
import Logo from '/logo.png';
import { ModelSearchCombobox } from '@/components/Forms/ModelSearchCombobox';
import { CustomErrorComponent } from '@/components/Feedback/CustomErrorComponent';
import { ThemeProvider } from '@/providers/ThemeProvider';
import { AuthProvider } from '@/providers/AuthProvider';
import { NotificationProvider } from '@/providers/NotificationProvider';
import { ThemeToggle } from '@/components/Layout/ThemeToggle';
import { AdminMenu } from '@/components/Layout/AdminMenu';

const queryClient = new QueryClient();

// BackgroundPoller: Runs all critical queries with continuous 60s polling
// This component is always mounted inside the QueryClientProvider
function BackgroundPoller(): null {
  // Root-level continuous polling (60s) for all critical dashboard endpoints
  // These queries run in the background throughout the entire app lifecycle
  // Child components automatically receive cached data via query key deduplication
  useQuery({
    ...listAllModelsOptions(),
    refetchInterval: 60000,
    staleTime: 55000,
  });

  useQuery({
    ...listExternalModelsOptions(),
    refetchInterval: 60000,
    staleTime: 55000,
  });

  // Poll all transformations unfiltered (components filter client-side as needed)
  useQuery({
    ...listTransformationsOptions(),
    refetchInterval: 60000,
    staleTime: 55000,
  });

  useQuery({
    ...listExternalBoundsOptions(),
    refetchInterval: 10000,
    staleTime: 55000,
  });

  useQuery({
    ...listTransformationCoverageOptions(),
    refetchInterval: 10000,
    staleTime: 55000,
  });

  useQuery({
    ...getIntervalTypesOptions(),
    refetchInterval: 60000,
    staleTime: 55000,
  });

  useQuery({
    ...listScheduledRunsOptions(),
    refetchInterval: 5000,
    staleTime: 55000,
  });

  return null;
}

function RootComponent(): JSX.Element {
  const navigate = useNavigate();
  const routerState = useRouterState();
  const currentPath = routerState.location.pathname;

  // Determine selected tab index based on current route
  const selectedIndex = currentPath === '/dag' ? 1 : 0;

  const handleTabChange = (index: number): void => {
    if (index === 0) {
      void navigate({ to: '/' });
    } else if (index === 1) {
      void navigate({ to: '/dag' });
    }
  };

  return (
    <ThemeProvider>
      <AuthProvider>
        <NotificationProvider>
          <QueryClientProvider client={queryClient}>
            <BackgroundPoller />
            <HeadContent />
            <div className="relative min-h-dvh bg-background">
              <header className="relative z-30 border-b border-border/50 bg-surface/60 shadow-sm backdrop-blur-xl">
                <div className="mx-auto max-w-screen-4xl px-4 py-6 sm:px-6 lg:px-8">
                  <div className="flex items-center gap-4 md:gap-6">
                    <div className="group relative shrink-0">
                      <div className="absolute -inset-0.5 rounded-2xl opacity-75 transition-opacity group-hover:opacity-100">
                        <div className="glimmer-border absolute inset-0 rounded-2xl" style={{ padding: '2px' }}>
                          <div className="size-full rounded-2xl bg-surface" />
                        </div>
                      </div>
                      <img
                        src={Logo}
                        className="relative size-12 rounded-2xl object-contain transition-all duration-500 group-hover:scale-105 sm:size-14 md:size-20"
                        alt="Logo"
                      />
                    </div>
                    <div className="flex min-w-0 flex-1 items-center justify-between gap-4">
                      <div className="min-w-0 shrink">
                        <Link to="/" className="group inline-flex items-baseline gap-3 transition-all">
                          <h1 className="text-2xl font-black tracking-tight text-accent transition-all group-hover:text-accent/80 sm:text-3xl">
                            <span className="md:hidden">CBT</span>
                            <span className="hidden md:inline">CBT Dashboard</span>
                          </h1>
                        </Link>
                        <p className="mt-1.5 hidden text-sm/6 font-medium text-muted lg:block">
                          ClickHouse Build Tool · Real-time Model Coverage Analytics
                        </p>
                      </div>

                      {/* Search - Icon on mobile/tablet, full on large screens */}
                      <div className="hidden flex-1 lg:block lg:max-w-xl">
                        <ModelSearchCombobox variant="full" />
                      </div>

                      <nav className="flex shrink-0 items-center gap-2">
                        {/* Mobile/tablet search icon */}
                        <div className="lg:hidden">
                          <ModelSearchCombobox variant="icon" />
                        </div>

                        <TabGroup selectedIndex={selectedIndex} onChange={handleTabChange}>
                          <TabList className="flex items-center gap-1 rounded-lg bg-secondary/40 p-1">
                            <Tab className="rounded-md px-3 py-1.5 text-xs font-semibold text-foreground/80 transition-all hover:bg-secondary/60 hover:text-accent focus:outline-hidden data-[selected]:bg-accent/20 data-[selected]:text-accent data-[selected]:shadow-xs sm:px-4 sm:py-2 sm:text-sm">
                              Dashboard
                            </Tab>
                            <Tab className="rounded-md px-3 py-1.5 text-xs font-semibold text-foreground/80 transition-all hover:bg-secondary/60 hover:text-accent focus:outline-hidden data-[selected]:bg-accent/20 data-[selected]:text-accent data-[selected]:shadow-xs sm:px-4 sm:py-2 sm:text-sm">
                              DAG View
                            </Tab>
                          </TabList>
                        </TabGroup>
                        <AdminMenu />
                        <ThemeToggle />
                      </nav>
                    </div>
                  </div>
                </div>
              </header>

              <main className="relative mx-auto max-w-screen-4xl px-4 py-10 sm:px-6 lg:px-8">
                <Outlet />
              </main>
            </div>
          </QueryClientProvider>
        </NotificationProvider>
      </AuthProvider>
    </ThemeProvider>
  );
}

export const Route = createRootRoute({
  component: RootComponent,
  errorComponent: CustomErrorComponent,
  head: () => ({
    meta: [
      { title: import.meta.env.VITE_BASE_TITLE },
      { charSet: 'utf-8' },
      { name: 'viewport', content: 'width=device-width, initial-scale=1.0, maximum-scale=5.0' },
      { name: 'description', content: 'ClickHouse Build Tool - Real-time Model Coverage Analytics' },
      { property: 'og:title', content: import.meta.env.VITE_BASE_TITLE },
      { property: 'og:description', content: 'ClickHouse Build Tool - Real-time Model Coverage Analytics' },
      { property: 'og:type', content: 'website' },
      { name: 'twitter:card', content: 'summary' },
      { name: 'twitter:title', content: import.meta.env.VITE_BASE_TITLE },
    ],
  }),
});
