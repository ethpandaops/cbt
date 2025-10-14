import { type JSX } from 'react';
import { createRootRoute, Outlet, Link, useRouterState, useNavigate } from '@tanstack/react-router';
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
} from '@api/@tanstack/react-query.gen';
import Logo from '/logo.png';
import { ModelSearchCombobox } from '@/components/ModelSearchCombobox';

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
    <QueryClientProvider client={queryClient}>
      <BackgroundPoller />
      <div className="relative min-h-dvh bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
        {/* Ambient background decoration */}
        <div className="pointer-events-none fixed inset-0 overflow-hidden">
          <div className="absolute -left-48 -top-48 size-96 rounded-full bg-gradient-to-br from-indigo-500/20 to-purple-500/20 blur-3xl" />
          <div className="absolute -right-48 top-48 size-96 rounded-full bg-gradient-to-br from-blue-500/20 to-cyan-500/20 blur-3xl" />
          <div className="absolute -bottom-48 left-1/2 size-96 -translate-x-1/2 rounded-full bg-gradient-to-br from-violet-500/20 to-fuchsia-500/20 blur-3xl" />
        </div>

        <header className="relative z-30 border-b border-slate-700/50 bg-slate-900/60 shadow-xl backdrop-blur-xl">
          <div className="mx-auto max-w-screen-2xl px-4 py-6 sm:px-6 lg:px-8">
            <div className="flex items-center gap-4 md:gap-6">
              <div className="relative group shrink-0">
                {/* Glass morphism container with Apple-style frosted glass effect */}
                <div className="absolute inset-0 rounded-2xl bg-white/5 backdrop-blur-xl" />

                {/* Subtle gradient overlay for depth */}
                <div className="absolute inset-0 rounded-2xl bg-linear-to-br from-white/10 via-transparent to-white/5" />

                {/* Very subtle inner shadow for glass edge effect */}
                <div
                  className="absolute inset-0 rounded-2xl shadow-inner"
                  style={{
                    boxShadow: 'inset 0 1px 2px 0 rgba(255, 255, 255, 0.1), inset 0 -1px 1px 0 rgba(0, 0, 0, 0.1)',
                  }}
                />

                {/* Animated border glimmer container */}
                <div className="absolute -inset-0.5 rounded-2xl opacity-75 group-hover:opacity-100 transition-opacity">
                  {/* Rotating conic gradient */}
                  <div className="glimmer-border absolute inset-0 rounded-2xl" style={{ padding: '2px' }}>
                    <div className="size-full rounded-2xl bg-slate-900/95" />
                  </div>

                  {/* Additional glow layer */}
                  <div
                    className="absolute inset-0 rounded-2xl bg-linear-to-r from-cyan-500/0 via-purple-500/30 to-pink-500/0 blur-md"
                    style={{
                      background:
                        'linear-gradient(105deg, transparent 40%, rgba(34, 211, 238, 0.3) 50%, transparent 60%)',
                      backgroundSize: '200% 200%',
                      animation: 'border-glimmer 3s linear infinite',
                    }}
                  />
                </div>

                {/* Logo image */}
                <img
                  src={Logo}
                  className="relative size-12 rounded-2xl object-contain transition-all duration-500 group-hover:scale-105 sm:size-14 md:size-20"
                  alt="Logo"
                />

                {/* Extra shimmer highlight */}
                <div
                  className="pointer-events-none absolute inset-0 rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-500"
                  style={{
                    background:
                      'linear-gradient(105deg, transparent 40%, rgba(255, 255, 255, 0.1) 50%, transparent 60%)',
                    backgroundSize: '200% 200%',
                    animation: 'border-glimmer 2s linear infinite',
                  }}
                />
              </div>
              <div className="flex min-w-0 flex-1 items-center justify-between gap-4">
                <div className="min-w-0 shrink">
                  <Link to="/" className="group inline-flex items-baseline gap-3 transition-all">
                    <h1 className="bg-linear-to-r from-orange-400 via-amber-400 to-orange-400 bg-clip-text text-2xl font-black tracking-tight text-transparent transition-all group-hover:from-orange-300 group-hover:via-amber-300 group-hover:to-orange-300 sm:text-3xl">
                      <span className="md:hidden">CBT</span>
                      <span className="hidden md:inline">CBT Dashboard</span>
                    </h1>
                  </Link>
                  <p className="mt-1.5 hidden text-sm/6 font-medium text-slate-400 lg:block">
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
                    <TabList className="flex items-center gap-1 rounded-lg bg-slate-800/40 p-1">
                      <Tab className="rounded-md px-3 py-1.5 text-xs font-semibold text-slate-300 transition-all hover:bg-slate-700/60 hover:text-indigo-400 data-[selected]:bg-indigo-500/20 data-[selected]:text-indigo-300 data-[selected]:shadow-sm focus:outline-hidden sm:px-4 sm:py-2 sm:text-sm">
                        Dashboard
                      </Tab>
                      <Tab className="rounded-md px-3 py-1.5 text-xs font-semibold text-slate-300 transition-all hover:bg-slate-700/60 hover:text-indigo-400 data-[selected]:bg-indigo-500/20 data-[selected]:text-indigo-300 data-[selected]:shadow-sm focus:outline-hidden sm:px-4 sm:py-2 sm:text-sm">
                        DAG View
                      </Tab>
                    </TabList>
                  </TabGroup>
                </nav>
              </div>
            </div>
          </div>
        </header>

        <main className="relative mx-auto max-w-screen-2xl px-4 py-10 sm:px-6 lg:px-8">
          <Outlet />
        </main>
      </div>
    </QueryClientProvider>
  );
}

export const Route = createRootRoute({
  component: RootComponent,
});
