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
    refetchInterval: 60000,
    staleTime: 55000,
  });

  useQuery({
    ...listTransformationCoverageOptions(),
    refetchInterval: 60000,
    staleTime: 55000,
  });

  useQuery({
    ...getIntervalTypesOptions(),
    refetchInterval: 60000,
    staleTime: 55000,
  });

  useQuery({
    ...listScheduledRunsOptions(),
    refetchInterval: 60000,
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

        <header className="relative border-b border-slate-700/50 bg-slate-900/60 shadow-xl backdrop-blur-xl">
          <div className="mx-auto max-w-screen-2xl px-4 py-6 sm:px-6 lg:px-8">
            <div className="flex items-center gap-6">
              <div className="relative group">
                <div className="absolute inset-0 rounded-2xl bg-gradient-to-br from-indigo-500/40 via-purple-500/40 to-pink-500/40 blur-xl transition-all duration-300 group-hover:blur-2xl group-hover:from-indigo-500/50 group-hover:via-purple-500/50 group-hover:to-pink-500/50" />
                <div className="absolute inset-0 rounded-2xl bg-gradient-to-br from-indigo-500/20 to-purple-500/20 blur-md" />
                <img
                  src={Logo}
                  className="relative size-14 rounded-2xl object-contain backdrop-blur-sm transition-all duration-300 group-hover:scale-105 md:size-20"
                  alt="Logo"
                />
              </div>
              <div className="flex flex-1 items-center justify-between">
                <div>
                  <Link to="/" className="group inline-flex items-baseline gap-3 transition-all">
                    <h1 className="bg-gradient-to-r from-orange-400 via-amber-400 to-orange-400 bg-clip-text text-3xl font-black tracking-tight text-transparent transition-all group-hover:from-orange-300 group-hover:via-amber-300 group-hover:to-orange-300">
                      <span className="md:hidden">CBT</span>
                      <span className="hidden md:inline">CBT Dashboard</span>
                    </h1>
                  </Link>
                  <p className="mt-1.5 hidden text-sm font-medium text-slate-400 md:block">
                    ClickHouse Build Tool · Real-time Model Coverage Analytics
                  </p>
                </div>
                <nav>
                  <TabGroup selectedIndex={selectedIndex} onChange={handleTabChange}>
                    <TabList className="flex items-center gap-1 rounded-lg bg-slate-800/40 p-1">
                      <Tab className="rounded-md px-4 py-2 text-sm font-semibold text-slate-300 transition-all hover:bg-slate-700/60 hover:text-indigo-400 data-[selected]:bg-indigo-500/20 data-[selected]:text-indigo-300 data-[selected]:shadow-sm focus:outline-none">
                        Dashboard
                      </Tab>
                      <Tab className="rounded-md px-4 py-2 text-sm font-semibold text-slate-300 transition-all hover:bg-slate-700/60 hover:text-indigo-400 data-[selected]:bg-indigo-500/20 data-[selected]:text-indigo-300 data-[selected]:shadow-sm focus:outline-none">
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
