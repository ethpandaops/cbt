import { type JSX } from 'react';
import { createRootRoute, Outlet, Link } from '@tanstack/react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import Logo from '/logo.png';

const queryClient = new QueryClient();

function RootComponent(): JSX.Element {
  return (
    <QueryClientProvider client={queryClient}>
      <div className="relative min-h-dvh bg-gradient-to-br from-slate-950 via-slate-900 to-slate-950">
        {/* Ambient background decoration */}
        <div className="pointer-events-none fixed inset-0 overflow-hidden">
          <div className="absolute -left-48 -top-48 size-96 rounded-full bg-gradient-to-br from-indigo-500/20 to-purple-500/20 blur-3xl" />
          <div className="absolute -right-48 top-48 size-96 rounded-full bg-gradient-to-br from-blue-500/20 to-cyan-500/20 blur-3xl" />
          <div className="absolute -bottom-48 left-1/2 size-96 -translate-x-1/2 rounded-full bg-gradient-to-br from-violet-500/20 to-fuchsia-500/20 blur-3xl" />
        </div>

        <header className="relative border-b border-slate-700/50 bg-slate-900/60 shadow-xl backdrop-blur-xl">
          <div className="mx-auto max-w-screen-2xl px-4 py-6 sm:px-6 lg:px-8">
            <div className="flex items-center gap-4">
              <div className="relative">
                <div className="absolute inset-0 rounded-xl bg-gradient-to-br from-indigo-500/30 to-purple-500/30 blur-lg" />
                <img
                  src={Logo}
                  className="relative size-14 rounded-xl object-contain p-2 ring-1 ring-slate-700/50 backdrop-blur-sm"
                  alt="Logo"
                />
              </div>
              <div className="flex flex-1 items-center justify-between">
                <div>
                  <Link to="/" className="group inline-flex items-baseline gap-3 transition-all">
                    <h1 className="bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-3xl font-black tracking-tight text-transparent transition-all group-hover:from-indigo-300 group-hover:via-purple-300 group-hover:to-indigo-300">
                      CBT Dashboard
                    </h1>
                  </Link>
                  <p className="mt-1.5 text-sm font-medium text-slate-400">
                    ClickHouse Build Tool · Real-time Model Coverage Analytics
                  </p>
                </div>
                <nav className="flex items-center gap-4">
                  <Link
                    to="/"
                    className="rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400 [&.active]:bg-indigo-500/20 [&.active]:text-indigo-300"
                    activeProps={{ className: 'active' }}
                  >
                    Dashboard
                  </Link>
                  <Link
                    to="/dag"
                    className="rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400 [&.active]:bg-indigo-500/20 [&.active]:text-indigo-300"
                    activeProps={{ className: 'active' }}
                  >
                    DAG View
                  </Link>
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
