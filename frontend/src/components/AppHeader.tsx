import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import Logo from '/logo.png';

export interface AppHeaderProps {
  showLinks?: boolean;
}

export function AppHeader({ showLinks = true }: AppHeaderProps): JSX.Element {
  return (
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
              {showLinks ? (
                <Link to="/" className="group inline-flex items-baseline gap-3 transition-all">
                  <h1 className="bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-3xl font-black tracking-tight text-transparent transition-all group-hover:from-indigo-300 group-hover:via-purple-300 group-hover:to-indigo-300">
                    CBT Dashboard
                  </h1>
                </Link>
              ) : (
                <h1 className="bg-gradient-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-3xl font-black tracking-tight text-transparent">
                  CBT Dashboard
                </h1>
              )}
              <p className="mt-1.5 text-sm font-medium text-slate-400">
                ClickHouse Build Tool · Real-time Model Coverage Analytics
              </p>
            </div>
            <nav className="flex items-center gap-4">
              {showLinks ? (
                <>
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
                </>
              ) : (
                <>
                  <button className="rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400">
                    Dashboard
                  </button>
                  <button className="rounded-lg px-4 py-2 text-sm font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400">
                    DAG View
                  </button>
                </>
              )}
            </nav>
          </div>
        </div>
      </div>
    </header>
  );
}
