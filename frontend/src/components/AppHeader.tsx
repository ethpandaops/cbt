import { type JSX } from 'react';
import { Link } from '@tanstack/react-router';
import Logo from '/logo.png';

export interface AppHeaderProps {
  showLinks?: boolean;
}

export function AppHeader({ showLinks = true }: AppHeaderProps): JSX.Element {
  return (
    <header className="relative border-b border-slate-700/50 bg-slate-900/60 shadow-xl backdrop-blur-xl">
      <div className="mx-auto max-w-screen-2xl px-4 py-4 sm:px-6 sm:py-6 lg:px-8">
        <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:gap-6">
          <div className="relative group shrink-0">
            <div className="absolute inset-0 rounded-xl bg-linear-to-br from-indigo-500/40 via-purple-500/40 to-pink-500/40 blur-xl transition-all duration-300 group-hover:blur-2xl group-hover:from-indigo-500/50 group-hover:via-purple-500/50 group-hover:to-pink-500/50 sm:rounded-2xl" />
            <div className="absolute inset-0 rounded-xl bg-linear-to-br from-indigo-500/20 to-purple-500/20 blur-sm sm:rounded-2xl" />
            <img
              src={Logo}
              className="relative size-14 rounded-xl object-contain p-2.5 backdrop-blur-sm transition-all duration-300 group-hover:scale-105 sm:size-20 sm:rounded-2xl sm:p-3"
              alt="Logo"
            />
          </div>
          <div className="flex flex-1 flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
            <div>
              {showLinks ? (
                <Link to="/" className="group inline-flex items-baseline gap-2 transition-all sm:gap-3">
                  <h1 className="bg-linear-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-xl font-black tracking-tight text-transparent transition-all group-hover:from-indigo-300 group-hover:via-purple-300 group-hover:to-indigo-300 sm:text-2xl lg:text-3xl">
                    CBT Dashboard
                  </h1>
                </Link>
              ) : (
                <h1 className="bg-linear-to-r from-indigo-400 via-purple-400 to-indigo-400 bg-clip-text text-xl font-black tracking-tight text-transparent sm:text-2xl lg:text-3xl">
                  CBT Dashboard
                </h1>
              )}
              <p className="mt-1 text-xs font-medium text-slate-400 sm:mt-1.5 sm:text-sm/6">
                ClickHouse Build Tool · Real-time Model Coverage Analytics
              </p>
            </div>
            <nav className="flex items-center gap-2 sm:gap-4">
              {showLinks ? (
                <>
                  <Link
                    to="/"
                    className="rounded-lg px-3 py-1.5 text-xs font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400 sm:px-4 sm:py-2 sm:text-sm [&.active]:bg-indigo-500/20 [&.active]:text-indigo-300"
                    activeProps={{ className: 'active' }}
                  >
                    Dashboard
                  </Link>
                  <Link
                    to="/dag"
                    className="rounded-lg px-3 py-1.5 text-xs font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400 sm:px-4 sm:py-2 sm:text-sm [&.active]:bg-indigo-500/20 [&.active]:text-indigo-300"
                    activeProps={{ className: 'active' }}
                  >
                    DAG View
                  </Link>
                </>
              ) : (
                <>
                  <button className="rounded-lg px-3 py-1.5 text-xs font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400 sm:px-4 sm:py-2 sm:text-sm">
                    Dashboard
                  </button>
                  <button className="rounded-lg px-3 py-1.5 text-xs font-semibold text-slate-300 transition-all hover:bg-slate-800/60 hover:text-indigo-400 sm:px-4 sm:py-2 sm:text-sm">
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
