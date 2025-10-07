import { type JSX } from 'react';
import Logo from '/logo.png';
import { ModelsList } from './components/ModelsList';

function App(): JSX.Element {
  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      <header className="border-b border-gray-200 bg-white/80 backdrop-blur-sm">
        <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
          <div className="flex items-center gap-4">
            <img src={Logo} className="h-12 w-12 object-contain" alt="Logo" />
            <div>
              <h1 className="text-3xl font-bold tracking-tight text-gray-900">CBT Dashboard</h1>
              <p className="mt-1 text-sm text-gray-500">ClickHouse Build Tool</p>
            </div>
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <ModelsList />
      </main>

      <footer className="border-t border-gray-200 bg-white/50 backdrop-blur-sm">
        <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
          <p className="text-center text-sm text-gray-500">
            Built with{' '}
            <a
              href="https://github.com/ethpandaops"
              target="_blank"
              rel="noopener noreferrer"
              className="font-medium text-indigo-600 hover:text-indigo-500"
            >
              ethPandaOps
            </a>
          </p>
        </div>
      </footer>
    </div>
  );
}

export default App;
