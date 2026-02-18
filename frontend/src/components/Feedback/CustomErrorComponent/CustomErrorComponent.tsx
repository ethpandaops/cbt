import type { JSX } from 'react';

export function CustomErrorComponent({ error }: { error: Error }): JSX.Element {
  console.error('Route error:', error);

  return (
    <div className="fixed inset-0 z-50 flex flex-col items-center justify-center bg-background">
      <img src="/logo.png" className="h-72 w-72 rotate-180 object-contain" alt="CBT Logo" />
      <h1 className="mt-6 text-2xl text-danger">Uhh... Something went wrong</h1>
      <p className="mt-2 text-sm text-muted">{error.message}</p>
      <button
        onClick={() => window.location.reload()}
        className="mt-4 rounded-lg bg-danger/20 px-6 py-2 text-sm text-danger transition-colors hover:bg-danger/30"
      >
        Reload Page
      </button>
    </div>
  );
}
