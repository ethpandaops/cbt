import { StrictMode, type JSX } from 'react';
import { createRoot } from 'react-dom/client';
import { RouterProvider, createRouter } from '@tanstack/react-router';
import './index.css';

// Import the generated route tree
import { routeTree } from './routeTree.gen';

// Custom error component for TanStack Router
function CustomErrorComponent({ error }: { error: Error }): JSX.Element {
  console.error('Route error:', error);

  return (
    <div className="flex h-screen w-screen flex-col items-center justify-center bg-slate-950">
      <img src="/logo.png" className="h-72 w-72 rotate-180 object-contain" alt="CBT Logo" />
      <h1 className="mt-6 text-2xl text-red-400">Uhh... Something went wrong</h1>
      <p className="mt-2 text-sm text-slate-400">{error.message}</p>
      <button
        onClick={() => window.location.reload()}
        className="mt-4 rounded-lg bg-red-500/20 px-6 py-2 text-sm text-red-300 transition-colors hover:bg-red-500/30"
      >
        Reload Page
      </button>
    </div>
  );
}

// Create a new router instance with custom error component
const router = createRouter({
  routeTree,
  defaultErrorComponent: CustomErrorComponent,
});

// Register the router instance for type safety
declare module '@tanstack/react-router' {
  interface Register {
    router: typeof router;
  }
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <RouterProvider router={router} />
  </StrictMode>
);
