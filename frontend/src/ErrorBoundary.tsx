import { Component, type ErrorInfo, type ReactNode } from 'react';

interface Props {
  children?: ReactNode;
}

interface State {
  hasError: boolean;
}

class ErrorBoundary extends Component<Props, State> {
  public state: State = {
    hasError: false,
  };

  public static getDerivedStateFromError(): State {
    // Update state so the next render will show the fallback UI.
    return { hasError: true };
  }

  public componentDidCatch(error: Error, errorInfo: ErrorInfo): void {
    console.error('Uncaught error:', error, errorInfo);
  }

  public render(): ReactNode {
    if (this.state.hasError) {
      return (
        <div className="flex h-screen w-screen flex-col items-center justify-center bg-slate-950">
          <img src="/logo.png" className="h-72 w-72 rotate-180 object-contain" alt="CBT Logo" />
          <h1 className="mt-6 text-2xl text-red-400">Uhh... Something went wrong</h1>
          <button
            onClick={() => window.location.reload()}
            className="mt-4 rounded-lg bg-red-500/20 px-6 py-2 text-sm text-red-300 transition-colors hover:bg-red-500/30"
          >
            Reload Page
          </button>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
