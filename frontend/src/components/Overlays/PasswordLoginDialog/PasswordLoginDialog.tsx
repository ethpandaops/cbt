import { type JSX, useState } from 'react';
import { Dialog, DialogPanel, DialogTitle, DialogBackdrop } from '@headlessui/react';
import { KeyIcon } from '@heroicons/react/24/outline';

interface PasswordLoginDialogProps {
  open: boolean;
  onClose: () => void;
  onSubmit: (password: string) => void;
  error?: string;
  isSubmitting?: boolean;
}

export function PasswordLoginDialog({
  open,
  onClose,
  onSubmit,
  error,
  isSubmitting,
}: PasswordLoginDialogProps): JSX.Element {
  const [password, setPassword] = useState('');

  const handleSubmit = (e: React.FormEvent): void => {
    e.preventDefault();
    if (password.trim() && !isSubmitting) {
      onSubmit(password.trim());
    }
  };

  return (
    <Dialog open={open} onClose={onClose} className="relative z-50">
      <DialogBackdrop className="fixed inset-0 bg-primary/28 backdrop-blur-sm" />
      <div className="fixed inset-0 flex items-center justify-center p-4">
        <DialogPanel className="glass-surface w-full max-w-sm p-6">
          <div className="flex items-center gap-3">
            <div className="flex size-10 items-center justify-center rounded-lg border border-accent/30 bg-accent/12 ring-1 ring-accent/20">
              <KeyIcon className="size-5 text-accent" />
            </div>
            <DialogTitle className="text-lg font-semibold text-foreground">Admin Login</DialogTitle>
          </div>

          <form onSubmit={handleSubmit} className="mt-4 space-y-4">
            <div>
              <label htmlFor="admin-password" className="block text-sm font-medium text-muted">
                Password
              </label>
              <input
                id="admin-password"
                type="password"
                value={password}
                onChange={e => setPassword(e.target.value)}
                disabled={isSubmitting}
                className="mt-1 block w-full rounded-lg border border-border/65 bg-surface/90 px-3 py-2 text-sm text-foreground shadow-xs ring-1 ring-border/35 placeholder:text-muted/60 focus:border-accent focus:ring-2 focus:ring-accent/45 focus:outline-hidden disabled:opacity-50"
                placeholder="Enter admin password"
                autoFocus
              />
              {error && <p className="mt-1.5 text-sm text-danger">{error}</p>}
            </div>

            <div className="flex justify-end gap-2">
              <button
                type="button"
                onClick={onClose}
                disabled={isSubmitting}
                className="glass-control px-3 py-2 text-sm font-medium text-muted hover:text-primary disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={!password.trim() || isSubmitting}
                className="rounded-lg border border-accent/50 bg-accent px-4 py-2 text-sm font-medium text-white shadow-xs transition-colors hover:bg-accent/90 disabled:opacity-50 dark:text-background"
              >
                {isSubmitting ? 'Checking...' : 'Login'}
              </button>
            </div>
          </form>
        </DialogPanel>
      </div>
    </Dialog>
  );
}
