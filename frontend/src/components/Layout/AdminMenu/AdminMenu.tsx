import { type JSX, useState, Fragment } from 'react';
import { Menu, MenuButton, MenuItem, MenuItems, Transition } from '@headlessui/react';
import { LockClosedIcon, Cog6ToothIcon, ArrowRightStartOnRectangleIcon } from '@heroicons/react/24/outline';
import { useAuth } from '@/hooks/useAuth';
import { PasswordLoginDialog } from '@/components/Overlays/PasswordLoginDialog';

export function AdminMenu(): JSX.Element | null {
  const { managementEnabled, authMethods, session, isLoading, loginWithPassword, loginWithGitHub, logout } = useAuth();
  const [showPasswordDialog, setShowPasswordDialog] = useState(false);
  const [passwordError, setPasswordError] = useState<string>();
  const [isSubmitting, setIsSubmitting] = useState(false);

  if (!managementEnabled || isLoading) {
    return null;
  }

  const isAuthenticated = session?.authenticated ?? false;
  const hasPassword = authMethods.includes('password');
  const hasGitHub = authMethods.includes('github');
  const noAuth = authMethods.length === 0;

  const handlePasswordSubmit = (password: string): void => {
    setPasswordError(undefined);
    setIsSubmitting(true);

    loginWithPassword(password)
      .then(authenticated => {
        if (authenticated) {
          setShowPasswordDialog(false);
          setPasswordError(undefined);
        } else {
          setPasswordError('Incorrect password');
        }
      })
      .catch(() => {
        setPasswordError('Login failed');
      })
      .finally(() => {
        setIsSubmitting(false);
      });
  };

  const handlePasswordDialogClose = (): void => {
    if (isSubmitting) return;
    setShowPasswordDialog(false);
    setPasswordError(undefined);
  };

  // No auth configured — show subtle admin indicator.
  if (noAuth) {
    return (
      <div
        className="flex size-8 items-center justify-center rounded-md bg-success/10 text-success ring-1 ring-success/30"
        title="Management enabled (no auth)"
      >
        <Cog6ToothIcon className="size-5" />
      </div>
    );
  }

  // Not authenticated — show lock icon with login options.
  if (!isAuthenticated) {
    // Single auth method: direct action.
    if (hasPassword && !hasGitHub) {
      return (
        <>
          <button
            type="button"
            onClick={() => setShowPasswordDialog(true)}
            className="flex size-8 items-center justify-center rounded-md bg-surface/95 text-muted shadow-sm ring-1 ring-border/75 transition-all hover:bg-secondary/85 hover:text-accent hover:ring-accent/55 focus:ring-2 focus:ring-accent/55 focus:outline-hidden active:scale-[0.98] dark:bg-surface/85 dark:ring-border/65 dark:hover:bg-secondary/80 dark:hover:ring-accent/65"
            title="Admin login"
          >
            <LockClosedIcon className="size-5" />
          </button>
          <PasswordLoginDialog
            open={showPasswordDialog}
            onClose={handlePasswordDialogClose}
            onSubmit={handlePasswordSubmit}
            error={passwordError}
            isSubmitting={isSubmitting}
          />
        </>
      );
    }

    if (hasGitHub && !hasPassword) {
      return (
        <button
          type="button"
          onClick={loginWithGitHub}
          className="flex size-8 items-center justify-center rounded-md bg-surface/95 text-muted shadow-sm ring-1 ring-border/75 transition-all hover:bg-secondary/85 hover:text-accent hover:ring-accent/55 focus:ring-2 focus:ring-accent/55 focus:outline-hidden active:scale-[0.98] dark:bg-surface/85 dark:ring-border/65 dark:hover:bg-secondary/80 dark:hover:ring-accent/65"
          title="Login with GitHub"
        >
          <LockClosedIcon className="size-5" />
        </button>
      );
    }

    // Both auth methods — dropdown.
    return (
      <>
        <Menu as="div" className="relative">
          <MenuButton className="flex size-8 items-center justify-center rounded-md bg-surface/95 text-muted shadow-sm ring-1 ring-border/75 transition-all hover:bg-secondary/85 hover:text-accent hover:ring-accent/55 focus:ring-2 focus:ring-accent/55 focus:outline-hidden active:scale-[0.98] dark:bg-surface/85 dark:ring-border/65 dark:hover:bg-secondary/80 dark:hover:ring-accent/65">
            <LockClosedIcon className="size-5" />
          </MenuButton>
          <Transition
            as={Fragment}
            enter="transition ease-out duration-100"
            enterFrom="transform opacity-0 scale-95"
            enterTo="transform opacity-100 scale-100"
            leave="transition ease-in duration-75"
            leaveFrom="transform opacity-100 scale-100"
            leaveTo="transform opacity-0 scale-95"
          >
            <MenuItems className="absolute right-0 z-50 mt-2 w-48 origin-top-right rounded-lg border border-border/50 bg-surface py-1 shadow-lg ring-1 ring-black/5 focus:outline-hidden">
              <MenuItem>
                <button
                  type="button"
                  onClick={() => setShowPasswordDialog(true)}
                  className="block w-full px-4 py-2 text-left text-sm text-foreground data-[focus]:bg-secondary/50"
                >
                  Login with password
                </button>
              </MenuItem>
              <MenuItem>
                <button
                  type="button"
                  onClick={loginWithGitHub}
                  className="block w-full px-4 py-2 text-left text-sm text-foreground data-[focus]:bg-secondary/50"
                >
                  Login with GitHub
                </button>
              </MenuItem>
            </MenuItems>
          </Transition>
        </Menu>
        <PasswordLoginDialog
          open={showPasswordDialog}
          onClose={handlePasswordDialogClose}
          onSubmit={handlePasswordSubmit}
          error={passwordError}
          isSubmitting={isSubmitting}
        />
      </>
    );
  }

  // Authenticated — show cog dropdown with user info + logout.
  return (
    <Menu as="div" className="relative">
      <MenuButton className="flex size-8 items-center justify-center rounded-md bg-success/10 text-success shadow-sm ring-1 ring-success/30 transition-all hover:bg-success/20 hover:ring-success/50 focus:ring-2 focus:ring-success/50 focus:outline-hidden active:scale-[0.98]">
        <Cog6ToothIcon className="size-5" />
      </MenuButton>
      <Transition
        as={Fragment}
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <MenuItems className="absolute right-0 z-50 mt-2 w-48 origin-top-right rounded-lg border border-border/50 bg-surface py-1 shadow-lg ring-1 ring-black/5 focus:outline-hidden">
          {session?.username && (
            <div className="border-b border-border/30 px-4 py-2">
              <p className="text-xs text-muted">Signed in as</p>
              <p className="truncate text-sm font-medium text-foreground">{session.username}</p>
            </div>
          )}
          <MenuItem>
            <button
              type="button"
              onClick={() => void logout()}
              className="flex w-full items-center gap-2 px-4 py-2 text-left text-sm text-foreground data-[focus]:bg-secondary/50"
            >
              <ArrowRightStartOnRectangleIcon className="size-4" />
              Logout
            </button>
          </MenuItem>
        </MenuItems>
      </Transition>
    </Menu>
  );
}
