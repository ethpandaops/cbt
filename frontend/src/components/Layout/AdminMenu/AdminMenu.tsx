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
  const lockedButtonClass = 'glass-icon-control text-muted';

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
        className="glass-icon-control border-success/35 bg-success/12 text-success ring-success/30 hover:bg-success/18 hover:text-success"
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
            className={lockedButtonClass}
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
        <button type="button" onClick={loginWithGitHub} className={lockedButtonClass} title="Login with GitHub">
          <LockClosedIcon className="size-5" />
        </button>
      );
    }

    // Both auth methods — dropdown.
    return (
      <>
        <Menu as="div" className="relative">
          <MenuButton className={lockedButtonClass}>
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
            <MenuItems className="glass-surface-subtle absolute right-0 z-50 mt-2 w-48 origin-top-right py-1 focus:outline-hidden">
              <MenuItem>
                <button
                  type="button"
                  onClick={() => setShowPasswordDialog(true)}
                  className="block w-full px-4 py-2 text-left text-sm text-foreground transition-colors data-[focus]:bg-secondary/55 data-[focus]:text-primary"
                >
                  Login with password
                </button>
              </MenuItem>
              <MenuItem>
                <button
                  type="button"
                  onClick={loginWithGitHub}
                  className="block w-full px-4 py-2 text-left text-sm text-foreground transition-colors data-[focus]:bg-secondary/55 data-[focus]:text-primary"
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
      <MenuButton className="glass-icon-control border-success/35 bg-success/12 text-success ring-success/30 hover:bg-success/20 hover:text-success hover:ring-success/45 focus:ring-success/40">
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
        <MenuItems className="glass-surface-subtle absolute right-0 z-50 mt-2 w-48 origin-top-right py-1 focus:outline-hidden">
          {session?.username && (
            <div className="border-b border-border/40 px-4 py-2">
              <p className="text-xs text-muted">Signed in as</p>
              <p className="truncate text-sm font-medium text-foreground">{session.username}</p>
            </div>
          )}
          <MenuItem>
            <button
              type="button"
              onClick={() => void logout()}
              className="flex w-full items-center gap-2 px-4 py-2 text-left text-sm text-foreground transition-colors data-[focus]:bg-secondary/55 data-[focus]:text-primary"
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
