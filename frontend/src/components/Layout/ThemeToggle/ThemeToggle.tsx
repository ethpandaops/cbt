import { type JSX } from 'react';
import { MoonIcon, SunIcon } from '@heroicons/react/24/solid';
import { useTheme } from '@/hooks/useTheme';

/**
 * ThemeToggle component that toggles between light and dark themes.
 *
 * Displays the current theme icon and switches to the opposite theme on click.
 */
export function ThemeToggle(): JSX.Element {
  const { theme, setTheme } = useTheme();

  const toggleTheme = (): void => {
    setTheme(theme === 'light' ? 'dark' : 'light');
  };

  const label = theme === 'light' ? 'Light theme' : 'Dark theme';

  return (
    <button
      type="button"
      onClick={toggleTheme}
      className="glass-icon-control"
      aria-label={`Current theme: ${label}. Click to toggle.`}
      title={label}
    >
      {theme === 'light' ? <SunIcon className="size-5 text-warning" /> : <MoonIcon className="size-5 text-accent" />}
    </button>
  );
}
