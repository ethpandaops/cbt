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
      className="flex size-8 items-center justify-center rounded-md bg-surface/95 text-primary shadow-sm ring-1 ring-border/75 transition-all hover:bg-secondary/85 hover:text-accent hover:ring-accent/55 focus:ring-2 focus:ring-accent/55 focus:outline-hidden active:scale-[0.98] dark:bg-surface/85 dark:text-foreground dark:ring-border/65 dark:hover:bg-secondary/80 dark:hover:ring-accent/65"
      aria-label={`Current theme: ${label}. Click to toggle.`}
      title={label}
    >
      {theme === 'light' ? <SunIcon className="size-5 text-amber-500" /> : <MoonIcon className="size-5 text-accent" />}
    </button>
  );
}
