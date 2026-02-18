import { useMemo, useSyncExternalStore } from 'react';
import { resolveCssColorToHex } from '@/utils/color';

/**
 * Theme color interface - all semantic tokens as hex strings
 */
export interface ThemeColors {
  primary: string;
  secondary: string;
  accent: string;
  background: string;
  surface: string;
  foreground: string;
  muted: string;
  border: string;
  success: string;
  warning: string;
  danger: string;
  external: string;
  scheduled: string;
  incremental: string;
  orEdge: string;
}

/**
 * Get current theme colors by reading computed CSS variables
 */
function getComputedThemeColors(): ThemeColors {
  const styles = getComputedStyle(document.documentElement);

  return {
    primary: resolveCssColorToHex(styles.getPropertyValue('--color-primary').trim()),
    secondary: resolveCssColorToHex(styles.getPropertyValue('--color-secondary').trim()),
    accent: resolveCssColorToHex(styles.getPropertyValue('--color-accent').trim()),
    background: resolveCssColorToHex(styles.getPropertyValue('--color-background').trim()),
    surface: resolveCssColorToHex(styles.getPropertyValue('--color-surface').trim()),
    foreground: resolveCssColorToHex(styles.getPropertyValue('--color-foreground').trim()),
    muted: resolveCssColorToHex(styles.getPropertyValue('--color-muted').trim()),
    border: resolveCssColorToHex(styles.getPropertyValue('--color-border').trim()),
    success: resolveCssColorToHex(styles.getPropertyValue('--color-success').trim()),
    warning: resolveCssColorToHex(styles.getPropertyValue('--color-warning').trim()),
    danger: resolveCssColorToHex(styles.getPropertyValue('--color-danger').trim()),
    external: resolveCssColorToHex(styles.getPropertyValue('--color-external').trim()),
    scheduled: resolveCssColorToHex(styles.getPropertyValue('--color-scheduled').trim()),
    incremental: resolveCssColorToHex(styles.getPropertyValue('--color-incremental').trim()),
    orEdge: resolveCssColorToHex(styles.getPropertyValue('--color-or-edge').trim()),
  };
}

/**
 * Get current theme mode from HTML class
 */
function getCurrentTheme(): 'light' | 'dark' {
  if (document.documentElement.classList.contains('dark')) return 'dark';
  return 'light';
}

/**
 * Get theme colors as hex values suitable for chart libraries.
 *
 * Reactively updates when theme changes (light/dark mode).
 * Colors are computed from CSS variables (src/index.css), ensuring single source of truth.
 *
 * @returns Object containing all semantic theme colors as hex strings
 */
export function useThemeColors(): ThemeColors {
  const theme = useSyncExternalStore(
    callback => {
      const observer = new MutationObserver(callback);
      observer.observe(document.documentElement, {
        attributes: true,
        attributeFilter: ['class'],
      });
      return () => observer.disconnect();
    },
    getCurrentTheme,
    getCurrentTheme
  );

  // eslint-disable-next-line react-hooks/exhaustive-deps -- theme dependency triggers recomputation on theme change
  return useMemo(() => getComputedThemeColors(), [theme]);
}
