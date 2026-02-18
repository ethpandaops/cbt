import { type JSX, useEffect, useState, useMemo, useCallback } from 'react';
import { ThemeContext, type Theme } from '@/contexts/ThemeContext';

interface ThemeProviderProps {
  children: React.ReactNode;
}

function getSystemTheme(): Theme {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light';
}

function getInitialTheme(): Theme {
  const stored = localStorage.getItem('theme') as Theme | null;
  if (stored === 'light' || stored === 'dark') {
    return stored;
  }
  return getSystemTheme();
}

export function ThemeProvider({ children }: ThemeProviderProps): JSX.Element {
  const [theme, setThemeState] = useState<Theme>(getInitialTheme);

  // Apply theme to document and update theme-color meta tag
  useEffect(() => {
    const root = document.documentElement;
    root.classList.remove('dark');
    if (theme === 'dark') {
      root.classList.add('dark');
    }

    const themeColors: Record<Theme, string> = { light: '#f6f6f6', dark: '#050a14' };
    const meta = document.querySelector('meta[name="theme-color"]');
    if (meta) {
      meta.setAttribute('content', themeColors[theme]);
    }
  }, [theme]);

  // Listen for system theme changes (only if no localStorage override)
  useEffect(() => {
    const stored = localStorage.getItem('theme');
    if (stored) return;

    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)');
    const handleChange = (): void => {
      setThemeState(getSystemTheme());
    };

    mediaQuery.addEventListener('change', handleChange);
    return () => mediaQuery.removeEventListener('change', handleChange);
  }, []);

  const setTheme = useCallback((newTheme: Theme): void => {
    setThemeState(newTheme);
    const systemTheme = getSystemTheme();

    if (newTheme !== systemTheme) {
      localStorage.setItem('theme', newTheme);
    } else {
      localStorage.removeItem('theme');
    }
  }, []);

  const clearTheme = useCallback((): void => {
    localStorage.removeItem('theme');
    setThemeState(getSystemTheme());
  }, []);

  const value = useMemo(() => ({ theme, setTheme, clearTheme }), [theme, setTheme, clearTheme]);

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}
