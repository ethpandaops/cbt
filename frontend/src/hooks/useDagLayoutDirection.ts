import { useState, useEffect } from 'react';

const STORAGE_KEY = 'cbt-dag-layout-direction';

export type LayoutDirection = 'TB' | 'LR' | 'RL';

/**
 * Custom hook to manage persistent DAG layout direction.
 * Stores the layout direction in localStorage and persists across sessions.
 */
export function useDagLayoutDirection(): {
  layoutDirection: LayoutDirection;
  setLayoutDirection: (direction: LayoutDirection) => void;
} {
  const [layoutDirection, setLayoutDirectionState] = useState<LayoutDirection>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored && (stored === 'TB' || stored === 'LR' || stored === 'RL')) {
        return stored as LayoutDirection;
      }
      return 'TB'; // Default to Top-Bottom
    } catch {
      return 'TB';
    }
  });

  // Persist layout direction to localStorage whenever it changes
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, layoutDirection);
    } catch {
      // Ignore storage errors (e.g., quota exceeded, disabled localStorage)
    }
  }, [layoutDirection]);

  const setLayoutDirection = (direction: LayoutDirection): void => {
    setLayoutDirectionState(direction);
  };

  return {
    layoutDirection,
    setLayoutDirection,
  };
}
