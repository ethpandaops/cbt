import { useState, useEffect, useCallback } from 'react';
import type { IntervalTypeTransformation } from '@api/types.gen';

const STORAGE_KEY = 'cbt-transformation-selections';

/**
 * Custom hook to manage persistent transformation selections per interval type.
 * Stores selections in localStorage and validates them against available transformations.
 */
export function useTransformationSelection(): {
  getSelectedIndex: (intervalType: string, transformations: IntervalTypeTransformation[]) => number;
  setSelectedIndex: (intervalType: string, index: number) => void;
} {
  const [selections, setSelections] = useState<Record<string, number>>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      return stored ? JSON.parse(stored) : {};
    } catch {
      return {};
    }
  });

  // Persist selections to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(selections));
    } catch {
      // Ignore storage errors (e.g., quota exceeded, disabled localStorage)
    }
  }, [selections]);

  /**
   * Get the selected index for an interval type, validating against available transformations.
   * Returns 0 (default) if the stored index is invalid or out of bounds.
   */
  const getSelectedIndex = useCallback(
    (intervalType: string, transformations: IntervalTypeTransformation[]): number => {
      const storedIndex = selections[intervalType] ?? 0;

      // Validate that the stored index is within bounds
      if (transformations.length === 0) {
        return 0;
      }

      if (storedIndex >= 0 && storedIndex < transformations.length) {
        return storedIndex;
      }

      // Invalid index - reset to 0
      return 0;
    },
    [selections]
  );

  /**
   * Set the selected index for an interval type
   */
  const setSelectedIndex = useCallback((intervalType: string, index: number) => {
    setSelections(prev => ({
      ...prev,
      [intervalType]: index,
    }));
  }, []);

  return {
    getSelectedIndex,
    setSelectedIndex,
  };
}
