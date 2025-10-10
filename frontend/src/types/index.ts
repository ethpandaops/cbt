import type { Range } from '@api/types.gen';

// Re-export model type configuration
export * from './modelTypes';

// Shared types for zoom functionality
export interface ZoomRange {
  start: number;
  end: number;
}

export type ZoomRanges = Record<string, ZoomRange>;

// Model with unified type for incremental section
export interface IncrementalModelItem {
  id: string;
  type: 'transformation' | 'external';
  intervalType: string;
  depends_on?: string[];
  data: {
    coverage?: Array<Range>;
    bounds?: { min: number; max: number };
  };
}
