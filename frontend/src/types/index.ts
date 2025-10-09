import type { Range } from '@api/types.gen';

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
  data: {
    coverage?: Array<Range>;
    bounds?: { min: number; max: number };
  };
}
