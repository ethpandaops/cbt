import { describe, it, expect } from 'vitest';
import { findCoverageAtPosition, mergeRanges, findGapAtPosition } from './coverage-helpers';
import type { Range } from '@api/types.gen';

describe('coverage-helpers', () => {
  describe('findCoverageAtPosition', () => {
    it('should return null for undefined ranges', () => {
      expect(findCoverageAtPosition(undefined, 100)).toBeNull();
    });

    it('should return null for empty ranges', () => {
      expect(findCoverageAtPosition([], 100)).toBeNull();
    });

    it('should find coverage when position is at range start', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      const result = findCoverageAtPosition(ranges, 100);

      expect(result).toEqual({ position: 100, interval: 50 });
    });

    it('should find coverage when position is within range', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      const result = findCoverageAtPosition(ranges, 125);

      expect(result).toEqual({ position: 100, interval: 50 });
    });

    it('should find coverage when position is at range end - 1', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      const result = findCoverageAtPosition(ranges, 149);

      expect(result).toEqual({ position: 100, interval: 50 });
    });

    it('should return null when position is at range end (exclusive)', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      const result = findCoverageAtPosition(ranges, 150);

      expect(result).toBeNull();
    });

    it('should return null when position is before all ranges', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      const result = findCoverageAtPosition(ranges, 50);

      expect(result).toBeNull();
    });

    it('should return null when position is after all ranges', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      const result = findCoverageAtPosition(ranges, 200);

      expect(result).toBeNull();
    });

    it('should find coverage in multiple ranges', () => {
      const ranges: Range[] = [
        { position: 0, interval: 10 },
        { position: 20, interval: 10 },
        { position: 40, interval: 10 },
      ];

      expect(findCoverageAtPosition(ranges, 5)).toEqual({ position: 0, interval: 10 });
      expect(findCoverageAtPosition(ranges, 25)).toEqual({ position: 20, interval: 10 });
      expect(findCoverageAtPosition(ranges, 45)).toEqual({ position: 40, interval: 10 });
      expect(findCoverageAtPosition(ranges, 15)).toBeNull(); // Gap
      expect(findCoverageAtPosition(ranges, 35)).toBeNull(); // Gap
    });
  });

  describe('mergeRanges', () => {
    it('should return empty array for empty input', () => {
      expect(mergeRanges([])).toEqual([]);
    });

    it('should return single range unchanged', () => {
      const ranges: Range[] = [{ position: 100, interval: 50 }];
      expect(mergeRanges(ranges)).toEqual([{ position: 100, interval: 50 }]);
    });

    it('should merge adjacent ranges', () => {
      const ranges: Range[] = [
        { position: 0, interval: 10 },
        { position: 10, interval: 10 },
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([{ position: 0, interval: 20 }]);
    });

    it('should merge overlapping ranges', () => {
      const ranges: Range[] = [
        { position: 0, interval: 15 },
        { position: 10, interval: 10 },
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([{ position: 0, interval: 20 }]);
    });

    it('should not merge non-adjacent ranges', () => {
      const ranges: Range[] = [
        { position: 0, interval: 10 },
        { position: 20, interval: 10 },
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([
        { position: 0, interval: 10 },
        { position: 20, interval: 10 },
      ]);
    });

    it('should merge multiple overlapping ranges', () => {
      const ranges: Range[] = [
        { position: 0, interval: 10 },
        { position: 5, interval: 10 },
        { position: 10, interval: 10 },
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([{ position: 0, interval: 20 }]);
    });

    it('should sort ranges before merging', () => {
      const ranges: Range[] = [
        { position: 20, interval: 10 },
        { position: 0, interval: 10 },
        { position: 10, interval: 10 },
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([{ position: 0, interval: 30 }]);
    });

    it('should handle complex scenario with gaps and overlaps', () => {
      const ranges: Range[] = [
        { position: 0, interval: 5 },
        { position: 3, interval: 7 }, // Overlaps with first
        { position: 20, interval: 10 }, // Gap
        { position: 25, interval: 10 }, // Overlaps with previous
        { position: 50, interval: 10 }, // Gap
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([
        { position: 0, interval: 10 },
        { position: 20, interval: 15 },
        { position: 50, interval: 10 },
      ]);
    });

    it('should handle range completely contained in another', () => {
      const ranges: Range[] = [
        { position: 0, interval: 100 },
        { position: 10, interval: 10 },
      ];

      const result = mergeRanges(ranges);
      expect(result).toEqual([{ position: 0, interval: 100 }]);
    });
  });

  describe('findGapAtPosition', () => {
    const zoomStart = 0;
    const zoomEnd = 100;

    it('should return entire range as gap when ranges is undefined', () => {
      const result = findGapAtPosition(undefined, 50, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 0, interval: 100 });
    });

    it('should return entire range as gap when ranges is empty', () => {
      const result = findGapAtPosition([], 50, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 0, interval: 100 });
    });

    it('should return gap before first coverage', () => {
      const ranges: Range[] = [{ position: 50, interval: 10 }];
      const result = findGapAtPosition(ranges, 25, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 0, interval: 50 });
    });

    it('should return gap between two coverage ranges', () => {
      const ranges: Range[] = [
        { position: 10, interval: 10 },
        { position: 30, interval: 10 },
      ];
      const result = findGapAtPosition(ranges, 25, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 20, interval: 10 });
    });

    it('should return gap after last coverage', () => {
      const ranges: Range[] = [{ position: 10, interval: 10 }];
      const result = findGapAtPosition(ranges, 50, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 20, interval: 80 });
    });

    it('should return null when position is in coverage', () => {
      const ranges: Range[] = [{ position: 40, interval: 20 }];
      const result = findGapAtPosition(ranges, 50, zoomStart, zoomEnd);

      expect(result).toBeNull();
    });

    it('should merge overlapping ranges before finding gap', () => {
      const ranges: Range[] = [
        { position: 10, interval: 10 },
        { position: 15, interval: 10 }, // Overlaps, merges to position:10, interval:15
        { position: 40, interval: 10 },
      ];
      const result = findGapAtPosition(ranges, 30, zoomStart, zoomEnd);

      // Gap should be from 25 (end of merged range) to 40 (start of next)
      expect(result).toEqual({ position: 25, interval: 15 });
    });

    it('should handle position at exact gap start', () => {
      const ranges: Range[] = [
        { position: 10, interval: 10 },
        { position: 30, interval: 10 },
      ];
      const result = findGapAtPosition(ranges, 20, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 20, interval: 10 });
    });

    it('should handle position at exact gap end', () => {
      const ranges: Range[] = [
        { position: 10, interval: 10 },
        { position: 30, interval: 10 },
      ];
      const result = findGapAtPosition(ranges, 29, zoomStart, zoomEnd);

      expect(result).toEqual({ position: 20, interval: 10 });
    });

    it('should handle multiple gaps and find correct one', () => {
      const ranges: Range[] = [
        { position: 0, interval: 10 },
        { position: 20, interval: 10 },
        { position: 40, interval: 10 },
        { position: 60, interval: 10 },
      ];

      // Gap 1: 10-20
      expect(findGapAtPosition(ranges, 15, zoomStart, zoomEnd)).toEqual({
        position: 10,
        interval: 10,
      });

      // Gap 2: 30-40
      expect(findGapAtPosition(ranges, 35, zoomStart, zoomEnd)).toEqual({
        position: 30,
        interval: 10,
      });

      // Gap 3: 50-60
      expect(findGapAtPosition(ranges, 55, zoomStart, zoomEnd)).toEqual({
        position: 50,
        interval: 10,
      });

      // Gap 4: 70-100
      expect(findGapAtPosition(ranges, 80, zoomStart, zoomEnd)).toEqual({
        position: 70,
        interval: 30,
      });
    });

    it('should respect zoom boundaries', () => {
      const ranges: Range[] = [{ position: 50, interval: 10 }];

      // Gap before coverage uses zoomStart
      const beforeGap = findGapAtPosition(ranges, 25, 10, 200);
      expect(beforeGap).toEqual({ position: 10, interval: 40 });

      // Gap after coverage uses zoomEnd
      const afterGap = findGapAtPosition(ranges, 100, 10, 200);
      expect(afterGap).toEqual({ position: 60, interval: 140 });
    });
  });

  describe('real-world scenarios', () => {
    it('should handle incremental transformation coverage data', () => {
      const ranges: Range[] = [
        { position: 0, interval: 1000 },
        { position: 1000, interval: 1000 },
        { position: 3000, interval: 1000 },
      ];

      // Find coverage
      expect(findCoverageAtPosition(ranges, 500)).toEqual({ position: 0, interval: 1000 });
      expect(findCoverageAtPosition(ranges, 1500)).toEqual({ position: 1000, interval: 1000 });

      // Find gap
      const gap = findGapAtPosition(ranges, 2500, 0, 10000);
      expect(gap).toEqual({ position: 2000, interval: 1000 });

      // Merge for display
      const merged = mergeRanges(ranges);
      expect(merged).toEqual([
        { position: 0, interval: 2000 },
        { position: 3000, interval: 1000 },
      ]);
    });

    it('should handle fragmented coverage with many small ranges', () => {
      const ranges: Range[] = [];
      for (let i = 0; i < 100; i += 2) {
        ranges.push({ position: i * 10, interval: 10 });
      }

      // Should have 50 ranges with gaps between them
      const merged = mergeRanges(ranges);
      expect(merged.length).toBe(50); // No merging since all have gaps

      // Find a gap
      const gap = findGapAtPosition(ranges, 15, 0, 1000);
      expect(gap).toEqual({ position: 10, interval: 10 });
    });
  });
});
