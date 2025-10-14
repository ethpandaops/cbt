import { describe, it, expect } from 'vitest';
import { calculateDefaultZoomRange } from './zoom-helpers';

describe('zoom-helpers', () => {
  describe('calculateDefaultZoomRange', () => {
    it('should calculate default zoom for typical data range', () => {
      const transformationMin = 5000;
      const transformationMax = 10000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(10000 - 86400, 5000)) = max(0, max(-76400, 5000)) = max(0, 5000) = 5000
      expect(result).toEqual({
        start: 5000,
        end: 10000,
      });
    });

    it('should calculate default zoom when data range is larger than window', () => {
      const transformationMin = 0;
      const transformationMax = 100000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(100000 - 86400, 0)) = max(0, max(13600, 0)) = max(0, 13600) = 13600
      expect(result).toEqual({
        start: 13600,
        end: 100000,
      });
    });

    it('should use transformationMin when it is greater than calculated min', () => {
      const transformationMin = 50000;
      const transformationMax = 60000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(60000 - 86400, 50000)) = max(0, max(-26400, 50000)) = max(0, 50000) = 50000
      expect(result).toEqual({
        start: 50000,
        end: 60000,
      });
    });

    it('should clamp negative calculated min to 0', () => {
      const transformationMin = -5000;
      const transformationMax = 1000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(1000 - 86400, -5000)) = max(0, max(-85400, -5000)) = max(0, -5000) = 0
      expect(result).toEqual({
        start: 0,
        end: 1000,
      });
    });

    it('should handle edge case where transformationMin equals transformationMax', () => {
      const transformationMin = 5000;
      const transformationMax = 5000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(5000 - 86400, 5000)) = max(0, max(-81400, 5000)) = max(0, 5000) = 5000
      expect(result).toEqual({
        start: 5000,
        end: 5000,
      });
    });

    it('should handle zero values', () => {
      const transformationMin = 0;
      const transformationMax = 0;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(0 - 86400, 0)) = max(0, max(-86400, 0)) = max(0, 0) = 0
      expect(result).toEqual({
        start: 0,
        end: 0,
      });
    });

    it('should handle very large values', () => {
      const transformationMin = 1000000;
      const transformationMax = 2000000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(2000000 - 86400, 1000000)) = max(0, max(1913600, 1000000)) = max(0, 1913600) = 1913600
      expect(result).toEqual({
        start: 1913600,
        end: 2000000,
      });
    });

    it('should use exactly window size when data is larger', () => {
      const transformationMin = 0;
      const transformationMax = 200000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Window size is 86400, so start should be 200000 - 86400 = 113600
      expect(result.start).toBe(113600);
      expect(result.end).toBe(200000);
      expect(result.end - result.start).toBe(86400);
    });

    it('should handle small data range within window size', () => {
      const transformationMin = 100;
      const transformationMax = 1000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Data range (900) is much smaller than window (86400)
      // Expected: max(0, max(1000 - 86400, 100)) = max(0, max(-85400, 100)) = max(0, 100) = 100
      expect(result).toEqual({
        start: 100,
        end: 1000,
      });
    });
  });

  describe('real-world scenarios', () => {
    it('should handle typical Ethereum slot data (recent blocks)', () => {
      // Ethereum slot numbers around 9 million
      const transformationMin = 9000000;
      const transformationMax = 9100000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Expected: max(0, max(9100000 - 86400, 9000000)) = max(0, max(9013600, 9000000)) = 9013600
      expect(result).toEqual({
        start: 9013600,
        end: 9100000,
      });
      expect(result.end - result.start).toBe(86400);
    });

    it('should handle epoch data (smaller numbers)', () => {
      const transformationMin = 0;
      const transformationMax = 300000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Shows last 86400 epochs
      expect(result).toEqual({
        start: 213600,
        end: 300000,
      });
    });

    it('should handle new deployment with limited data', () => {
      const transformationMin = 0;
      const transformationMax = 1000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Should show all data since it's less than window size
      expect(result).toEqual({
        start: 0,
        end: 1000,
      });
    });

    it('should handle negative min bounds correctly (clamped to 0)', () => {
      const transformationMin = -100;
      const transformationMax = 50000;

      const result = calculateDefaultZoomRange(transformationMin, transformationMax);

      // Calculated would be negative, should clamp to 0
      expect(result.start).toBe(0);
      expect(result.start).toBeGreaterThanOrEqual(0);
    });
  });
});
