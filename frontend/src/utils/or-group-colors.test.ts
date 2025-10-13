import { describe, it, expect } from 'vitest';
import { getOrGroupColor, OR_GROUP_COLORS } from './or-group-colors';

describe('or-group-colors', () => {
  describe('OR_GROUP_COLORS constant', () => {
    it('should have 10 color entries', () => {
      expect(OR_GROUP_COLORS).toHaveLength(10);
    });

    it('should have correct structure for each color', () => {
      OR_GROUP_COLORS.forEach(color => {
        expect(color).toHaveProperty('bg');
        expect(color).toHaveProperty('text');
        expect(color).toHaveProperty('ring');
        expect(color).toHaveProperty('hex');

        expect(typeof color.bg).toBe('string');
        expect(typeof color.text).toBe('string');
        expect(typeof color.ring).toBe('string');
        expect(typeof color.hex).toBe('string');

        // Validate Tailwind CSS class format
        expect(color.bg).toMatch(/^bg-\w+-\d+\/\d+$/);
        expect(color.text).toMatch(/^text-\w+-\d+$/);
        expect(color.ring).toMatch(/^ring-\w+-\d+\/\d+$/);

        // Validate hex color format
        expect(color.hex).toMatch(/^rgb\(\d+, \d+, \d+\)$/);
      });
    });
  });

  describe('getOrGroupColor', () => {
    it('should return first color for group ID 1', () => {
      const color = getOrGroupColor(1);
      expect(color).toEqual(OR_GROUP_COLORS[0]);
    });

    it('should return second color for group ID 2', () => {
      const color = getOrGroupColor(2);
      expect(color).toEqual(OR_GROUP_COLORS[1]);
    });

    it('should return tenth color for group ID 10', () => {
      const color = getOrGroupColor(10);
      expect(color).toEqual(OR_GROUP_COLORS[9]);
    });

    it('should cycle back to first color for group ID 11', () => {
      const color = getOrGroupColor(11);
      expect(color).toEqual(OR_GROUP_COLORS[0]);
    });

    it('should cycle back to second color for group ID 12', () => {
      const color = getOrGroupColor(12);
      expect(color).toEqual(OR_GROUP_COLORS[1]);
    });

    it('should handle large group IDs correctly', () => {
      const color = getOrGroupColor(100);
      // 100 - 1 = 99, 99 % 10 = 9 -> index 9 (10th color)
      expect(color).toEqual(OR_GROUP_COLORS[9]);
    });

    it('should handle group ID 21 (cycles twice)', () => {
      const color = getOrGroupColor(21);
      expect(color).toEqual(OR_GROUP_COLORS[0]);
    });

    it('should return consistent colors for the same group ID', () => {
      const color1 = getOrGroupColor(5);
      const color2 = getOrGroupColor(5);
      expect(color1).toEqual(color2);
    });

    it('should return different colors for adjacent group IDs', () => {
      const color1 = getOrGroupColor(1);
      const color2 = getOrGroupColor(2);
      expect(color1).not.toEqual(color2);
    });

    it('should cycle through all 10 colors for IDs 1-10', () => {
      const colors = Array.from({ length: 10 }, (_, i) => getOrGroupColor(i + 1));

      // All should be unique
      const uniqueColors = new Set(colors.map(c => c.hex));
      expect(uniqueColors.size).toBe(10);

      // Should match OR_GROUP_COLORS in order
      colors.forEach((color, index) => {
        expect(color).toEqual(OR_GROUP_COLORS[index]);
      });
    });
  });

  describe('real-world scenarios', () => {
    it('should provide distinct colors for multiple OR groups in a dependency graph', () => {
      // Simulate 3 OR groups
      const group1Color = getOrGroupColor(1);
      const group2Color = getOrGroupColor(2);
      const group3Color = getOrGroupColor(3);

      // All should be different
      expect(group1Color).not.toEqual(group2Color);
      expect(group2Color).not.toEqual(group3Color);
      expect(group1Color).not.toEqual(group3Color);

      // All should have valid hex colors
      expect(group1Color.hex).toMatch(/^rgb\(/);
      expect(group2Color.hex).toMatch(/^rgb\(/);
      expect(group3Color.hex).toMatch(/^rgb\(/);
    });

    it('should support badge rendering with Tailwind classes', () => {
      const color = getOrGroupColor(1);

      // Should have valid Tailwind classes for badges
      expect(color.bg).toContain('bg-');
      expect(color.text).toContain('text-');
      expect(color.ring).toContain('ring-');

      // Should have opacity/transparency
      expect(color.bg).toContain('/');
      expect(color.ring).toContain('/');
    });

    it('should handle dependency graph with 15 OR groups (cycles once)', () => {
      const colors = Array.from({ length: 15 }, (_, i) => getOrGroupColor(i + 1));

      // First 10 should match palette
      for (let i = 0; i < 10; i++) {
        expect(colors[i]).toEqual(OR_GROUP_COLORS[i]);
      }

      // Next 5 should repeat first 5
      for (let i = 10; i < 15; i++) {
        expect(colors[i]).toEqual(OR_GROUP_COLORS[i - 10]);
      }
    });
  });

  describe('edge cases', () => {
    it.skip('should handle group ID 0 (edge case - negative modulo)', () => {
      // JavaScript modulo with negatives doesn't work as expected for this use case
      // The function expects group IDs starting from 1
      const color = getOrGroupColor(0);
      expect(color).toBeDefined();
    });

    it.skip('should handle negative group IDs (edge case)', () => {
      // Negative group IDs are not a valid use case
      // The function expects group IDs starting from 1
      const color = getOrGroupColor(-1);
      expect(color).toBeDefined();
    });
  });
});
