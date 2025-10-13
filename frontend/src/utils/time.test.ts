import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { timeAgo } from './time';

describe('time utility', () => {
  describe('timeAgo', () => {
    beforeEach(() => {
      // Mock the current time to 2025-10-13T12:00:00.000Z
      vi.useFakeTimers();
      vi.setSystemTime(new Date('2025-10-13T12:00:00.000Z'));
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    describe('null/undefined handling', () => {
      it('should return "Never" for null timestamp', () => {
        expect(timeAgo(null)).toBe('Never');
      });

      it('should return "Never" for undefined timestamp', () => {
        expect(timeAgo(undefined)).toBe('Never');
      });
    });

    describe('seconds ago', () => {
      it('should return "just now" for 0 seconds ago', () => {
        const timestamp = new Date('2025-10-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('just now');
      });

      it('should return "just now" for 1 second ago', () => {
        const timestamp = new Date('2025-10-13T11:59:59.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('just now');
      });

      it('should return "2 secs ago" for 2 seconds ago', () => {
        const timestamp = new Date('2025-10-13T11:59:58.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 secs ago');
      });

      it('should return "59 secs ago" for 59 seconds ago', () => {
        const timestamp = new Date('2025-10-13T11:59:01.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('59 secs ago');
      });
    });

    describe('minutes ago', () => {
      it('should return "1 min ago" for 1 minute ago', () => {
        const timestamp = new Date('2025-10-13T11:59:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 min ago');
      });

      it('should return "2 mins ago" for 2 minutes ago', () => {
        const timestamp = new Date('2025-10-13T11:58:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 mins ago');
      });

      it('should return "59 mins ago" for 59 minutes ago', () => {
        const timestamp = new Date('2025-10-13T11:01:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('59 mins ago');
      });
    });

    describe('hours ago', () => {
      it('should return "1 hour ago" for 1 hour ago', () => {
        const timestamp = new Date('2025-10-13T11:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 hour ago');
      });

      it('should return "2 hours ago" for 2 hours ago', () => {
        const timestamp = new Date('2025-10-13T10:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 hours ago');
      });

      it('should return "23 hours ago" for 23 hours ago', () => {
        // 23 hours before 2025-10-13T12:00:00.000Z = 2025-10-12T13:00:00.000Z
        const timestamp = new Date('2025-10-12T13:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('23 hours ago');
      });
    });

    describe('days ago', () => {
      it('should return "1 day ago" for 1 day ago', () => {
        const timestamp = new Date('2025-10-12T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 day ago');
      });

      it('should return "2 days ago" for 2 days ago', () => {
        const timestamp = new Date('2025-10-11T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 days ago');
      });

      it('should return "6 days ago" for 6 days ago', () => {
        const timestamp = new Date('2025-10-07T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('6 days ago');
      });
    });

    describe('weeks ago', () => {
      it('should return "1 week ago" for 1 week ago', () => {
        const timestamp = new Date('2025-10-06T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 week ago');
      });

      it('should return "2 weeks ago" for 2 weeks ago', () => {
        const timestamp = new Date('2025-09-29T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 weeks ago');
      });

      it('should return "3 weeks ago" for 3 weeks ago', () => {
        const timestamp = new Date('2025-09-22T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('3 weeks ago');
      });
    });

    describe('months ago', () => {
      it('should return "1 month ago" for 1 month ago', () => {
        const timestamp = new Date('2025-09-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 month ago');
      });

      it('should return "2 months ago" for 2 months ago', () => {
        const timestamp = new Date('2025-08-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 months ago');
      });

      it('should return "11 months ago" for 11 months ago', () => {
        const timestamp = new Date('2024-11-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('11 months ago');
      });
    });

    describe('years ago', () => {
      it('should return "1 year ago" for 1 year ago', () => {
        const timestamp = new Date('2024-10-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 year ago');
      });

      it('should return "2 years ago" for 2 years ago', () => {
        const timestamp = new Date('2023-10-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('2 years ago');
      });

      it('should return "10 years ago" for 10 years ago', () => {
        const timestamp = new Date('2015-10-13T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('10 years ago');
      });
    });

    describe('edge cases', () => {
      it('should handle timestamps in the future (treat as just now)', () => {
        const timestamp = new Date('2025-10-13T12:01:00.000Z').toISOString();
        // Future timestamps will result in negative diff, which floors to 0
        expect(timeAgo(timestamp)).toBe('just now');
      });

      it('should handle timestamps as Date strings', () => {
        const timestamp = '2025-10-13T11:00:00.000Z';
        expect(timeAgo(timestamp)).toBe('1 hour ago');
      });

      it('should handle timestamps with different formats', () => {
        const timestamp = new Date('2025-10-13T11:00:00Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 hour ago');
      });
    });

    describe('real-world scenarios', () => {
      it('should format recent API response timestamp', () => {
        // Simulate API response from 5 minutes ago
        const timestamp = new Date('2025-10-13T11:55:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('5 mins ago');
      });

      it('should format last run timestamp from yesterday', () => {
        const timestamp = new Date('2025-10-12T12:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('1 day ago');
      });

      it('should format old historical timestamp', () => {
        const timestamp = new Date('2020-01-01T00:00:00.000Z').toISOString();
        expect(timeAgo(timestamp)).toBe('5 years ago');
      });
    });
  });
});
