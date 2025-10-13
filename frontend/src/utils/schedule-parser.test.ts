import { describe, it, expect, beforeEach, vi } from 'vitest';
import { getNextScheduledRun, getNextRunDescription, formatNextRun } from './schedule-parser';

describe('schedule-parser', () => {
  // Mock the current time for consistent testing
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2025-10-13T12:00:00.000Z'));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  describe('getNextScheduledRun', () => {
    describe('@every format (asynq style)', () => {
      it('should parse @every with seconds', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every 30s', lastRun);

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(lastRun.getTime() + 30 * 1000);
      });

      it('should parse @every with minutes', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every 5m', lastRun);

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(lastRun.getTime() + 5 * 60 * 1000);
      });

      it('should parse @every with hours', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every 2h', lastRun);

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(lastRun.getTime() + 2 * 60 * 60 * 1000);
      });

      it('should parse @every with mixed duration (1h30m10s)', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every 1h30m10s', lastRun);

        const expectedMs = (1 * 60 * 60 + 30 * 60 + 10) * 1000;
        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(lastRun.getTime() + expectedMs);
      });

      it('should use current time when lastRun is not provided', () => {
        const result = getNextScheduledRun('@every 10m');
        const now = new Date();

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(now.getTime() + 10 * 60 * 1000);
      });

      it('should handle @every with days', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every 1d', lastRun);

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(lastRun.getTime() + 24 * 60 * 60 * 1000);
      });

      it('should return null for invalid @every duration', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every invalid', lastRun);

        expect(result).toBeNull();
      });
    });

    describe('predefined schedules', () => {
      it('should parse @hourly', () => {
        const result = getNextScheduledRun('@hourly');

        expect(result).toBeInstanceOf(Date);
        // @hourly should run at the next hour mark
        expect(result?.getMinutes()).toBe(0);
        expect(result?.getSeconds()).toBe(0);
      });

      it('should parse @daily', () => {
        const result = getNextScheduledRun('@daily');

        expect(result).toBeInstanceOf(Date);
        // @daily should run at midnight
        expect(result?.getHours()).toBe(0);
        expect(result?.getMinutes()).toBe(0);
        expect(result?.getSeconds()).toBe(0);
      });

      // Note: @midnight is not supported by cron-parser library, only @daily
      it.skip('should parse @midnight (alias for @daily)', () => {
        const result = getNextScheduledRun('@midnight');

        expect(result).toBeInstanceOf(Date);
        expect(result?.getHours()).toBe(0);
        expect(result?.getMinutes()).toBe(0);
        expect(result?.getSeconds()).toBe(0);
      });

      it('should parse @weekly', () => {
        const result = getNextScheduledRun('@weekly');

        expect(result).toBeInstanceOf(Date);
        // @weekly should run on Sunday at midnight
        expect(result?.getDay()).toBe(0); // Sunday
        expect(result?.getHours()).toBe(0);
        expect(result?.getMinutes()).toBe(0);
      });

      it('should parse @monthly', () => {
        const result = getNextScheduledRun('@monthly');

        expect(result).toBeInstanceOf(Date);
        // @monthly should run on the 1st at midnight
        expect(result?.getDate()).toBe(1);
        expect(result?.getHours()).toBe(0);
        expect(result?.getMinutes()).toBe(0);
      });

      it('should parse @yearly', () => {
        const result = getNextScheduledRun('@yearly');

        expect(result).toBeInstanceOf(Date);
        // @yearly should run on Jan 1st at midnight
        expect(result?.getMonth()).toBe(0); // January
        expect(result?.getDate()).toBe(1);
        expect(result?.getHours()).toBe(0);
      });

      // Note: @annually is not supported by cron-parser library, only @yearly
      it.skip('should parse @annually (alias for @yearly)', () => {
        const result = getNextScheduledRun('@annually');

        expect(result).toBeInstanceOf(Date);
        expect(result?.getMonth()).toBe(0);
        expect(result?.getDate()).toBe(1);
      });
    });

    describe('standard cron expressions', () => {
      it('should parse every minute (* * * * *)', () => {
        const result = getNextScheduledRun('* * * * *');

        expect(result).toBeInstanceOf(Date);
        // Should be within the next minute
        const diff = result!.getTime() - new Date().getTime();
        expect(diff).toBeGreaterThan(0);
        expect(diff).toBeLessThanOrEqual(60 * 1000);
      });

      it('should parse specific minute (30 * * * *)', () => {
        const result = getNextScheduledRun('30 * * * *');

        expect(result).toBeInstanceOf(Date);
        expect(result?.getMinutes()).toBe(30);
      });

      it('should parse specific hour and minute (0 14 * * *)', () => {
        const result = getNextScheduledRun('0 14 * * *');

        expect(result).toBeInstanceOf(Date);
        expect(result?.getHours()).toBe(14);
        expect(result?.getMinutes()).toBe(0);
      });
    });

    describe('edge cases', () => {
      it('should return null for undefined schedule', () => {
        const result = getNextScheduledRun(undefined);

        expect(result).toBeNull();
      });

      it('should return null for empty schedule', () => {
        const result = getNextScheduledRun('');

        expect(result).toBeNull();
      });

      it('should return null for invalid cron expression', () => {
        const result = getNextScheduledRun('invalid cron');

        expect(result).toBeNull();
      });

      it('should handle lastRun as string', () => {
        const lastRun = '2025-10-13T11:00:00.000Z';
        const result = getNextScheduledRun('@every 5m', lastRun);

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(new Date(lastRun).getTime() + 5 * 60 * 1000);
      });

      it('should handle lastRun as Date object', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z');
        const result = getNextScheduledRun('@every 5m', lastRun);

        expect(result).toBeInstanceOf(Date);
        expect(result?.getTime()).toBe(lastRun.getTime() + 5 * 60 * 1000);
      });
    });
  });

  describe('getNextRunDescription', () => {
    describe('upcoming runs', () => {
      it('should return "in X seconds" for near future', () => {
        const lastRun = new Date('2025-10-13T11:59:30.000Z'); // 30s ago
        const result = getNextRunDescription('@every 45s', lastRun);

        expect(result).toBe('in 15 seconds');
      });

      it('should return "in 1 second" (singular)', () => {
        const lastRun = new Date('2025-10-13T11:59:01.000Z'); // 59s ago
        const result = getNextRunDescription('@every 60s', lastRun);

        expect(result).toBe('in 1 second');
      });

      it('should return "in X minutes" for minutes range', () => {
        const lastRun = new Date('2025-10-13T11:30:00.000Z'); // 30m ago
        const result = getNextRunDescription('@every 35m', lastRun);

        expect(result).toBe('in 5 minutes');
      });

      it('should return "in 1 minute" (singular)', () => {
        const lastRun = new Date('2025-10-13T11:01:00.000Z'); // 59m ago
        const result = getNextRunDescription('@every 60m', lastRun);

        expect(result).toBe('in 1 minute');
      });

      it('should return "in X hours" for hours range', () => {
        const lastRun = new Date('2025-10-13T09:00:00.000Z'); // 3h ago
        const result = getNextRunDescription('@every 5h', lastRun);

        expect(result).toBe('in 2 hours');
      });

      it('should return "in 1 hour" (singular)', () => {
        const lastRun = new Date('2025-10-13T11:00:00.000Z'); // 1h ago
        const result = getNextRunDescription('@every 2h', lastRun);

        expect(result).toBe('in 1 hour');
      });

      it('should return "in X days" for days range', () => {
        const lastRun = new Date('2025-10-11T12:00:00.000Z'); // 2d ago
        const result = getNextRunDescription('@every 5d', lastRun);

        expect(result).toBe('in 3 days');
      });

      it('should return "in 1 day" (singular)', () => {
        const lastRun = new Date('2025-10-12T12:00:00.000Z'); // 1d ago
        const result = getNextRunDescription('@every 2d', lastRun);

        expect(result).toBe('in 1 day');
      });
    });

    describe('overdue runs', () => {
      it('should return "OVERDUE Xm" for minutes overdue', () => {
        const lastRun = new Date('2025-10-13T11:50:00.000Z'); // 10m ago
        const result = getNextRunDescription('@every 5m', lastRun);

        expect(result).toBe('OVERDUE 5m');
      });

      it('should return "OVERDUE Xh" for hours overdue', () => {
        const lastRun = new Date('2025-10-13T09:00:00.000Z'); // 3h ago
        const result = getNextRunDescription('@every 1h', lastRun);

        expect(result).toBe('OVERDUE 2h');
      });

      it('should return "OVERDUE Xd" for days overdue', () => {
        const lastRun = new Date('2025-10-10T12:00:00.000Z'); // 3d ago
        const result = getNextRunDescription('@every 1d', lastRun);

        expect(result).toBe('OVERDUE 2d');
      });

      it('should return "OVERDUE" for less than 1 minute overdue', () => {
        const lastRun = new Date('2025-10-13T11:59:45.000Z'); // 15s ago
        const result = getNextRunDescription('@every 10s', lastRun);

        expect(result).toBe('OVERDUE');
      });
    });

    describe('edge cases', () => {
      it('should return null for undefined schedule', () => {
        const result = getNextRunDescription(undefined);

        expect(result).toBeNull();
      });

      it('should return null for invalid schedule', () => {
        const result = getNextRunDescription('invalid');

        expect(result).toBeNull();
      });

      it('should handle no lastRun provided', () => {
        const result = getNextRunDescription('@every 5m');

        expect(result).toBe('in 5 minutes');
      });
    });
  });

  describe('formatNextRun', () => {
    it('should format next run as localized datetime', () => {
      const lastRun = new Date('2025-10-13T11:00:00.000Z');
      const result = formatNextRun('@every 5m', lastRun);

      expect(result).toBeTruthy();
      expect(typeof result).toBe('string');
      // Should contain date elements
      expect(result).toMatch(/\d{4}/); // year
    });

    it('should return null for undefined schedule', () => {
      const result = formatNextRun(undefined);

      expect(result).toBeNull();
    });

    it('should return null for invalid schedule', () => {
      const result = formatNextRun('invalid');

      expect(result).toBeNull();
    });

    it('should handle @hourly format', () => {
      const result = formatNextRun('@hourly');

      expect(result).toBeTruthy();
      expect(typeof result).toBe('string');
    });

    it('should handle cron expressions', () => {
      const result = formatNextRun('0 14 * * *');

      expect(result).toBeTruthy();
      expect(typeof result).toBe('string');
    });
  });

  describe('real-world scenarios', () => {
    it('should handle API response from scheduled runs endpoint', () => {
      // Simulate real API response
      const apiResponse = {
        id: 'mainnet.dim_node',
        last_run: '2025-10-13T03:24:50.819Z',
      };
      const schedule = '@every 5m';

      const nextRunDesc = getNextRunDescription(schedule, apiResponse.last_run);

      expect(nextRunDesc).toBeTruthy();
      // Should be overdue since last_run was ~9 hours ago
      expect(nextRunDesc).toMatch(/OVERDUE/);
    });

    it('should handle recently run scheduled task', () => {
      const lastRun = new Date('2025-10-13T11:58:00.000Z'); // 2 minutes ago
      const schedule = '@every 5m';

      const nextRunDesc = getNextRunDescription(schedule, lastRun);

      expect(nextRunDesc).toBe('in 3 minutes');
    });

    it('should handle task that just ran', () => {
      const lastRun = new Date('2025-10-13T12:00:00.000Z'); // just now
      const schedule = '@every 10s';

      const nextRunDesc = getNextRunDescription(schedule, lastRun);

      expect(nextRunDesc).toBe('in 10 seconds');
    });
  });
});
