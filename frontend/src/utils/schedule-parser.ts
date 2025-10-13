import { parseExpression } from 'cron-parser';
import parseDuration from 'parse-duration';

/**
 * Parse schedule string and calculate next run time
 * Supports both asynq @every syntax (e.g., "@every 30m") and standard cron expressions
 *
 * @param schedule - Schedule string in either @every or cron format
 * @param lastRun - Last run time (for @every schedules, next run = lastRun + duration)
 * @returns Next run time as Date, or null if parsing fails
 */
export function getNextScheduledRun(schedule: string | undefined, lastRun?: string | Date): Date | null {
  if (!schedule) {
    return null;
  }

  try {
    // Check if it's an @every schedule (asynq format)
    if (schedule.startsWith('@every ')) {
      const durationStr = schedule.substring(7).trim(); // Remove '@every '
      const durationMs = parseDuration(durationStr);

      if (durationMs === null || durationMs === undefined) {
        console.warn(`Failed to parse duration: ${durationStr}`);
        return null;
      }

      // For @every, calculate from last run time (or now if no last run)
      const baseTime = lastRun ? new Date(lastRun) : new Date();
      return new Date(baseTime.getTime() + durationMs);
    }

    // Otherwise, treat as cron expression (calculate from now)
    const interval = parseExpression(schedule, {
      currentDate: new Date(),
    });

    return interval.next().toDate();
  } catch (error) {
    console.warn(`Failed to parse schedule "${schedule}":`, error);
    return null;
  }
}

/**
 * Get a human-readable description of when the next run will occur
 *
 * @param schedule - Schedule string in either @every or cron format
 * @param lastRun - Last run time (for @every schedules)
 * @returns Human-readable string like "in 5 minutes", "OVERDUE" or null if parsing fails
 */
export function getNextRunDescription(schedule: string | undefined, lastRun?: string | Date): string | null {
  const nextRun = getNextScheduledRun(schedule, lastRun);

  if (!nextRun) {
    return null;
  }

  const now = new Date().getTime();
  const next = nextRun.getTime();
  const diffMs = next - now;

  // If in the past, show how overdue it is
  if (diffMs < 0) {
    const overdueSecs = Math.floor(Math.abs(diffMs) / 1000);
    const overdueMins = Math.floor(overdueSecs / 60);
    const overdueHours = Math.floor(overdueMins / 60);
    const overdueDays = Math.floor(overdueHours / 24);

    if (overdueDays > 0) {
      return `OVERDUE by ${overdueDays} day${overdueDays !== 1 ? 's' : ''}`;
    }
    if (overdueHours > 0) {
      return `OVERDUE by ${overdueHours} hour${overdueHours !== 1 ? 's' : ''}`;
    }
    if (overdueMins > 0) {
      return `OVERDUE by ${overdueMins} minute${overdueMins !== 1 ? 's' : ''}`;
    }
    if (overdueSecs > 0) {
      return `OVERDUE by ${overdueSecs} second${overdueSecs !== 1 ? 's' : ''}`;
    }
    return 'OVERDUE';
  }

  // Convert to human-readable format
  const seconds = Math.floor(diffMs / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (days > 0) {
    return `in ${days} day${days !== 1 ? 's' : ''}`;
  }
  if (hours > 0) {
    return `in ${hours} hour${hours !== 1 ? 's' : ''}`;
  }
  if (minutes > 0) {
    return `in ${minutes} minute${minutes !== 1 ? 's' : ''}`;
  }
  return `in ${seconds} second${seconds !== 1 ? 's' : ''}`;
}

/**
 * Format next run time as a localized datetime string
 *
 * @param schedule - Schedule string in either @every or cron format
 * @param lastRun - Last run time (for @every schedules)
 * @returns Formatted datetime string or null if parsing fails
 */
export function formatNextRun(schedule: string | undefined, lastRun?: string | Date): string | null {
  const nextRun = getNextScheduledRun(schedule, lastRun);

  if (!nextRun) {
    return null;
  }

  return nextRun.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}
