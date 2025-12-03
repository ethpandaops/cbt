/**
 * Convert a timestamp to relative time format (e.g., "2 mins ago")
 */
export function timeAgo(timestamp: string | null | undefined): string {
  if (!timestamp) return 'Never';

  const now = new Date();
  const past = new Date(timestamp);
  const diffMs = now.getTime() - past.getTime();

  // Handle future timestamps (negative diff means timestamp is in the future)
  if (diffMs < 0) {
    const futureSecs = Math.floor(Math.abs(diffMs) / 1000);
    const futureMins = Math.floor(futureSecs / 60);
    const futureHours = Math.floor(futureMins / 60);
    const futureDays = Math.floor(futureHours / 24);

    if (futureDays > 0) {
      return futureDays === 1 ? 'in 1 day' : `in ${futureDays} days`;
    } else if (futureHours > 0) {
      return futureHours === 1 ? 'in 1 hour' : `in ${futureHours} hours`;
    } else if (futureMins > 0) {
      return futureMins === 1 ? 'in 1 min' : `in ${futureMins} mins`;
    } else {
      return futureSecs <= 1 ? 'in a moment' : `in ${futureSecs} secs`;
    }
  }

  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);
  const diffWeeks = Math.floor(diffDays / 7);
  const diffMonths = Math.floor(diffDays / 30);
  const diffYears = Math.floor(diffDays / 365);

  if (diffSecs < 60) {
    return diffSecs <= 1 ? 'just now' : `${diffSecs} secs ago`;
  } else if (diffMins < 60) {
    return diffMins === 1 ? '1 min ago' : `${diffMins} mins ago`;
  } else if (diffHours < 24) {
    return diffHours === 1 ? '1 hour ago' : `${diffHours} hours ago`;
  } else if (diffDays < 7) {
    return diffDays === 1 ? '1 day ago' : `${diffDays} days ago`;
  } else if (diffWeeks < 4) {
    return diffWeeks === 1 ? '1 week ago' : `${diffWeeks} weeks ago`;
  } else if (diffMonths < 12) {
    return diffMonths === 1 ? '1 month ago' : `${diffMonths} months ago`;
  } else {
    return diffYears === 1 ? '1 year ago' : `${diffYears} years ago`;
  }
}
