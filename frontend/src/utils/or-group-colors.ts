/**
 * Color palette and utilities for OR group badges
 * Used across DependencyRow and ModelCoverageRow components
 */

// Color palette for OR groups (cycles every 10) - bright indigo/purple theme colors
export const OR_GROUP_COLORS = [
  { bg: 'bg-fuchsia-500/40', text: 'text-fuchsia-200', ring: 'ring-fuchsia-500/60', hex: 'rgb(217, 70, 239)' }, // #1
  { bg: 'bg-purple-500/40', text: 'text-purple-200', ring: 'ring-purple-500/60', hex: 'rgb(168, 85, 247)' }, // #2
  { bg: 'bg-indigo-500/40', text: 'text-indigo-200', ring: 'ring-indigo-500/60', hex: 'rgb(99, 102, 241)' }, // #3
  { bg: 'bg-violet-500/40', text: 'text-violet-200', ring: 'ring-violet-500/60', hex: 'rgb(139, 92, 246)' }, // #4
  { bg: 'bg-pink-500/40', text: 'text-pink-200', ring: 'ring-pink-500/60', hex: 'rgb(236, 72, 153)' }, // #5
  { bg: 'bg-rose-500/40', text: 'text-rose-200', ring: 'ring-rose-500/60', hex: 'rgb(244, 63, 94)' }, // #6
  { bg: 'bg-sky-500/40', text: 'text-sky-200', ring: 'ring-sky-500/60', hex: 'rgb(14, 165, 233)' }, // #7
  { bg: 'bg-cyan-500/40', text: 'text-cyan-200', ring: 'ring-cyan-500/60', hex: 'rgb(6, 182, 212)' }, // #8
  { bg: 'bg-blue-500/40', text: 'text-blue-200', ring: 'ring-blue-500/60', hex: 'rgb(59, 130, 246)' }, // #9
  { bg: 'bg-emerald-500/40', text: 'text-emerald-200', ring: 'ring-emerald-500/60', hex: 'rgb(16, 185, 129)' }, // #10
] as const;

/**
 * Get the color configuration for an OR group ID
 * Colors cycle every 10 groups
 */
export function getOrGroupColor(groupId: number): (typeof OR_GROUP_COLORS)[number] {
  return OR_GROUP_COLORS[(groupId - 1) % OR_GROUP_COLORS.length];
}
