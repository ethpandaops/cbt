/**
 * Color palette and utilities for OR group badges
 * Used across DependencyRow and ModelCoverageRow components
 */

// Color palette for OR groups (cycles every 10) - subtle colors that blend with dark theme
export const OR_GROUP_COLORS = [
  { bg: 'bg-slate-600/30', text: 'text-slate-400', ring: 'ring-slate-600/40', hex: 'rgb(71, 85, 105)' }, // #1
  { bg: 'bg-emerald-600/25', text: 'text-emerald-400', ring: 'ring-emerald-600/30', hex: 'rgb(5, 150, 105)' }, // #2
  { bg: 'bg-sky-600/25', text: 'text-sky-400', ring: 'ring-sky-600/30', hex: 'rgb(2, 132, 199)' }, // #3
  { bg: 'bg-violet-600/25', text: 'text-violet-400', ring: 'ring-violet-600/30', hex: 'rgb(124, 58, 237)' }, // #4
  { bg: 'bg-rose-600/25', text: 'text-rose-400', ring: 'ring-rose-600/30', hex: 'rgb(225, 29, 72)' }, // #5
  { bg: 'bg-cyan-600/25', text: 'text-cyan-400', ring: 'ring-cyan-600/30', hex: 'rgb(8, 145, 178)' }, // #6
  { bg: 'bg-orange-600/25', text: 'text-orange-400', ring: 'ring-orange-600/30', hex: 'rgb(234, 88, 12)' }, // #7
  { bg: 'bg-lime-600/25', text: 'text-lime-400', ring: 'ring-lime-600/30', hex: 'rgb(101, 163, 13)' }, // #8
  { bg: 'bg-pink-600/25', text: 'text-pink-400', ring: 'ring-pink-600/30', hex: 'rgb(219, 39, 119)' }, // #9
  { bg: 'bg-teal-600/25', text: 'text-teal-400', ring: 'ring-teal-600/30', hex: 'rgb(13, 148, 136)' }, // #10
] as const;

/**
 * Get the color configuration for an OR group ID
 * Colors cycle every 10 groups
 */
export function getOrGroupColor(groupId: number): (typeof OR_GROUP_COLORS)[number] {
  return OR_GROUP_COLORS[(groupId - 1) % OR_GROUP_COLORS.length];
}
