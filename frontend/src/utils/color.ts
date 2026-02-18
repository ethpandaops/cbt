/**
 * Colour Utilities
 *
 * Helper functions for working with CSS colors, including modern color formats like oklch.
 * Uses culori library for robust color parsing and conversion.
 */

import { formatHex } from 'culori';

/**
 * Resolve any CSS color to hex format
 *
 * Converts modern CSS colors (oklch, color-mix, etc.) to hex format
 * that libraries like ReactFlow can understand. Uses culori for robust
 * color parsing across all modern CSS color formats.
 *
 * @param color - Any valid CSS color string
 * @param fallback - Fallback color if resolution fails
 * @returns Hex color string (e.g., '#06b6d4')
 */
export function resolveCssColorToHex(color: string, fallback = '#000000'): string {
  // If already a valid 6-digit hex color, return as-is
  if (/^#[0-9A-Fa-f]{6}$/.test(color)) {
    return color;
  }

  // Create a temporary element to resolve the color via browser's computed styles
  const temp = document.createElement('div');
  temp.style.color = color;

  // Append to body to ensure computed styles work
  document.body.appendChild(temp);

  // Get the computed color value (browser resolves oklch, color-mix, etc. to rgb)
  const computedColor = window.getComputedStyle(temp).color;

  // Clean up
  document.body.removeChild(temp);

  // Use culori to parse any CSS color format and convert to hex
  const hexColor = formatHex(computedColor);

  if (!hexColor) {
    console.warn(
      `[resolveCssColorToHex] Failed to resolve CSS color "${color}". ` +
        `Computed color: "${computedColor}". Falling back to ${fallback}.`
    );
    return fallback;
  }

  return hexColor;
}

/**
 * Convert hex color to rgba format with alpha channel
 *
 * Useful for canvas operations (like ReactFlow edge gradients) that require rgba format
 *
 * @param color - Any CSS color (will be resolved to hex first)
 * @param alpha - Alpha/opacity value between 0 (transparent) and 1 (opaque)
 * @param fallback - Fallback hex color if resolution fails
 * @returns RGBA color string (e.g., 'rgba(6, 182, 212, 0.5)')
 */
export function hexToRgba(color: string, alpha: number, fallback = '#06b6d4'): string {
  const clampedAlpha = Math.max(0, Math.min(1, alpha));
  const hex = resolveCssColorToHex(color, fallback);

  const r = parseInt(hex.slice(1, 3), 16);
  const g = parseInt(hex.slice(3, 5), 16);
  const b = parseInt(hex.slice(5, 7), 16);

  return `rgba(${r}, ${g}, ${b}, ${clampedAlpha})`;
}
