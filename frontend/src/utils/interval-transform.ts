import { evaluate } from 'cel-js';
import type { IntervalTypeTransformation } from '@api/types.gen';

// Math functions to be available in CEL expressions (without namespace prefix)
const mathFunctions = {
  floor: Math.floor,
  ceil: Math.ceil,
  round: Math.round,
  abs: Math.abs,
  sqrt: Math.sqrt,
  least: Math.min,
  greatest: Math.max,
};

/**
 * Preprocess CEL expression to remove math namespace prefix
 * cel-js doesn't support namespaced functions, so we convert math.floor() to floor()
 */
function preprocessExpression(expression: string): string {
  return expression.replace(/math\.(floor|ceil|round|abs|sqrt|least|greatest)/g, '$1');
}

/**
 * Transform a value using a CEL expression from an interval type transformation
 * @param value - The input value to transform
 * @param transformation - The transformation configuration with optional CEL expression
 * @returns The transformed value, or the original value if no expression is provided
 */
export function transformValue(value: number, transformation: IntervalTypeTransformation): number {
  // If no expression, return the value unchanged
  if (!transformation.expression) {
    return value;
  }

  try {
    // Preprocess the expression to remove math namespace
    const processedExpression = preprocessExpression(transformation.expression);

    // Evaluate the expression with 'value' as the input variable and math functions
    const result = evaluate(processedExpression, { value }, mathFunctions);

    // Ensure result is a number
    if (typeof result !== 'number') {
      console.error('CEL expression did not return a number:', transformation.expression, result);
      return value;
    }

    return result;
  } catch (error) {
    console.error('Error evaluating CEL expression:', transformation.expression, error);
    return value;
  }
}

/**
 * Format a transformed value based on the format hint
 * @param value - The value to format
 * @param format - The format hint (datetime, date, time, duration, relative)
 * @returns Formatted string representation
 */
export function formatValue(value: number, format?: string): string {
  if (!format) {
    return value.toLocaleString();
  }

  switch (format) {
    case 'datetime': {
      // Assume value is a Unix timestamp in milliseconds
      const date = new Date(value);
      return date.toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      });
    }
    case 'date': {
      const date = new Date(value);
      return date.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
    }
    case 'time': {
      const date = new Date(value);
      return date.toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      });
    }
    case 'duration': {
      // Format as human-readable duration
      const seconds = Math.floor(value / 1000);
      const minutes = Math.floor(seconds / 60);
      const hours = Math.floor(minutes / 60);
      const days = Math.floor(hours / 24);

      if (days > 0) return `${days}d ${hours % 24}h`;
      if (hours > 0) return `${hours}h ${minutes % 60}m`;
      if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
      return `${seconds}s`;
    }
    case 'relative': {
      // Format as relative time
      const now = Date.now();
      const diff = now - value;
      const seconds = Math.floor(diff / 1000);
      const minutes = Math.floor(seconds / 60);
      const hours = Math.floor(minutes / 60);
      const days = Math.floor(hours / 24);

      if (days > 0) return `${days} day${days > 1 ? 's' : ''} ago`;
      if (hours > 0) return `${hours} hour${hours > 1 ? 's' : ''} ago`;
      if (minutes > 0) return `${minutes} minute${minutes > 1 ? 's' : ''} ago`;
      return `${seconds} second${seconds > 1 ? 's' : ''} ago`;
    }
    default:
      return value.toLocaleString();
  }
}
