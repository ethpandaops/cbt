import { describe, expect, it } from 'vitest';
import { reverseTransformValue, transformValue } from './interval-transform';
import type { IntervalTypeTransformation } from '@/api/types.gen';

describe('reverseTransformValue', () => {
  it('returns targetValue unchanged when no expression', () => {
    const transformation: IntervalTypeTransformation = { name: 'Raw' };
    expect(reverseTransformValue(12345, transformation)).toBe(12345);
  });

  it('reverses value * 1000 (Datetime transformation)', () => {
    const transformation: IntervalTypeTransformation = {
      name: 'Datetime',
      expression: 'value * 1000',
      format: 'datetime',
    };
    const target = 1_700_000_000_000;
    const result = reverseTransformValue(target, transformation);
    expect(result).toBe(1_700_000_000);
    // Verify round-trip
    expect(transformValue(result, transformation)).toBe(target);
  });

  it('reverses math.floor((value - 1606824023) / 12) (Slot transformation)', () => {
    const transformation: IntervalTypeTransformation = {
      name: 'Slot',
      expression: 'math.floor((value - 1606824023) / 12)',
    };
    // Known: timestamp 1606824023 + 12 * slot = raw timestamp
    const knownSlot = 100000;
    const rawTimestamp = 1606824023 + 12 * knownSlot;
    // Forward: transformValue(rawTimestamp) should give knownSlot
    expect(transformValue(rawTimestamp, transformation)).toBe(knownSlot);
    // Reverse: reverseTransformValue(knownSlot) should give rawTimestamp
    const reversed = reverseTransformValue(knownSlot, transformation);
    // The floor in the expression means multiple raw values map to the same slot.
    // The reverse should find the first raw value that maps to the target slot.
    expect(transformValue(reversed, transformation)).toBe(knownSlot);
  });
});
