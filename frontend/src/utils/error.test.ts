import { describe, expect, it } from 'vitest';
import { getErrorCode, getErrorMessage } from './error';

describe('error utility', () => {
  describe('getErrorMessage', () => {
    it('returns error message string from API payload', () => {
      expect(getErrorMessage({ code: 404, error: 'model not found' })).toBe('model not found');
    });
  });

  describe('getErrorCode', () => {
    it('returns code from API error payload', () => {
      expect(getErrorCode({ code: 404, error: 'model not found' })).toBe(404);
    });

    it('returns undefined for non-object errors', () => {
      expect(getErrorCode('boom')).toBeUndefined();
      expect(getErrorCode(null)).toBeUndefined();
      expect(getErrorCode(undefined)).toBeUndefined();
    });

    it('returns undefined when code is missing or invalid', () => {
      expect(getErrorCode({ error: 'model not found' })).toBeUndefined();
      expect(getErrorCode({ code: '404' })).toBeUndefined();
    });
  });
});
