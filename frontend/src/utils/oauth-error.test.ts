import { describe, expect, it } from 'vitest';
import { parseOAuthCallbackError, stripOAuthCallbackErrorParams } from './oauth-error';

describe('oauth-error utility', () => {
  it('returns null when callback has no error', () => {
    expect(parseOAuthCallbackError('?foo=bar')).toBeNull();
  });

  it('maps access_denied to a user-friendly message', () => {
    expect(parseOAuthCallbackError('?error=access_denied')).toEqual({
      code: 'access_denied',
      message: 'GitHub login was cancelled or denied.',
    });
  });

  it('includes normalized error_description for non-denial errors', () => {
    expect(parseOAuthCallbackError('?error=invalid_request&error_description=  bad%20request%20  ')).toEqual({
      code: 'invalid_request',
      message: 'GitHub login request was invalid. Please try again. bad request',
    });
  });

  it('falls back to a generic message for unknown errors', () => {
    expect(parseOAuthCallbackError('?error=weird_error')).toEqual({
      code: 'weird_error',
      message: 'GitHub login failed. Please try again.',
    });
  });

  it('removes oauth error params and preserves other query values', () => {
    expect(stripOAuthCallbackErrorParams('?error=access_denied&error_description=nope&foo=bar')).toBe('?foo=bar');
  });

  it('returns empty string when no query params remain', () => {
    expect(stripOAuthCallbackErrorParams('?error=access_denied')).toBe('');
  });
});
