const oauthErrorMessages: Record<string, string> = {
  access_denied: 'GitHub login was cancelled or denied.',
  invalid_request: 'GitHub login request was invalid. Please try again.',
  invalid_grant: 'GitHub login session expired. Please try again.',
  temporarily_unavailable: 'GitHub login is temporarily unavailable. Please try again.',
  server_error: 'GitHub login failed due to a server error. Please try again.',
};

export interface OAuthCallbackError {
  code: string;
  message: string;
}

function normalizeDescription(raw: string): string {
  return raw.replace(/\s+/g, ' ').trim();
}

/**
 * Parse OAuth callback query parameters and return a user-friendly error.
 */
export function parseOAuthCallbackError(search: string): OAuthCallbackError | null {
  const params = new URLSearchParams(search);
  const code = params.get('error');

  if (!code) {
    return null;
  }

  const fallbackMessage = oauthErrorMessages[code] ?? 'GitHub login failed. Please try again.';
  const rawDescription = normalizeDescription(params.get('error_description') ?? '');

  // Avoid duplicating generic denial copy; include description for other errors when present.
  const message =
    rawDescription !== '' && code !== 'access_denied' ? `${fallbackMessage} ${rawDescription}` : fallbackMessage;

  return { code, message };
}

/**
 * Remove OAuth callback error params from a query string.
 */
export function stripOAuthCallbackErrorParams(search: string): string {
  const params = new URLSearchParams(search);

  params.delete('error');
  params.delete('error_description');
  params.delete('error_uri');

  const next = params.toString();
  return next === '' ? '' : `?${next}`;
}
