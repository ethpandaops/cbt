const PASSWORD_STORAGE_KEY = 'cbt_admin_password';

/**
 * Fetch wrapper that adds admin authentication headers.
 *
 * Includes a Bearer token from localStorage (if present) and sends
 * cookies for session-based auth.
 */
export async function adminFetch(url: string, options: RequestInit = {}): Promise<Response> {
  const headers = new Headers(options.headers);

  const storedPassword = localStorage.getItem(PASSWORD_STORAGE_KEY);
  if (storedPassword) {
    headers.set('Authorization', `Bearer ${storedPassword}`);
  }

  return fetch(url, {
    ...options,
    headers,
    credentials: 'same-origin',
  });
}
