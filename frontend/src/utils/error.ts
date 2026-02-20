export function getErrorMessage(err: unknown, fallback = 'Unknown error'): string {
  if (!err) return fallback;

  if (typeof err === 'object' && err !== null) {
    const withMessage = err as { message?: unknown };
    if (typeof withMessage.message === 'string' && withMessage.message.length > 0) {
      return withMessage.message;
    }

    const withApiError = err as { error?: unknown };
    if (typeof withApiError.error === 'string' && withApiError.error.length > 0) {
      return withApiError.error;
    }
  }

  if (typeof err === 'string' && err.length > 0) return err;

  return fallback;
}

export function getErrorCode(err: unknown): number | undefined {
  if (!err || typeof err !== 'object') return undefined;

  const withCode = err as { code?: unknown };
  return typeof withCode.code === 'number' ? withCode.code : undefined;
}
