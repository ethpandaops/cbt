/** Shape of the runtime config injected by the Go backend as window.__CONFIG__. */
export interface InjectedConfig {
  title?: string;
  baseUrl?: string;
  managementEnabled?: boolean;
  authMethods?: ('password' | 'github')[];
}

declare global {
  interface Window {
    __CONFIG__?: InjectedConfig;
  }
}

/** Reads the injected config from window.__CONFIG__, returning null if absent. */
export function getInjectedConfig(): InjectedConfig | null {
  if (typeof window === 'undefined' || !window.__CONFIG__) {
    return null;
  }

  return window.__CONFIG__;
}
