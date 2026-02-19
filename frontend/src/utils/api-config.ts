import type { CreateClientConfig } from '@/api/client.gen';
import { getInjectedConfig } from '@/utils/config';

export const createClientConfig: CreateClientConfig = config => ({
  ...config,
  baseUrl: getInjectedConfig()?.baseUrl ?? import.meta.env.VITE_API_URL ?? '/api/v1',
});
