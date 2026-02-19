import { defineConfig } from '@hey-api/openapi-ts';

export default defineConfig({
  input: '../api/openapi.yaml',
  output: {
    path: 'src/api',
    postProcess: ['eslint', 'prettier'],
  },
  plugins: [
    {
      name: '@hey-api/client-fetch',
      runtimeConfigPath: '../utils/api-config.ts',
    },
    {
      name: 'zod',
      compatibilityVersion: 'mini',
      metadata: false,
    },
    '@tanstack/react-query',
    {
      name: '@hey-api/sdk',
      validator: 'zod',
    },
  ],
});
