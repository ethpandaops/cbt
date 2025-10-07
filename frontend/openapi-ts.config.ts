import { defineConfig } from '@hey-api/openapi-ts';

export default defineConfig({
  input: '../api/openapi.yaml',
  output: {
    path: 'src/client',
    format: 'prettier',
    lint: 'eslint',
  },
  plugins: [
    {
      name: '@hey-api/client-fetch',
      runtimeConfigPath: '../lib/api-config.ts',
    },
    {
      metadata: true,
      name: 'zod',
    },
    '@tanstack/react-query',
    {
      dates: true,
      name: '@hey-api/sdk',
      validator: 'zod',
    },
  ],
});
