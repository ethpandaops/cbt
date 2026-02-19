import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';

const backendTarget = process.env.BACKEND ?? 'http://localhost:8080';

// https://vite.dev/config/
export default defineConfig({
  base: '/',
  define: {
    'import.meta.env.VITE_BASE_TITLE': JSON.stringify('CBT'),
  },
  plugins: [
    tanstackRouter({
      routesDirectory: './src/routes',
      generatedRouteTree: './src/routeTree.gen.ts',
      autoCodeSplitting: true,
      quoteStyle: 'single',
    }),
    tailwindcss(),
    react(),
    visualizer({
      open: false,
      gzipSize: true,
      brotliSize: true,
      filename: 'build/stats.html',
    }),
  ],
  server: {
    proxy: {
      '/api': {
        target: backendTarget,
        changeOrigin: true,
        secure: backendTarget.startsWith('https'),
      },
    },
  },
  resolve: {
    alias: {
      '@': path.resolve(__dirname, './src'),
    },
    conditions: ['import', 'module', 'browser', 'default'],
  },
  build: {
    outDir: 'build/frontend',
    commonjsOptions: {
      transformMixedEsModules: true,
    },
    rollupOptions: {
      output: {
        chunkFileNames: () => {
          const randomHash = Math.random().toString(36).substring(2, 10);
          return `assets/[name]-${randomHash}.js`;
        },
        entryFileNames: () => {
          const randomHash = Math.random().toString(36).substring(2, 10);
          return `assets/[name]-${randomHash}.js`;
        },
        assetFileNames: assetInfo => {
          if (assetInfo.name?.match(/\.(woff2?|ttf|eot|otf)$/)) {
            return 'assets/[name]-[hash].[ext]';
          }
          const randomHash = Math.random().toString(36).substring(2, 10);
          return `assets/[name]-${randomHash}.[ext]`;
        },
      },
    },
  },
});
