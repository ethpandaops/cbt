import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';

const BACKENDS: Record<string, string> = {
  local: 'http://localhost:8080',
  mainnet: 'https://cbt.mainnet.ethpandaops.io',
};

const backendKey = process.env.BACKEND ?? 'mainnet';
const backendTarget = BACKENDS[backendKey] ?? backendKey;

// https://vite.dev/config/
export default defineConfig({
  base: '/',
  define: {
    'import.meta.env.VITE_BASE_TITLE': JSON.stringify('CBT'),
    'import.meta.env.VITE_BASE_URL': JSON.stringify('http://localhost:5173'),
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
  optimizeDeps: {
    include: ['react-syntax-highlighter'],
  },
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
      '@components': path.resolve(__dirname, './src/components'),
      '@pages': path.resolve(__dirname, './src/pages'),
      '@hooks': path.resolve(__dirname, './src/hooks'),
      '@utils': path.resolve(__dirname, './src/utils'),
      '@api': path.resolve(__dirname, './src/api'),
      '@assets': path.resolve(__dirname, './src/assets'),
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
        manualChunks(id) {
          if (id.includes('cel-js')) {
            return 'vendor';
          }
        },
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
