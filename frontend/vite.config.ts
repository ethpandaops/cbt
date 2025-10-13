import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';
import { tanstackRouter } from '@tanstack/router-plugin/vite';
import { visualizer } from 'rollup-plugin-visualizer';
import path from 'path';

// https://vite.dev/config/
export default defineConfig({
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: './vitest.setup.ts',
  },
  plugins: [
    tanstackRouter({
      routesDirectory: './src/routes',
      generatedRouteTree: './src/routeTree.gen.ts',
      autoCodeSplitting: true,
    }),
    tailwindcss(),
    react(),
    visualizer({
      open: true,
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
        target: 'http://localhost:8888',
        changeOrigin: true,
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
      '@types': path.resolve(__dirname, './src/types'),
      '@assets': path.resolve(__dirname, './src/assets'),
    },
    conditions: ['import', 'module', 'browser', 'default'],
  },
  build: {
    outDir: 'build/frontend',
    commonjsOptions: {
      transformMixedEsModules: true, // Handle packages with mixed ESM/CommonJS
    },
    rollupOptions: {
      output: {
        manualChunks(id) {
          // TanStack ecosystem (check before react to avoid conflicts)
          if (id.includes('@tanstack')) {
            return 'tanstack-vendor';
          }

          // DAG visualization (check before react to avoid conflicts)
          if (id.includes('@xyflow') || id.includes('dagre')) {
            return 'dag-vendor';
          }

          // Syntax highlighting (check before react to avoid conflicts)
          // Only Prism is used, exclude highlight.js if it somehow gets imported
          if (id.includes('react-syntax-highlighter') && !id.includes('highlight.js')) {
            return 'syntax-vendor';
          }

          // Explicitly exclude highlight.js (we use Prism only)
          if (id.includes('highlight.js')) {
            return 'highlightjs-unused';
          }

          // Headless UI with its react-dom dependencies (check before react to avoid conflicts)
          if (id.includes('@headlessui') || id.includes('@floating-ui')) {
            return 'headlessui-vendor';
          }

          // Heroicons separately
          if (id.includes('@heroicons')) {
            return 'ui-vendor';
          }

          // React core only - use strict matching to avoid catching react-tooltip, react-range-slider-input
          if (id.includes('node_modules/react/') || id.includes('node_modules/react-dom/')) {
            return 'react-vendor';
          }

          // Tooltip library - used in specific components
          if (id.includes('react-tooltip')) {
            return 'tooltip-vendor';
          }

          // CEL expression evaluator - used for interval transformations
          if (id.includes('cel-js')) {
            return 'cel-vendor';
          }

          // Range slider - used in zoom controls
          if (id.includes('react-range-slider-input')) {
            return 'slider-vendor';
          }

          // Zod validation - keep with core vendor (used everywhere in API client)
          if (id.includes('node_modules/zod')) {
            return 'zod-vendor';
          }

          // Generated API client
          if (id.includes('/src/api/')) {
            return 'api-client';
          }

          // Other node_modules
          if (id.includes('node_modules')) {
            return 'vendor';
          }
        },
      },
    },
  },
});
