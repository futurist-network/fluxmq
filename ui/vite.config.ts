import { defineConfig } from 'vitest/config';
import react from '@vitejs/plugin-react';
import { readFileSync } from 'node:fs';

const pkg = JSON.parse(readFileSync(new URL('./package.json', import.meta.url), 'utf8')) as { version: string };

const target = process.env.FLUXMQ_URL ?? 'http://localhost:7421';

export default defineConfig({
  plugins: [react()],
  define: { __APP_VERSION__: JSON.stringify(pkg.version) },
  server: {
    port: 5173,
    proxy: {
      '/api': { target, changeOrigin: true },
      '/metrics': { target, changeOrigin: true },
      '/ws': { target: target.replace(/^http/, 'ws'), ws: true, changeOrigin: true },
    },
  },
  preview: {
    port: 4173,
    proxy: {
      '/api': { target, changeOrigin: true },
      '/metrics': { target, changeOrigin: true },
      '/ws': { target: target.replace(/^http/, 'ws'), ws: true, changeOrigin: true },
    },
  },
  build: {
    outDir: 'dist',
    sourcemap: true,
    chunkSizeWarningLimit: 800,
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts', 'mock/**/*.test.ts'],
  },
});
