import { defineConfig } from 'vite';

// MapLibre GL requires a modern target (BigInt literals, etc.).
export default defineConfig({
  server: { port: 5173 },
  build: { outDir: 'dist', target: 'es2020' },
});
