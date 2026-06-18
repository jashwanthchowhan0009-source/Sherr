import { defineConfig } from 'vite';

// MapLibre GL requires a modern target (BigInt literals, etc.).
export default defineConfig({
  // Served from the main SherrByte deployment at /globe/ (single repo, single host).
  base: '/globe/',
  server: { port: 5173 },
  build: { outDir: 'dist', target: 'es2020' },
});
