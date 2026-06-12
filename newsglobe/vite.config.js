import { defineConfig } from 'vite';

// Plain vanilla + three.js app. The /api functions are Vercel serverless
// functions (Node) and are not bundled by Vite.
export default defineConfig({
  server: { port: 5173 },
  build: { outDir: 'dist', target: 'es2019' },
});
