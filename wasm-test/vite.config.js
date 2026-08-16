import { defineConfig } from 'vite';

export default defineConfig({
  // Prevent Vite from pre-bundling the wasm package — it ships .wasm and
  // .worker.js files that must be served as static assets, not processed.
  optimizeDeps: {
    exclude: ['@duckdb/duckdb-wasm'],
  },
  server: {
    // DuckDB-Wasm needs cross-origin isolation for SharedArrayBuffer.
    headers: {
      'Cross-Origin-Opener-Policy': 'same-origin',
      'Cross-Origin-Embedder-Policy': 'require-corp',
    },
  },
});
