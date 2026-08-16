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
    // Proxy catalog API requests to the R2 Data Catalog endpoint.
    // This avoids CORS issues: the browser sees same-origin requests
    // (http://localhost:5173/catalog/...), and Vite forwards them to
    // https://catalog.cloudflarestorage.com/...
    //
    // Catalog URI in the test page should be set to:
    //   http://localhost:5173/catalog/<account-id>/<bucket-name>
    proxy: {
      '/catalog': {
        target: 'https://catalog.cloudflarestorage.com',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/catalog/, ''),
        secure: true,
      },
    },
  },
});
