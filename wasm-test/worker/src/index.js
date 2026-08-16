/**
 * CORS proxy for the Cloudflare R2 Data Catalog.
 *
 * The R2 Data Catalog endpoint (catalog.cloudflarestorage.com) doesn't
 * return CORS headers, so browser-based DuckDB-Wasm requests are blocked.
 * This Worker transparently forwards all requests to the catalog and adds
 * the necessary CORS headers to the response.
 *
 * Usage: set your Catalog URI to the Worker URL + the original catalog path.
 *   e.g. https://iceberg-cors-proxy.<you>.workers.dev/<account-id>/<bucket-name>
 *   instead of https://catalog.cloudflarestorage.com/<account-id>/<bucket-name>
 */

const CATALOG_ORIGIN = 'https://catalog.cloudflarestorage.com';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, HEAD, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': '*',
  'Access-Control-Max-Age': '86400',
};

export default {
  async fetch(request) {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    // Forward to the R2 Data Catalog
    const url = new URL(request.url);
    const targetUrl = CATALOG_ORIGIN + url.pathname + url.search;

    const response = await fetch(targetUrl, request);

    // Clone response and add CORS headers
    const newResponse = new Response(response.body, {
      status: response.status,
      statusText: response.statusText,
      headers: response.headers,
    });
    for (const [key, value] of Object.entries(corsHeaders)) {
      newResponse.headers.set(key, value);
    }
    return newResponse;
  },
};
