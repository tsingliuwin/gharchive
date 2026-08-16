import * as duckdb from '@duckdb/duckdb-wasm';
import wasmMvp from '@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url';
import wasmEh from '@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url';
import workerMvp from '@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url';
import workerEh from '@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url';

const output = document.getElementById('output');
const statusEl = document.getElementById('status');
const runBtn = document.getElementById('runBtn');

function log(msg) {
  output.textContent += msg + '\n';
}
function setStatus(msg) {
  statusEl.textContent = msg;
}

runBtn.addEventListener('click', runTest);

async function runTest() {
  output.textContent = '';
  runBtn.disabled = true;

  const catalogUri = document.getElementById('catalogUri').value.trim();
  const warehouse = document.getElementById('warehouse').value.trim();
  const apiToken = document.getElementById('apiToken').value.trim();

  if (!catalogUri || !warehouse || !apiToken) {
    log('ERROR: Please fill in all three fields.');
    runBtn.disabled = false;
    return;
  }

  try {
    // --- 1. Initialize DuckDB-Wasm ---
    setStatus('Initializing DuckDB-Wasm...');
    log('--- Step 1: Initialize DuckDB-Wasm ---');

    const bundles = {
      mvp: { mainModule: wasmMvp, mainWorker: workerMvp },
      eh: { mainModule: wasmEh, mainWorker: workerEh },
    };
    const bundle = await duckdb.selectBundle(bundles);
    const worker = new Worker(bundle.mainWorker, { type: 'module' });
    const db = new duckdb.AsyncDuckDB(new duckdb.ConsoleLogger(), worker);
    await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
    log('DuckDB-Wasm initialized.');

    const conn = await db.connect();

    // --- 2. Load httpfs ---
    setStatus('Loading httpfs extension...');
    log('\n--- Step 2: Load httpfs ---');
    try {
      await conn.query('INSTALL httpfs');
      await conn.query('LOAD httpfs');
      log('httpfs loaded.');
    } catch (e) {
      log('FAILED to load httpfs: ' + e.message);
      throw e;
    }

    // --- 3. Load iceberg (the critical test) ---
    setStatus('Loading iceberg extension...');
    log('\n--- Step 3: Load iceberg ---');
    try {
      await conn.query('INSTALL iceberg');
      await conn.query('LOAD iceberg');
      log('iceberg extension loaded!');
    } catch (e) {
      log('FAILED to load iceberg: ' + e.message);
      log('\n>>> The iceberg extension is NOT available for DuckDB-Wasm.');
      log('>>> DuckDB-Wasm cannot directly query Iceberg tables via the R2 Data Catalog.');
      log('>>> Alternatives:');
      log('>>>   1. Use DuckDB native (CLI or server) as a backend.');
      log('>>>   2. Export Iceberg data to Parquet and query with read_parquet().');
      log('>>>   3. Use pyiceberg in a serverless function to serve queries.');
      runBtn.disabled = false;
      setStatus('iceberg extension not available for DuckDB-Wasm.');
      return;
    }

    // --- 4. Connect to R2 Data Catalog ---
    setStatus('Connecting to R2 Data Catalog...');
    log('\n--- Step 4: Connect to R2 Data Catalog ---');
    await conn.query(`CREATE SECRET r2_secret (TYPE ICEBERG, TOKEN '${apiToken}')`);
    await conn.query(`ATTACH '${warehouse}' AS r2_catalog (TYPE ICEBERG, ENDPOINT '${catalogUri}')`);
    log('Catalog attached.');

    // --- 5. Test queries ---
    setStatus('Running test queries...');
    log('\n--- Step 5: Test queries ---');

    // Row count
    const countResult = await conn.query('SELECT COUNT(*) AS total FROM r2_catalog.gharchive.events');
    const countRows = countResult.toArray();
    log('Total rows: ' + countRows[0].total);

    // Sample rows
    const sampleResult = await conn.query(
      'SELECT id, type, actor.login AS actor, repo.name AS repo_name, created_at ' +
      'FROM r2_catalog.gharchive.events LIMIT 5'
    );
    const sampleRows = sampleResult.toArray();
    log('\nSample rows:');
    for (const row of sampleRows) {
      log('  ' + JSON.stringify(row));
    }

    // Count by event type
    const typeResult = await conn.query(
      'SELECT type, COUNT(*) AS cnt FROM r2_catalog.gharchive.events ' +
      'GROUP BY type ORDER BY cnt DESC LIMIT 10'
    );
    const typeRows = typeResult.toArray();
    log('\nTop 10 event types:');
    for (const row of typeRows) {
      log(`  ${row.type}: ${row.cnt}`);
    }

    setStatus('Test complete! DuckDB-Wasm can query Iceberg data.');
    log('\n=== SUCCESS: DuckDB-Wasm can query Iceberg tables on R2 Data Catalog ===');

  } catch (e) {
    log('\nERROR: ' + e.message);
    if (e.stack) log(e.stack);
    setStatus('Test failed. See output for details.');
  } finally {
    runBtn.disabled = false;
  }
}
