# GHArchive → Cloudflare R2 (Parquet)

A one-file DuckDB ETL that turns a full day of GitHub Archive hourly
`.json.gz` files into a single sorted, ZSTD-compressed Parquet file in
Cloudflare R2. Runs in GitHub Actions on a daily cron; backfill a day or a whole
month by hand.

## Pipeline

```
data.gharchive.org/{date}-{0..23}.json.gz   (~3 GB compressed / ~30 GB raw)
        │  read over HTTPS via DuckDB httpfs
        ▼
   read_json_auto(...)            # ignore_errors=false fails on a bad line
        │  SELECT id, type, actor.login, repo.name, created_at::TIMESTAMP
        │  ORDER BY repo_name, created_at     (spills to runner disk)
        ▼
   r2://<bucket>/{year}/{month}/{date}.parquet   (PARQUET, ZSTD)
       e.g. r2://gharchive/2026/01/2026-01-01.parquet
```

The 24 hourly files are passed to DuckDB as an **explicit list** — DuckDB does
not expand `*` globs over plain HTTP, so the original one-liner glob form does
not work and must be an explicit URL list. Missing or still-uploading hourly
files are skipped after a HEAD check, so a delayed hour won't fail the run.

## Set up R2 credentials

1. In the Cloudflare dashboard open **R2 → Overview → Manage R2 API Tokens**
   (or **R2 → Manage API Tokens**).
2. Create a token with **Object Read & Write** permission scoped to the bucket
   you will use (or to the account, if you prefer).
3. The token gives you an **Access Key ID**, a **Secret Access Key**, and shows
   your **Account ID**. Create a bucket (e.g. `gharchive`) if you don't have one.

## Add the GitHub Actions secrets

In the repo **Settings → Secrets and variables → Actions → New repository
secret**, add:

| Secret name            | Value                                |
| ---------------------- | ------------------------------------ |
| `R2_ACCOUNT_ID`        | Your Cloudflare account ID           |
| `R2_ACCESS_KEY_ID`     | Access Key ID from the R2 API token |
| `R2_SECRET_ACCESS_KEY` | Secret Access Key from that token    |
| `R2_BUCKET`            | Bucket name, e.g. `gharchive`        |

Credentials are read from the environment at runtime — they are never written
into the repo or the script.

## Run it

Push the workflow. It runs automatically at 04:00 UTC each day for the previous
day. To backfill, use **Actions → GHArchive to R2 → Run workflow** and enter
either:

- `YYYY-MM-DD` — one day, one job.
- `YYYY-MM` — the whole month. A `prepare` job expands it into one matrix job
  per day (up to 8 in parallel), each writing its own Parquet. One bad day
  doesn't cancel the others (`fail-fast: false`); the run is red if any day
  failed, and the job log names which days failed.

Leave the field empty for yesterday (used by the daily cron).

## Testing

There are four tiers, from no credentials up to the real CI run.

**1. Local, no R2 credentials (one hour, ~90 s)** — tests the whole pipeline
except the R2 transport: HTTP read, `ignore_errors=false` parsing, sort,
ZSTD Parquet write, and read-back verification.

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATE=2024-01-01 MAX_FILES=1 OUTPUT_LOCAL=/tmp/out.parquet \
  .venv/bin/python gharchive_to_r2.py
```

`OUTPUT_LOCAL` writes to a local file and skips R2 authentication; `MAX_FILES`
limits the run to the first N hourly files (1–24) for speed.

**2. Local, full day, no R2 credentials (~30 min)** — exercises the real
all-24-files sort with disk spilling.

```bash
DATE=2024-01-01 OUTPUT_LOCAL=/tmp/day.parquet .venv/bin/python gharchive_to_r2.py
```

**3. Real R2 write, one hour (needs credentials)** — verifies the R2 secret,
the `r2://` write, and read-back from R2.

```bash
R2_ACCOUNT_ID=... R2_ACCESS_KEY_ID=... R2_SECRET_ACCESS_KEY=... \
R2_BUCKET=gharchive DATE=2024-01-01 MAX_FILES=1 \
  .venv/bin/python gharchive_to_r2.py
```

**4. GitHub Actions** — push the repo, add the four secrets (below), then
**Actions → GHArchive to R2 → Run workflow** with a `YYYY-MM-DD` or `YYYY-MM`.
The 04:00 UTC cron handles the previous day automatically.

## Output schema

| Column       | Type      | Notes                                  |
| ------------ | --------- | -------------------------------------- |
| `id`         | VARCHAR   | GitHub event id                         |
| `type`       | VARCHAR   | Event type (PushEvent, WatchEvent, …)   |
| `actor`      | VARCHAR   | `actor.login`                           |
| `repo_name`  | VARCHAR   | `repo.name` (`owner/repo`)              |
| `created_at` | TIMESTAMP | Event time (UTC, stored without tz)     |

Sorted by `(repo_name, created_at)`, which makes point lookups and range scans
on a repo cheap via Parquet row-group statistics.

## Notes

- The runner (`ubuntu-latest`) has ~7 GB RAM and ~14 GB free disk. DuckDB sorts
  the day's data with `memory_limit=6GB` and spills to `/tmp/duckdb`, which
  fits.
- `ignore_errors=false` means a single malformed JSON line fails the whole
  day loudly (no output written) rather than dropping it silently. Verified
  clean on real files, but if GHArchive ever serves a corrupt line for a day,
  re-run it after the source is corrected. Flip to `true` only if you'd rather
  tolerate occasional dropped lines.
- Output path is date-driven: `{year}/{month}/{date}.parquet`
  (e.g. `2026/01/2026-01-01.parquet`), so there's no configurable prefix.
- Re-running a day overwrites that date's object in the bucket. Enable R2
  object versioning if you want prior outputs retained.

---

# GHArchive → Cloudflare R2 (Apache Iceberg)

A second pipeline that writes each hour of GitHub Archive data directly into
an Apache Iceberg table on the R2 Data Catalog. Instead of pre-combining 24
hours into one Parquet file, each hour is a separate `INSERT` — Iceberg
handles file layout and small-file compaction automatically. Querying an
Iceberg table is as natural as querying a regular table.

## Pipeline

```
data.gharchive.org/{date}-{0..23}.json.gz
        │  read over HTTPS via DuckDB httpfs (one hour at a time)
        ▼
   read_json_auto(url, ignore_errors=false)
        │  SELECT id, type, actor.login, repo.name, created_at::TIMESTAMP
        ▼
   INSERT INTO r2_catalog.gharchive.events   (Iceberg, partitioned by day)
       24 inserts/day → 24 snapshots → Iceberg compacts small files
```

The table is partitioned by `day(created_at)`, so date-range queries prune
to the relevant partitions. Missing or still-uploading hourly files are
skipped after a HEAD check, and each hour is idempotent (re-runs only
process hours not yet written).

## Set up the R2 Data Catalog

1. Install or update `wrangler` (Cloudflare CLI):
   ```bash
   npm install -g wrangler
   wrangler login
   ```

2. Enable the Data Catalog on your R2 bucket. This produces a **Warehouse
   name** and a **Catalog URI**:
   ```bash
   wrangler r2 bucket catalog enable <BUCKET_NAME>
   ```

3. Create an R2 API token with **Admin Read & Write** permission for both
   **R2 Data Catalog** and **R2 Storage**. In the Cloudflare dashboard:
   **R2 → Manage R2 API Tokens → Create API Token**.

   The token covers both catalog API access and underlying data-file
   writes — the catalog vendors SigV4 credentials to DuckDB for R2 storage,
   so no separate S3 keys are needed.

## Add the GitHub Actions secrets

In the repo **Settings → Secrets and variables → Actions → New repository
secret**, add:

| Secret name        | Value                                |
| ------------------ | ------------------------------------ |
| `R2_CATALOG_URI`    | Catalog URI from `catalog enable`   |
| `R2_WAREHOUSE`      | Warehouse name from `catalog enable` |
| `R2_API_TOKEN`      | R2 API token (Data Catalog + Storage) |

## Run it

Push the `gharchive-to-iceberg.yml` workflow. It runs at 04:00 UTC each day
for the previous day. To backfill, use **Actions → GHArchive to Iceberg →
Run workflow** and enter either:

- `YYYY-MM-DD` — one day, one job.
- `YYYY-MM` — the whole month. A `prepare` job fans out to one matrix job
  per day (up to 4 in parallel). `max-parallel` is 4 (not 8) to reduce
  concurrent commit conflicts on the shared Iceberg table; the script also
  retries on commit failures with exponential backoff.

Leave the field empty for yesterday (used by the daily cron).

## Testing

**One hour, real R2 credentials (~90 s)** — verifies the full path: R2
Data Catalog connection, table creation, JSON read, INSERT, and read-back.

```bash
R2_CATALOG_URI=... R2_WAREHOUSE=... R2_API_TOKEN=... \
  DATE=2024-01-01 MAX_HOURS=1 \
  .venv/bin/python gharchive_to_iceberg.py
```

`MAX_HOURS=1` limits the run to the first hourly file for a quick smoke
test. `MAX_HOURS` can be 1–24.

## Table schema

| Column       | Type      | Notes                                  |
| ------------ | --------- | -------------------------------------- |
| `id`         | VARCHAR   | GitHub event id                         |
| `type`       | VARCHAR   | Event type (PushEvent, WatchEvent, …)   |
| `actor`      | VARCHAR   | `actor.login`                           |
| `repo_name`  | VARCHAR   | `repo.name` (`owner/repo`)              |
| `created_at` | TIMESTAMP | Event time (UTC, stored without tz)     |

Partitioned by `day(created_at)`. No sort order is declared — Iceberg's
built-in file-level min/max statistics provide basic pruning, and the day
partition gives date-range pruning.

## Idempotency

Before INSERTing an hour, the script checks whether the table already has
rows for that hour (`SELECT 1 … LIMIT 1`). If it does, that hour is
skipped. This means:

- Re-running a completed day is a no-op.
- Re-running a partially failed day only processes the hours that were not
  yet written.

Iceberg INSERTs are atomic — a failed commit leaves no partial data, so a
retry (manual or automatic) won't create duplicates.

## Notes

- The R2 Data Catalog is in public beta. Check the [Cloudflare R2 Data
  Catalog docs](https://developers.cloudflare.com/r2-data-catalog/) for
  the current limitations.
- DuckDB's `iceberg` extension is marked experimental. APIs may change
  between releases.
- The old Parquet pipeline (`gharchive_to_r2.py` / `gharchive-to-r2.yml`)
  is left untouched. Disable its workflow when you no longer need it.
- Compaction and snapshot expiration are managed by the R2 Data Catalog
  (see Cloudflare docs). You can also trigger compaction from DuckDB or
  PyIceberg if needed.
