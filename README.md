# GHArchive → Cloudflare R2 (Parquet)

A one-file DuckDB ETL that turns a full day of GitHub Archive hourly
`.json.gz` files into a single sorted, ZSTD-compressed Parquet file in
Cloudflare R2. Runs in GitHub Actions on a daily cron; backfill any day by hand.

## Pipeline

```
data.gharchive.org/{date}-{0..23}.json.gz   (~3 GB compressed / ~30 GB raw)
        │  read over HTTPS via DuckDB httpfs
        ▼
   read_json_auto(...)            # ignore_errors skips malformed lines
        │  SELECT id, type, actor.login, repo.name, created_at::TIMESTAMP
        │  ORDER BY repo_name, created_at     (spills to runner disk)
        ▼
   r2://<bucket>/gharchive/{date}.parquet     (PARQUET, ZSTD)
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
day. To backfill a specific day, use **Actions → GHArchive to R2 → Run
workflow** and enter a `YYYY-MM-DD`.

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
**Actions → GHArchive to R2 → Run workflow** with a `YYYY-MM-DD`. The 04:00 UTC
cron handles the previous day automatically.

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
- Re-running a day overwrites `gharchive/{date}.parquet` in the bucket. Enable
  R2 object versioning if you want prior outputs retained.
