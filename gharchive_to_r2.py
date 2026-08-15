#!/usr/bin/env python3
"""GHArchive daily -> Cloudflare R2 Parquet ETL.

Reads a full day of GitHub Archive hourly `.json.gz` files, projects to a slim
column set, sorts by (repo_name, created_at), and writes a single
ZSTD-compressed Parquet file to Cloudflare R2. Designed to run in GitHub
Actions against a day of data (~30 GB uncompressed) with DuckDB spilling the
sort to the runner's disk.

Output object key (date-driven, no prefix):
    {year}/{month}/{date}.parquet   e.g. 2026/01/2026-01-01.parquet

DATE input:
    ""          yesterday (default; used by the daily cron)
    YYYY-MM-DD  a single day
    YYYY-MM     every day in that month (run locally: sequential; in Actions:
                the workflow fans these out to parallel per-day jobs)

Configuration via environment variables:
    R2_ACCOUNT_ID          Cloudflare account ID (builds the R2 endpoint). Skipped
                            when OUTPUT_LOCAL is set.
    R2_ACCESS_KEY_ID        R2 access key ID (GitHub Actions secret).
    R2_SECRET_ACCESS_KEY    R2 secret access key (GitHub Actions secret).
    R2_BUCKET               R2 bucket to write into.
    GHARCHIVE_BASE_URL      Optional. Base URL (default: https://data.gharchive.org).

Local testing (no R2 credentials needed):
    OUTPUT_LOCAL            If set, write Parquet to this local path instead of R2,
                            and skip R2 authentication. e.g. OUTPUT_LOCAL=/tmp/out.parquet
    MAX_FILES               Optional. Process only the first N hourly files (1-24),
                            for a fast local test. e.g. MAX_FILES=1
"""
from __future__ import annotations

import calendar
import os
import re
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
# R2 bucket naming rules: lowercase alnum + hyphens, 3-63 chars.
BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
LOCAL_PATH_RE = re.compile(r"^[A-Za-z0-9_./\-]+$")


class ETLError(RuntimeError):
    """A per-day failure (e.g. no source files, a malformed line). Caller
    decides whether to abort (single-day) or record and continue (month)."""


def fail(msg: str, code: int = 1) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)


def resolve_dates(raw: str) -> list[str]:
    """Expand an input into concrete YYYY-MM-DD days.

    ""          -> [yesterday]
    YYYY-MM-DD  -> [that day]
    YYYY-MM     -> every day in that month (handles leap years)
    """
    if not raw:
        return [(datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")]
    if DATE_RE.match(raw):
        return [raw]
    if MONTH_RE.match(raw):
        year, month = int(raw[:4]), int(raw[5:7])
        n_days = calendar.monthrange(year, month)[1]
        return [f"{raw}-{d:02d}" for d in range(1, n_days + 1)]
    fail(f"DATE must be YYYY-MM-DD or YYYY-MM, got: {raw!r}")


@dataclass
class Config:
    account_id: str
    access_key_id: str
    secret_access_key: str
    bucket: str
    date: str
    base_url: str
    output_local: str  # "" => write to R2; else a local file path
    max_files: int  # 0 => all 24 hourly files

    @classmethod
    def from_env(cls, date: str) -> "Config":
        output_local = os.environ.get("OUTPUT_LOCAL", "").strip()
        if output_local and not LOCAL_PATH_RE.match(output_local):
            fail(f"OUTPUT_LOCAL has disallowed characters: {output_local!r}")

        max_files = 0
        raw_max = os.environ.get("MAX_FILES", "").strip()
        if raw_max:
            try:
                max_files = int(raw_max)
            except ValueError:
                fail(f"MAX_FILES must be an integer 1-24, got: {raw_max!r}")
            if not 1 <= max_files <= 24:
                fail(f"MAX_FILES must be 1-24, got: {max_files}")

        # R2 credentials are only needed for the real (non-local) run.
        if output_local:
            account_id = access_key_id = secret_access_key = ""
            bucket = ""
        else:
            def need(name: str) -> str:
                v = os.environ.get(name, "").strip()
                if not v:
                    fail(f"missing required env var: {name}")
                return v

            account_id = need("R2_ACCOUNT_ID")
            access_key_id = need("R2_ACCESS_KEY_ID")
            secret_access_key = need("R2_SECRET_ACCESS_KEY")
            bucket = need("R2_BUCKET")
            if not BUCKET_RE.match(bucket):
                fail(f"R2_BUCKET has an invalid name: {bucket!r}")

        if not DATE_RE.match(date):
            fail(f"DATE must be YYYY-MM-DD, got: {date!r}")

        base_url = os.environ.get("GHARCHIVE_BASE_URL", "https://data.gharchive.org").strip().rstrip("/")
        return cls(account_id, access_key_id, secret_access_key, bucket, date,
                    base_url, output_local, max_files)

    @property
    def uses_r2(self) -> bool:
        return not self.output_local

    @property
    def object_key(self) -> str:
        # 2026-01-01 -> 2026/01/2026-01-01.parquet
        return f"{self.date[:4]}/{self.date[5:7]}/{self.date}.parquet"

    @property
    def r2_uri(self) -> str:
        # bucket is charset-validated, so this is safe to embed in SQL.
        return f"r2://{self.bucket}/{self.object_key}"

    @property
    def destination(self) -> str:
        return self.output_local if self.output_local else self.r2_uri


def hourly_urls(cfg: Config) -> list[str]:
    urls = [f"{cfg.base_url}/{cfg.date}-{h}.json.gz" for h in range(24)]
    if cfg.max_files:
        urls = urls[:cfg.max_files]
    return urls


def url_exists(url: str, timeout: float = 15.0) -> bool:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "gharchive-to-r2/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 400
    except Exception:
        return False


def filter_existing(urls: list[str]) -> list[str]:
    with ThreadPoolExecutor(max_workers=8) as ex:
        present = [u for u, ok in zip(urls, ex.map(url_exists, urls)) if ok]
    return sorted(present)


def sql_quote(value: str) -> str:
    """Single-quote a string for SQL, doubling embedded quotes."""
    return "'" + value.replace("'", "''") + "'"


def sql_list_literal(items: list[str]) -> str:
    return "[" + ", ".join(sql_quote(i) for i in items) + "]"


def run_etl(cfg: Config) -> None:
    urls = hourly_urls(cfg)
    existing = filter_existing(urls)
    if not existing:
        raise ETLError(f"no source files found for {cfg.date} (tried {len(urls)} hourly URLs)")

    missing = len(urls) - len(existing)
    scope = f"max_files={cfg.max_files}" if cfg.max_files else "full_day"
    print(f"date={cfg.date} {scope} source_files={len(existing)}/{len(urls)}"
          + (f" missing={missing}" if missing else ""))

    import duckdb  # lazy: lets the prepare job import this module without duckdb

    con = duckdb.connect()
    # httpfs is required even for the local run, since the source is read over HTTPS.
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("SET temp_directory='/tmp/duckdb';")
    con.sql("SET memory_limit='6GB';")
    con.sql("SET preserve_insertion_order=false;")

    if cfg.uses_r2:
        # R2 credentials via DuckDB's modern secret manager: path-style URLs and
        # the endpoint are derived automatically from the account ID.
        con.sql(
            "CREATE SECRET r2_secret ("
            f"TYPE r2, KEY_ID {sql_quote(cfg.access_key_id)}, "
            f"SECRET {sql_quote(cfg.secret_access_key)}, "
            f"ACCOUNT_ID {sql_quote(cfg.account_id)});"
        )

    src = sql_list_literal(existing)
    dest = cfg.destination
    print(f"writing {dest} ...")

    # ignore_errors=false: fail loudly on a malformed line rather than silently
    # dropping it. If GHArchive serves a corrupt line for the day, the run fails
    # and produces no output — re-run after the source file is corrected.
    con.sql(f"""
        COPY (
            SELECT
                id::VARCHAR            AS id,
                type::VARCHAR          AS type,
                actor.login::VARCHAR   AS actor,
                repo.name::VARCHAR     AS repo_name,
                created_at::TIMESTAMP  AS created_at
            FROM read_json_auto({src}, ignore_errors=false)
            ORDER BY repo_name, created_at
        ) TO {sql_quote(dest)} (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Read back just the row-group metadata to confirm the object is intact.
    row_count = con.sql(f"SELECT COUNT(*) FROM read_parquet({sql_quote(dest)})").fetchone()[0]
    print(f"done: {dest} rows={row_count:,}")
    if missing:
        print(f"note: {missing} hourly file(s) were unavailable and skipped")


def main() -> None:
    raw = os.environ.get("DATE", "").strip()
    dates = resolve_dates(raw)

    if len(dates) == 1:
        try:
            run_etl(Config.from_env(dates[0]))
        except ETLError as e:
            print(f"ERROR: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: {type(e).__name__}: {e}", file=sys.stderr)
            sys.exit(1)
        return

    # Month mode: process each day, tolerate per-day failures, summarize.
    ok: list[str] = []
    failed: list[tuple[str, str]] = []
    for i, d in enumerate(dates, 1):
        print(f"\n=== {d} ({i}/{len(dates)}) ===")
        try:
            run_etl(Config.from_env(d))
            ok.append(d)
        except ETLError as e:
            failed.append((d, str(e)))
        except Exception as e:
            failed.append((d, f"{type(e).__name__}: {str(e)[:160]}"))

    print(f"\n=== summary: {len(ok)} ok, {len(failed)} failed ===")
    for d, why in failed:
        print(f"  FAILED {d}: {why}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
