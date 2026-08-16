#!/usr/bin/env python3
"""GHArchive hourly -> Cloudflare R2 Iceberg ETL.

Reads each hour of GitHub Archive `.json.gz` and INSERTs it directly into
an Apache Iceberg table managed by the Cloudflare R2 Data Catalog. No daily
pre-combining — each hour is a separate INSERT, and Iceberg handles file
layout and small-file compaction automatically.

Output table:
    r2_catalog.gharchive.events  (partitioned by day(created_at))

DATE input:
    ""          yesterday (default; used by the daily cron)
    YYYY-MM-DD  a single day
    YYYY-MM     every day in that month (run locally: sequential; in Actions:
                the workflow fans these out to parallel per-day jobs)

Configuration via environment variables:
    R2_CATALOG_URI     R2 Data Catalog endpoint URI
    R2_WAREHOUSE       R2 Data Catalog warehouse name
    R2_API_TOKEN       R2 API token (Data Catalog + Storage r/w)
    GHARCHIVE_BASE_URL Optional. Base URL (default: https://data.gharchive.org).
    MAX_HOURS          Optional. Process only the first N hourly files (1-24),
                        for a fast test. e.g. MAX_HOURS=1

The R2 Data Catalog is a managed Iceberg REST catalog built into an R2
bucket. The API token covers both catalog access and underlying data-file
access (the catalog vendors SigV4 credentials for R2 storage), so no
separate S3 keys are needed.
"""
from __future__ import annotations

import calendar
import os
import re
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

MAX_RETRIES = 3
RETRY_BASE_DELAY = 2  # seconds


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
    catalog_uri: str
    warehouse: str
    api_token: str
    date: str
    base_url: str
    max_hours: int  # 0 => all 24 hourly files

    @classmethod
    def from_env(cls, date: str) -> "Config":
        def need(name: str) -> str:
            v = os.environ.get(name, "").strip()
            if not v:
                fail(f"missing required env var: {name}")
            return v

        catalog_uri = need("R2_CATALOG_URI")
        warehouse = need("R2_WAREHOUSE")
        api_token = need("R2_API_TOKEN")

        max_hours = 0
        raw_max = os.environ.get("MAX_HOURS", "").strip()
        if raw_max:
            try:
                max_hours = int(raw_max)
            except ValueError:
                fail(f"MAX_HOURS must be an integer 1-24, got: {raw_max!r}")
            if not 1 <= max_hours <= 24:
                fail(f"MAX_HOURS must be 1-24, got: {max_hours}")

        if not DATE_RE.match(date):
            fail(f"DATE must be YYYY-MM-DD, got: {date!r}")

        base_url = os.environ.get("GHARCHIVE_BASE_URL", "https://data.gharchive.org").strip().rstrip("/")
        return cls(catalog_uri, warehouse, api_token, date, base_url, max_hours)


def hourly_urls(cfg: Config) -> list[str]:
    urls = [f"{cfg.base_url}/{cfg.date}-{h}.json.gz" for h in range(24)]
    if cfg.max_hours:
        urls = urls[:cfg.max_hours]
    return urls


def url_exists(url: str, timeout: float = 15.0) -> bool:
    req = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "gharchive-to-iceberg/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 400
    except Exception:
        return False


def filter_existing(urls: list[str]) -> set[str]:
    with ThreadPoolExecutor(max_workers=8) as ex:
        present = {u for u, ok in zip(urls, ex.map(url_exists, urls)) if ok}
    return present


def sql_quote(value: str) -> str:
    """Single-quote a string for SQL, doubling embedded quotes."""
    return "'" + value.replace("'", "''") + "'"


def hour_ts_literal(date: str, hour: int) -> str:
    """Build a TIMESTAMP literal: TIMESTAMP '2026-01-01 05:00:00'."""
    return f"TIMESTAMP '{date} {hour:02d}:00:00'"


def is_commit_conflict(exc: Exception) -> bool:
    """Detect Iceberg optimistic-concurrency commit-failed errors."""
    msg = str(exc).lower()
    return any(kw in msg for kw in ("commit", "conflict", "concurrent", "occ", "retry"))


def hour_has_data(con: duckdb.DuckDBPyConnection, date: str, hour: int) -> bool:
    """Check whether the Iceberg table already has rows for this hour."""
    start = hour_ts_literal(date, hour)
    result = con.sql(f"""
        SELECT 1 FROM r2_catalog.gharchive.events
        WHERE created_at >= {start}
          AND created_at <  {start} + INTERVAL 1 hour
        LIMIT 1
    """).fetchone()
    return result is not None


def insert_hour(con: duckdb.DuckDBPyConnection, date: str, hour: int, url: str) -> None:
    """INSERT one hour of GHArchive data into the Iceberg table.

    Retries on Iceberg commit conflict (concurrent writers in month backfill).
    Iceberg INSERTs are atomic — a failed commit leaves no partial data.
    """
    insert_sql = f"""
        INSERT INTO r2_catalog.gharchive.events BY NAME
        SELECT
            id::VARCHAR            AS id,
            type::VARCHAR          AS type,
            actor.login::VARCHAR   AS actor,
            repo.name::VARCHAR     AS repo_name,
            created_at::TIMESTAMP  AS created_at
        FROM read_json_auto({sql_quote(url)}, ignore_errors=false)
    """
    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            con.sql(insert_sql)
            return
        except Exception as e:
            last_exc = e
            if is_commit_conflict(e) and attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                print(f"  hour={hour} commit conflict, retry {attempt + 1}/{MAX_RETRIES} in {delay}s ...")
                time.sleep(delay)
                continue
            raise
    if last_exc:
        raise last_exc


def run_etl(cfg: Config) -> None:
    urls = hourly_urls(cfg)
    existing = filter_existing(urls)
    if not existing:
        raise ETLError(f"no source files found for {cfg.date} (tried {len(urls)} hourly URLs)")

    missing = len(urls) - len(existing)
    scope = f"max_hours={cfg.max_hours}" if cfg.max_hours else "full_day"
    print(f"date={cfg.date} {scope} source_files={len(existing)}/{len(urls)}"
          + (f" missing={missing}" if missing else ""))

    import duckdb

    con = duckdb.connect()
    # httpfs: read source JSON over HTTPS. iceberg: write to the Iceberg table.
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL iceberg; LOAD iceberg;")
    con.sql("SET temp_directory='/tmp/duckdb';")
    con.sql("SET memory_limit='6GB';")
    con.sql("SET preserve_insertion_order=false;")

    # The R2 Data Catalog token covers both catalog API and data-file access
    # (the catalog vendors SigV4 credentials for underlying R2 storage).
    con.sql(f"""
        CREATE SECRET r2_iceberg_secret (
            TYPE ICEBERG,
            TOKEN {sql_quote(cfg.api_token)}
        );
    """)
    con.sql(f"""
        ATTACH {sql_quote(cfg.warehouse)} AS r2_catalog (
            TYPE ICEBERG,
            ENDPOINT {sql_quote(cfg.catalog_uri)}
        );
    """)

    # Create schema and table (idempotent — safe to run every time).
    con.sql("CREATE SCHEMA IF NOT EXISTS r2_catalog.gharchive;")
    con.sql("""
        CREATE TABLE IF NOT EXISTS r2_catalog.gharchive.events (
            id VARCHAR,
            type VARCHAR,
            actor VARCHAR,
            repo_name VARCHAR,
            created_at TIMESTAMP
        ) PARTITIONED BY (day(created_at));
    """)

    inserted = 0
    skipped_existing = 0
    skipped_missing = 0

    for hour, url in enumerate(urls):
        if url not in existing:
            print(f"  hour={hour} status=missing_source")
            skipped_missing += 1
            continue
        if hour_has_data(con, cfg.date, hour):
            print(f"  hour={hour} status=skipped_existing")
            skipped_existing += 1
            continue
        # ignore_errors=false: a single malformed JSON line fails this hour
        # loudly (no partial write — Iceberg INSERTs are atomic). Only that
        # hour fails; the remaining hours are still processed.
        try:
            insert_hour(con, cfg.date, hour, url)
            print(f"  hour={hour} status=inserted")
            inserted += 1
        except Exception as e:
            raise ETLError(f"hour={hour} insert failed: {type(e).__name__}: {e}")

    # Verify total rows for the date.
    total_rows = con.sql(f"""
        SELECT COUNT(*) FROM r2_catalog.gharchive.events
        WHERE created_at >= TIMESTAMP '{cfg.date} 00:00:00'
          AND created_at <  TIMESTAMP '{cfg.date} 00:00:00' + INTERVAL 1 day
    """).fetchone()[0]

    print(f"done: date={cfg.date} hours_inserted={inserted} "
          f"hours_skipped_existing={skipped_existing} "
          f"hours_missing={skipped_missing} total_rows={total_rows:,}")
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
