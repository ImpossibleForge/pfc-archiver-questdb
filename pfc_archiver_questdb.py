#!/usr/bin/env python3
"""
pfc-archiver-questdb v0.1.0 — Autonomous cold partition archive daemon for QuestDB
====================================================================================

Watches a QuestDB instance for data older than a configurable retention window,
compresses it to PFC format, writes it to local storage or S3, verifies integrity,
and optionally deletes the archived rows from the source.

Connects via QuestDB's PostgreSQL wire protocol (port 8812).

Archive cycle per partition:
  1. SCAN    — find partitions older than retention_days
  2. EXPORT  — stream rows from QuestDB to a temp JSONL file
  3. COMPRESS — pipe through pfc_jsonl compress -> .pfc + .pfc.bidx + .pfc.idx
  4. UPLOAD  — copy archive to output_dir (local path or s3://bucket/prefix/)
  5. VERIFY  — decompress and count rows; must match exported count
  6. DELETE  — DELETE rows from source DB (only if delete_after_archive = true)
  7. LOG     — write JSON run log to log_dir

Requirements:
  pip install psycopg2-binary
  pip install tomli   # Python < 3.11 only (3.11+ has tomllib built in)

Usage:
  python pfc_archiver_questdb.py --config config/questdb.toml
  python pfc_archiver_questdb.py --config config/questdb.toml --dry-run
  python pfc_archiver_questdb.py --config config/questdb.toml --once
"""

__version__ = "0.1.0"

import argparse
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# psycopg2 is optional at import time — installed separately per the README.
# Imported at module level so tests can patch it via @patch("pfc_archiver_questdb.psycopg2").
try:
    import psycopg2
except ImportError:
    psycopg2 = None  # type: ignore[assignment]

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("pfc-archiver-questdb")


# ---------------------------------------------------------------------------
# Config loader
# ---------------------------------------------------------------------------

def load_config(path: str) -> dict:
    """Load TOML config file. Returns dict."""
    try:
        import tomllib                  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib     # pip install tomli for <3.11
        except ImportError:
            raise ImportError(
                "tomllib not available. On Python < 3.11: pip install tomli"
            )

    with open(path, "rb") as f:
        cfg = tomllib.load(f)

    # Validate required fields
    required = [
        ("db", "host"),
        ("db", "table"),
        ("db", "ts_column"),
        ("archive", "retention_days"),
        ("archive", "output_dir"),
    ]
    for section, key in required:
        if section not in cfg or key not in cfg[section]:
            raise ValueError(f"Missing required config: [{section}] {key}")

    return cfg


# ---------------------------------------------------------------------------
# Database connector (QuestDB via PostgreSQL wire protocol, port 8812)
# ---------------------------------------------------------------------------

def _connect(db_cfg: dict):
    """Create a psycopg2 connection from [db] config section."""
    if psycopg2 is None:
        log.error("psycopg2 not installed. Run: pip install psycopg2-binary")
        sys.exit(1)

    return psycopg2.connect(
        host=db_cfg.get("host", "localhost"),
        port=db_cfg.get("port", 8812),        # QuestDB PostgreSQL wire port
        user=db_cfg.get("user", "admin"),      # QuestDB default user
        password=db_cfg.get("password", "quest"),  # QuestDB default password
        dbname=db_cfg.get("dbname", "qdb"),    # QuestDB default database
        connect_timeout=30,
    )


def get_partition_ranges(db_cfg: dict, retention_days: int, partition_days: int) -> list:
    """
    Find time partitions older than retention_days that haven't been archived yet.

    Returns list of (from_ts, to_ts) tuples — each partition_days wide.

    Strategy:
      - Find MIN(ts_column) in the table
      - Walk forward in partition_days steps
      - Stop at (now - retention_days)  → everything beyond that is still "hot"

    Note: QuestDB does not use schemas — tables are referenced by name only.
    """
    conn = _connect(db_cfg)
    conn.autocommit = True

    table     = db_cfg["table"]
    ts_col    = db_cfg["ts_column"]
    cutoff_ts = datetime.now(timezone.utc) - timedelta(days=retention_days)

    try:
        cur = conn.cursor()
        cur.execute(f'SELECT MIN("{ts_col}"), MAX("{ts_col}") FROM "{table}"')
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row or row[0] is None:
        log.info("Table is empty — nothing to archive")
        return []

    min_ts, max_ts = row

    # Normalize to UTC-aware datetimes.
    # QuestDB returns timestamps as datetime objects without tzinfo via
    # the PostgreSQL wire protocol — treat them as UTC.
    if isinstance(min_ts, (int, float)):
        min_ts = datetime.fromtimestamp(min_ts / 1_000_000, tz=timezone.utc)
    elif min_ts.tzinfo is None:
        min_ts = min_ts.replace(tzinfo=timezone.utc)

    # Walk forward in partition_days steps, stay below cutoff
    partitions = []
    current = min_ts.replace(hour=0, minute=0, second=0, microsecond=0)

    while current < cutoff_ts:
        next_ts = current + timedelta(days=partition_days)
        if next_ts > cutoff_ts:
            next_ts = cutoff_ts
        partitions.append((current, next_ts))
        current = next_ts

    log.info(
        f"Found {len(partitions)} partition(s) to archive "
        f"(table min={min_ts.date()}, cutoff={cutoff_ts.date()})"
    )
    return partitions


# ---------------------------------------------------------------------------
# Export: QuestDB partition → JSONL → PFC
# ---------------------------------------------------------------------------

def export_partition_to_pfc(
    db_cfg: dict,
    from_ts: datetime,
    to_ts: datetime,
    output_path: Path,
    pfc_binary: str,
    batch_size: int = 10_000,
    dry_run: bool = False,
) -> dict:
    """
    Stream one time partition from QuestDB → PFC archive.

    Returns: {"rows": int, "jsonl_mb": float, "output_mb": float, "ratio_pct": float}
    """
    table  = db_cfg["table"]
    ts_col = db_cfg["ts_column"]

    query = (
        f'SELECT * FROM "{table}" '
        f'WHERE "{ts_col}" >= %s AND "{ts_col}" < %s '
        f'ORDER BY "{ts_col}"'
    )

    if dry_run:
        log.info(f"  [DRY-RUN] would export: {from_ts.date()} → {to_ts.date()} → {output_path.name}")
        return {"rows": 0, "jsonl_mb": 0, "output_mb": 0, "ratio_pct": 0}

    log.info(f"  Exporting {from_ts.date()} → {to_ts.date()} ...")

    conn = _connect(db_cfg)
    conn.autocommit = True

    tmp_fd, tmp_jsonl = tempfile.mkstemp(suffix=".jsonl", prefix="pfc_arch_")
    os.close(tmp_fd)

    row_count  = 0
    jsonl_bytes = 0

    try:
        cur = conn.cursor()
        cur.execute(query, (from_ts.isoformat(), to_ts.isoformat()))

        col_names = [desc[0] for desc in cur.description]

        with open(tmp_jsonl, "w", encoding="utf-8") as fout:
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                for raw_row in batch:
                    row_dict = {}
                    for col, val in zip(col_names, raw_row):
                        if isinstance(val, datetime):
                            val = val.isoformat()
                        elif isinstance(val, bytes):
                            val = val.hex()
                        row_dict[col] = val

                    # Ensure pfc_jsonl can build its timestamp index.
                    # pfc_jsonl recognises "timestamp" / "@timestamp" — not
                    # arbitrary column names. Add "timestamp" as alias so
                    # pfc_jsonl query / s3-fetch --from --to work correctly.
                    if ts_col in row_dict and "timestamp" not in row_dict:
                        row_dict["timestamp"] = row_dict[ts_col]

                    line = json.dumps(row_dict, ensure_ascii=False) + "\n"
                    fout.write(line)
                    jsonl_bytes += len(line.encode("utf-8"))
                    row_count += 1

                if row_count % 500_000 == 0 and row_count > 0:
                    log.info(
                        f"    {row_count:,} rows  "
                        f"({jsonl_bytes / 1_048_576:.1f} MiB) ..."
                    )

        cur.close()
        conn.close()

        log.info(
            f"  Exported {row_count:,} rows  "
            f"({jsonl_bytes / 1_048_576:.1f} MiB JSONL) — compressing ..."
        )

        # Guard: nothing to compress
        if row_count == 0 or jsonl_bytes == 0:
            log.info("  No data in partition range — skipping.")
            return {"rows": 0, "jsonl_mb": 0.0, "output_mb": 0.0,
                    "ratio_pct": 0.0, "output": str(output_path), "skipped": True}

        # Compress JSONL → PFC
        output_path.parent.mkdir(parents=True, exist_ok=True)
        proc = subprocess.run(
            [pfc_binary, "compress", tmp_jsonl, str(output_path)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"pfc_jsonl compress failed: {proc.stderr.strip()}"
            )

        jsonl_mb  = jsonl_bytes / 1_048_576
        output_mb = output_path.stat().st_size / 1_048_576
        ratio_pct = (output_mb / jsonl_mb * 100) if jsonl_mb > 0 else 0.0

        log.info(
            f"  ✓ {row_count:,} rows  |  "
            f"JSONL {jsonl_mb:.1f} MiB → PFC {output_mb:.1f} MiB  "
            f"({ratio_pct:.1f}%)  →  {output_path.name}"
        )

        return {
            "rows":      row_count,
            "jsonl_mb":  jsonl_mb,
            "output_mb": output_mb,
            "ratio_pct": ratio_pct,
        }

    except Exception:
        conn.close()
        raise
    finally:
        if os.path.exists(tmp_jsonl):
            os.unlink(tmp_jsonl)


# ---------------------------------------------------------------------------
# Upload: copy .pfc + .bidx to S3 or local destination
# ---------------------------------------------------------------------------

def upload_archive(local_pfc: Path, archive_cfg: dict, dry_run: bool = False) -> bool:
    """
    Copy the .pfc (and .pfc.bidx index) to the configured destination.

    Supports:
      output_dir = "s3://my-bucket/cold-storage/"   → S3 upload
      output_dir = "/mnt/nas/pfc-archives/"         → local copy
    """
    dest = archive_cfg.get("output_dir", ".")

    local_bidx = Path(str(local_pfc) + ".bidx")

    if dest.startswith("s3://"):
        s3_path   = dest[5:]
        bucket, _, prefix = s3_path.partition("/")
        prefix = prefix.rstrip("/")

        key_pfc  = f"{prefix}/{local_pfc.name}"  if prefix else local_pfc.name
        key_bidx = f"{prefix}/{local_bidx.name}" if prefix else local_bidx.name

        if dry_run:
            log.info(f"  [DRY-RUN] would upload: s3://{bucket}/{key_pfc}")
            return True

        try:
            import boto3
        except ImportError:
            log.error("boto3 required for S3 upload: pip install boto3")
            raise

        kwargs = {}
        if archive_cfg.get("s3_region"):
            kwargs["region_name"] = archive_cfg["s3_region"]
        if archive_cfg.get("s3_endpoint"):
            kwargs["endpoint_url"] = archive_cfg["s3_endpoint"]
        if archive_cfg.get("s3_access_key"):
            kwargs["aws_access_key_id"]     = archive_cfg["s3_access_key"]
            kwargs["aws_secret_access_key"] = archive_cfg["s3_secret_key"]

        s3 = boto3.client("s3", **kwargs)
        log.info(f"  Uploading s3://{bucket}/{key_pfc} ...")
        s3.upload_file(str(local_pfc), bucket, key_pfc)
        if local_bidx.exists():
            s3.upload_file(str(local_bidx), bucket, key_bidx)
            log.info(f"  Uploading s3://{bucket}/{key_bidx}  (index)")
        log.info("  ✓ S3 upload complete")

    else:
        dest_dir = Path(dest)
        dest_dir.mkdir(parents=True, exist_ok=True)

        if dry_run:
            log.info(f"  [DRY-RUN] would copy to: {dest_dir}")
            return True

        shutil.copy2(local_pfc, dest_dir / local_pfc.name)
        if local_bidx.exists():
            shutil.copy2(local_bidx, dest_dir / local_bidx.name)
        log.info(f"  ✓ Copied to {dest_dir}")

    return True


# ---------------------------------------------------------------------------
# Verify: row count in PFC == rows exported
# ---------------------------------------------------------------------------

def verify_archive(local_pfc: Path, expected_rows: int, pfc_binary: str) -> bool:
    """
    Verify the PFC archive by counting rows (decompress + count JSONL lines).

    Returns True if counts match. Raises on mismatch.
    """
    log.info(f"  Verifying {local_pfc.name} (expected {expected_rows:,} rows) ...")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_out = Path(tmpdir) / "verify.jsonl"

        proc = subprocess.run(
            [pfc_binary, "decompress", str(local_pfc), str(tmp_out)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"Decompression failed during verify: {proc.stderr.strip()}"
            )

        actual_rows = sum(1 for _ in open(tmp_out, encoding="utf-8"))

    if actual_rows != expected_rows:
        raise RuntimeError(
            f"VERIFY FAILED: exported {expected_rows:,} rows, "
            f"archive contains {actual_rows:,} rows"
        )

    log.info(f"  ✓ Verified: {actual_rows:,} rows match")
    return True


# ---------------------------------------------------------------------------
# Delete: remove archived partition from source DB
# ---------------------------------------------------------------------------

def delete_partition(
    db_cfg: dict,
    from_ts: datetime,
    to_ts: datetime,
    dry_run: bool = False,
):
    """
    Delete the archived time range from the source database.

    QuestDB: uses DELETE WHERE ts_column >= from_ts AND ts_column < to_ts.
    Deletion only runs after a successful row-count verify.

    Note: QuestDB DELETE support requires version 7.3.0+.
    """
    table  = db_cfg["table"]
    ts_col = db_cfg["ts_column"]

    sql = (
        f'DELETE FROM "{table}" '
        f'WHERE "{ts_col}" >= %s AND "{ts_col}" < %s'
    )

    if dry_run:
        log.info(
            f"  [DRY-RUN] would DELETE: {from_ts.date()} → {to_ts.date()} "
            f'from "{table}"'
        )
        return

    log.info(f'  Deleting {from_ts.date()} → {to_ts.date()} from "{table}" ...')

    conn = _connect(db_cfg)
    try:
        cur = conn.cursor()
        cur.execute(sql, (from_ts.isoformat(), to_ts.isoformat()))
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        if deleted >= 0:
            log.info(f"  ✓ Deleted {deleted:,} rows from source DB")
        else:
            log.info("  ✓ Delete executed (QuestDB may not report exact rowcount)")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Archive run log
# ---------------------------------------------------------------------------

def write_run_log(log_dir: str, partition: dict):
    """Append a JSON log entry for this archive run."""
    log_dir  = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "archive_runs.jsonl"

    entry = {
        "ts": datetime.now(timezone.utc).isoformat(),
        **partition,
    }
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")


# ---------------------------------------------------------------------------
# Main archive loop
# ---------------------------------------------------------------------------

def archive_cycle(cfg: dict, pfc_binary: str, dry_run: bool = False):
    """
    One full archive cycle:
      1. Find old partitions
      2. For each: export → upload → verify → delete → log
    """
    db_cfg   = cfg["db"]
    arch_cfg = cfg["archive"]

    retention_days = int(arch_cfg["retention_days"])
    partition_days = int(arch_cfg.get("partition_days", 1))
    verify_enabled = arch_cfg.get("verify", True)
    delete_enabled = arch_cfg.get("delete_after_archive", False)
    log_dir        = arch_cfg.get("log_dir", "./archive_logs")

    # Step 1: find partitions
    try:
        partitions = get_partition_ranges(db_cfg, retention_days, partition_days)
    except Exception as exc:
        log.error(f"Scan failed — could not query QuestDB: {exc}")
        return

    if not partitions:
        log.info("No partitions to archive — sleeping until next cycle")
        return

    table = db_cfg["table"]

    for from_ts, to_ts in partitions:
        date_str = from_ts.strftime("%Y%m%d")
        date_end = to_ts.strftime("%Y%m%d")
        pfc_name = f"{table}__{date_str}__{date_end}.pfc"

        with tempfile.TemporaryDirectory(prefix="pfc_arch_") as tmpdir:
            local_pfc = Path(tmpdir) / pfc_name

            log.info(f"\n── Partition {from_ts.date()} → {to_ts.date()} ──")

            # Step 2: Export
            try:
                stats = export_partition_to_pfc(
                    db_cfg, from_ts, to_ts, local_pfc,
                    pfc_binary, dry_run=dry_run,
                )
            except Exception as exc:
                log.error(f"  EXPORT FAILED: {exc}")
                continue

            if dry_run:
                continue

            if stats.get("rows", 0) == 0:
                log.info("  Empty partition — skipping upload/delete")
                continue

            # Step 3: Upload
            try:
                upload_archive(local_pfc, arch_cfg, dry_run=dry_run)
            except Exception as exc:
                log.error(f"  UPLOAD FAILED: {exc}")
                continue

            # Step 4: Verify (optional but strongly recommended)
            if verify_enabled and not dry_run:
                try:
                    verify_archive(local_pfc, stats["rows"], pfc_binary)
                except Exception as exc:
                    log.error(f"  VERIFY FAILED — skipping delete: {exc}")
                    write_run_log(log_dir, {
                        "status":  "verify_failed",
                        "table":   table,
                        "from_ts": from_ts.isoformat(),
                        "to_ts":   to_ts.isoformat(),
                        "error":   str(exc),
                    })
                    continue

            # Step 5: Delete from source DB (only if verify passed)
            if delete_enabled and not dry_run:
                try:
                    delete_partition(db_cfg, from_ts, to_ts, dry_run=dry_run)
                except Exception as exc:
                    log.error(f"  DELETE FAILED: {exc}")
                    write_run_log(log_dir, {
                        "status":  "delete_failed",
                        "table":   table,
                        "from_ts": from_ts.isoformat(),
                        "to_ts":   to_ts.isoformat(),
                        "error":   str(exc),
                    })
                    continue

            # Step 6: Log success
            write_run_log(log_dir, {
                "status":  "ok",
                "table":   table,
                "from_ts": from_ts.isoformat(),
                "to_ts":   to_ts.isoformat(),
                **stats,
                "deleted": delete_enabled,
            })

    log.info("\nCycle complete.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        prog="pfc-archiver-questdb",
        description="Automated PFC cold-storage archiving daemon for QuestDB",
    )
    parser.add_argument(
        "--config", "-c",
        required=True, metavar="FILE",
        help="Path to TOML config file",
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Simulate run: find partitions but do not export, upload, or delete",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run one archive cycle then exit (no daemon loop)",
    )
    parser.add_argument(
        "--pfc-binary",
        default=None, metavar="PATH",
        help="Path to pfc_jsonl binary (default: auto-detect)",
    )
    parser.add_argument(
        "--version", action="version", version=f"pfc-archiver-questdb {__version__}"
    )
    args = parser.parse_args()

    # Load config
    cfg = load_config(args.config)

    # Locate pfc_jsonl binary
    pfc_binary = args.pfc_binary
    if not pfc_binary:
        import shutil
        pfc_binary = shutil.which("pfc_jsonl") or "/usr/local/bin/pfc_jsonl"
    if not os.path.isfile(pfc_binary):
        log.error(
            f"pfc_jsonl binary not found at: {pfc_binary}\n"
            "Install: https://github.com/ImpossibleForge/pfc-jsonl/releases"
        )
        sys.exit(1)

    interval_s = int(cfg.get("daemon", {}).get("interval_seconds", 3600))

    log.info(f"pfc-archiver-questdb {__version__} starting  |  config: {args.config}")
    log.info(
        f"DB: {cfg['db'].get('host')}:{cfg['db'].get('port', 8812)}"
        f"  table: {cfg['db']['table']}"
        f"  retention: {cfg['archive']['retention_days']}d"
    )
    if args.dry_run:
        log.info("DRY-RUN mode — no data will be moved or deleted")

    # Graceful shutdown on SIGTERM / SIGINT
    running = {"ok": True}
    def _stop(sig, frame):
        log.info("Signal received — shutting down after current cycle")
        running["ok"] = False
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT,  _stop)

    # Run
    while running["ok"]:
        try:
            archive_cycle(cfg, pfc_binary, dry_run=args.dry_run)
        except Exception as exc:
            log.error(f"Cycle failed: {exc}", exc_info=True)

        if args.once or not running["ok"]:
            break

        log.info(f"Next cycle in {interval_s}s  (Ctrl-C to stop)")
        for _ in range(interval_s):
            if not running["ok"]:
                break
            time.sleep(1)

    log.info("pfc-archiver-questdb stopped.")


if __name__ == "__main__":
    main()
