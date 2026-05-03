#!/usr/bin/env python3
"""
Integration tests for pfc-archiver-questdb — requires live QuestDB.

Run on the server:
  python3 tests/test_integration_questdb_archiver.py

QuestDB: localhost:8812 (Docker container quest-test)

Strategy: retention_days=0 so all test data is immediately "archivable".
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

QUEST_HOST = "localhost"
QUEST_PORT = 8812
QUEST_USER = "admin"
QUEST_PASS = "quest"
QUEST_DB   = "qdb"
PFC_BINARY = "/usr/local/bin/pfc_jsonl"
TABLE      = "pfc_archiver_integration_test"

try:
    import psycopg2
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


def pfc_binary_available():
    return os.path.isfile(PFC_BINARY) and os.access(PFC_BINARY, os.X_OK)


def get_conn():
    return psycopg2.connect(
        host=QUEST_HOST, port=QUEST_PORT,
        user=QUEST_USER, password=QUEST_PASS,
        dbname=QUEST_DB, connect_timeout=10,
    )


def make_cfg(output_dir, log_dir, delete_after=False, retention_days=0):
    return {
        "db": {
            "host":      QUEST_HOST,
            "port":      QUEST_PORT,
            "user":      QUEST_USER,
            "password":  QUEST_PASS,
            "dbname":    QUEST_DB,
            "table":     TABLE,
            "ts_column": "ts",
        },
        "archive": {
            "retention_days":       retention_days,
            "partition_days":       1,
            "output_dir":           str(output_dir),
            "verify":               True,
            "delete_after_archive": delete_after,
            "log_dir":              str(log_dir),
        },
        "pfc": {"binary": PFC_BINARY},
        "daemon": {"interval_seconds": 3600},
    }


@unittest.skipUnless(HAS_PSYCOPG2, "psycopg2 not installed")
@unittest.skipUnless(pfc_binary_available(), "pfc_jsonl binary not found")
class TestQuestDBArchiverIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.conn = get_conn()
        cls.conn.autocommit = True
        cur = cls.conn.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        except Exception:
            cls.conn.rollback()

        cur.execute(f"""
            CREATE TABLE {TABLE} (
                id      INT,
                ts      TIMESTAMP,
                level   SYMBOL,
                message STRING,
                value   DOUBLE
            ) TIMESTAMP(ts) PARTITION BY DAY
        """)

        # 3 days of data: day -4, -3, -2 (all clearly older than today)
        base = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=4)
        rows = []
        for day in range(3):
            for hour in range(8):
                ts = base + timedelta(days=day, hours=hour)
                i  = day * 8 + hour
                ts_us = int(ts.timestamp() * 1_000_000)
                rows.append((i, ts_us, ["INFO", "WARN", "ERROR"][i % 3],
                              f"log message {i}", float(i) * 1.5))

        for row in rows:
            cur.execute(
                f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s, %s)",
                row,
            )
        cur.close()
        cls.total_rows = len(rows)  # 24

    @classmethod
    def tearDownClass(cls):
        cur = cls.conn.cursor()
        try:
            cur.execute(f"DROP TABLE IF EXISTS {TABLE}")
        except Exception:
            pass
        cur.close()
        cls.conn.close()

    # ------------------------------------------------------------------
    # 1. SCAN finds correct partitions
    # ------------------------------------------------------------------
    def test_scan_finds_partitions(self):
        from pfc_archiver_questdb import get_partition_ranges
        cfg = make_cfg("/tmp", "/tmp")
        partitions = get_partition_ranges(cfg["db"], retention_days=0, partition_days=1)
        self.assertGreaterEqual(len(partitions), 3,
            f"Expected >= 3 partitions, got {len(partitions)}: {partitions}")

    # ------------------------------------------------------------------
    # 2. SCAN returns empty when all data is within retention window
    # ------------------------------------------------------------------
    def test_scan_empty_when_data_is_hot(self):
        from pfc_archiver_questdb import get_partition_ranges
        cfg = make_cfg("/tmp", "/tmp")
        partitions = get_partition_ranges(cfg["db"], retention_days=9999, partition_days=1)
        self.assertEqual(len(partitions), 0, "Expected 0 partitions (all data still hot)")

    # ------------------------------------------------------------------
    # 3. Full archive cycle — .pfc files created for all non-empty partitions
    # ------------------------------------------------------------------
    def test_full_archive_cycle_creates_pfc_files(self):
        from pfc_archiver_questdb import archive_cycle
        with tempfile.TemporaryDirectory() as out_dir, \
             tempfile.TemporaryDirectory() as log_dir:
            cfg = make_cfg(out_dir, log_dir, delete_after=False)
            archive_cycle(cfg, PFC_BINARY, dry_run=False)
            pfc_files = list(Path(out_dir).glob("*.pfc"))
            self.assertGreaterEqual(len(pfc_files), 3,
                f"Expected >= 3 .pfc files, got {len(pfc_files)}")

    # ------------------------------------------------------------------
    # 4. VERIFY passes — row counts match between DB and .pfc
    # ------------------------------------------------------------------
    def test_verify_row_count_matches(self):
        from pfc_archiver_questdb import get_partition_ranges, export_partition_to_pfc, verify_archive
        with tempfile.TemporaryDirectory() as out_dir:
            cfg    = make_cfg(out_dir, out_dir)
            db_cfg = cfg["db"]
            partitions = get_partition_ranges(db_cfg, retention_days=0, partition_days=1)
            total_archived = 0
            for from_ts, to_ts in partitions:
                out_path = Path(out_dir) / f"test_{from_ts.date()}.pfc"
                stats = export_partition_to_pfc(db_cfg, from_ts, to_ts, out_path, PFC_BINARY)
                if stats["rows"] == 0:
                    continue
                self.assertTrue(verify_archive(out_path, stats["rows"], PFC_BINARY),
                    f"Verify failed for partition {from_ts.date()}")
                total_archived += stats["rows"]
            self.assertEqual(total_archived, self.total_rows,
                f"Total archived {total_archived} != inserted {self.total_rows}")

    # ------------------------------------------------------------------
    # 5. .bidx files created for all partitions
    # ------------------------------------------------------------------
    def test_bidx_created_for_all_partitions(self):
        from pfc_archiver_questdb import archive_cycle
        with tempfile.TemporaryDirectory() as out_dir, \
             tempfile.TemporaryDirectory() as log_dir:
            cfg = make_cfg(out_dir, log_dir)
            archive_cycle(cfg, PFC_BINARY, dry_run=False)
            bidx_files = list(Path(out_dir).glob("*.pfc.bidx"))
            self.assertGreaterEqual(len(bidx_files), 3,
                f"Expected >= 3 .bidx files, got {len(bidx_files)}")

    # ------------------------------------------------------------------
    # 6. Run log written
    # ------------------------------------------------------------------
    def test_run_log_written(self):
        from pfc_archiver_questdb import archive_cycle
        with tempfile.TemporaryDirectory() as out_dir, \
             tempfile.TemporaryDirectory() as log_dir:
            cfg = make_cfg(out_dir, log_dir)
            archive_cycle(cfg, PFC_BINARY, dry_run=False)
            log_file = Path(log_dir) / "archive_runs.jsonl"
            self.assertTrue(log_file.exists(), "archive_runs.jsonl not created")
            with open(log_file) as f:
                entry = json.loads(f.readline())
            self.assertIn("status", entry)
            self.assertIn("rows", entry)

    # ------------------------------------------------------------------
    # 7. Dry-run — no files created
    # ------------------------------------------------------------------
    def test_dry_run_creates_no_files(self):
        from pfc_archiver_questdb import archive_cycle
        with tempfile.TemporaryDirectory() as out_dir, \
             tempfile.TemporaryDirectory() as log_dir:
            cfg = make_cfg(out_dir, log_dir)
            archive_cycle(cfg, PFC_BINARY, dry_run=True)
            pfc_files = list(Path(out_dir).glob("*.pfc"))
            self.assertEqual(len(pfc_files), 0,
                f"Dry-run should create no files, found: {pfc_files}")

    # ------------------------------------------------------------------
    # 8. delete_after_archive=True — partitions dropped from QuestDB
    #    QuestDB does not support row-level DELETE via PostgreSQL wire.
    #    The archiver uses ALTER TABLE DROP PARTITION instead.
    # ------------------------------------------------------------------
    def test_z_delete_after_archive(self):
        from pfc_archiver_questdb import archive_cycle, get_partition_ranges
        cfg = make_cfg("/tmp", "/tmp")
        partitions_before = get_partition_ranges(cfg["db"], retention_days=0, partition_days=1)
        self.assertGreaterEqual(len(partitions_before), 3)

        with tempfile.TemporaryDirectory() as out_dir, \
             tempfile.TemporaryDirectory() as log_dir:
            cfg = make_cfg(out_dir, log_dir, delete_after=True)
            archive_cycle(cfg, PFC_BINARY, dry_run=False)

        # After dropping partitions, scan should find 0 (or only today's empty window)
        cfg2 = make_cfg("/tmp", "/tmp")
        partitions_after = get_partition_ranges(cfg2["db"], retention_days=0, partition_days=1)
        non_empty = [p for p in partitions_after
                     if (p[1] - p[0]).total_seconds() >= 86400]
        self.assertEqual(len(non_empty), 0,
            f"Expected no full-day partitions after drop, got: {non_empty}")

        # Restore data for subsequent tests
        base = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=4)
        cur = self.conn.cursor()
        for day in range(3):
            for hour in range(8):
                ts = base + timedelta(days=day, hours=hour)
                i  = day * 8 + hour
                ts_us = int(ts.timestamp() * 1_000_000)
                cur.execute(
                    f"INSERT INTO {TABLE} VALUES (%s, %s, %s, %s, %s)",
                    (i, ts_us, ["INFO", "WARN", "ERROR"][i % 3],
                     f"log message {i}", float(i) * 1.5),
                )
        cur.close()

    # ------------------------------------------------------------------
    # 9. QuestDB-specific: SYMBOL type survives roundtrip as string
    # ------------------------------------------------------------------
    def test_symbol_type_preserved_in_pfc(self):
        import subprocess
        from pfc_archiver_questdb import export_partition_to_pfc, get_partition_ranges
        with tempfile.TemporaryDirectory() as out_dir:
            cfg    = make_cfg(out_dir, out_dir)
            db_cfg = cfg["db"]
            partitions = get_partition_ranges(db_cfg, retention_days=0, partition_days=1)
            # Take first non-empty partition
            for from_ts, to_ts in partitions:
                out_path = Path(out_dir) / f"symbol_test_{from_ts.date()}.pfc"
                stats = export_partition_to_pfc(db_cfg, from_ts, to_ts, out_path, PFC_BINARY)
                if stats["rows"] == 0:
                    continue
                result = subprocess.run(
                    [PFC_BINARY, "decompress", str(out_path), "-"],
                    capture_output=True, text=True,
                )
                lines = [l for l in result.stdout.splitlines() if l.startswith("{")]
                self.assertGreater(len(lines), 0)
                for line in lines:
                    record = json.loads(line)
                    self.assertIsInstance(record["level"], str,
                        f"SYMBOL 'level' should be string, got {type(record['level'])}")
                    self.assertIn(record["level"], ["INFO", "WARN", "ERROR"])
                break


if __name__ == "__main__":
    print(f"QuestDB Archiver Integration Tests — {QUEST_HOST}:{QUEST_PORT}")
    print(f"pfc_jsonl binary: {'found' if pfc_binary_available() else 'NOT FOUND'}")
    print("-" * 60)
    unittest.main(verbosity=2)
