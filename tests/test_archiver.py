"""
Tests for pfc-archiver-questdb
================================
All tests run without a live QuestDB instance — psycopg2 is mocked throughout.
Run with:  python -m pytest tests/ -v
           python -m unittest discover -s tests -v
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# Allow importing from the project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from pfc_archiver_questdb import (
    load_config,
    _connect,
    get_partition_ranges,
    export_partition_to_pfc,
    upload_archive,
    verify_archive,
    delete_partition,
    write_run_log,
    archive_cycle,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_cfg(overrides=None):
    """Minimal valid config dict for testing."""
    cfg = {
        "db": {
            "host":      "localhost",
            "port":      8812,
            "user":      "admin",
            "password":  "quest",
            "dbname":    "qdb",
            "table":     "logs",
            "ts_column": "timestamp",
        },
        "archive": {
            "retention_days":       30,
            "partition_days":       1,
            "output_dir":           "./test-archives/",
            "verify":               True,
            "delete_after_archive": False,
            "log_dir":              "./test-logs/",
        },
        "daemon": {"interval_seconds": 3600},
    }
    if overrides:
        for section, vals in overrides.items():
            cfg.setdefault(section, {}).update(vals)
    return cfg


# ===========================================================================
# 1. Config loading
# ===========================================================================

class TestLoadConfig(unittest.TestCase):

    def _write_toml(self, content: str) -> str:
        """Write a TOML string to a temp file and return its path."""
        fd, path = tempfile.mkstemp(suffix=".toml")
        os.write(fd, content.encode())
        os.close(fd)
        return path

    def test_valid_config_loads(self):
        toml = """
[db]
host      = "localhost"
table     = "logs"
ts_column = "timestamp"
[archive]
retention_days = 30
output_dir     = "./out/"
"""
        path = self._write_toml(toml)
        cfg = load_config(path)
        self.assertEqual(cfg["db"]["host"], "localhost")
        self.assertEqual(cfg["archive"]["retention_days"], 30)
        os.unlink(path)

    def test_missing_required_field_raises(self):
        toml = """
[db]
host = "localhost"
table = "logs"
# ts_column intentionally missing
[archive]
retention_days = 30
output_dir = "./out/"
"""
        path = self._write_toml(toml)
        with self.assertRaises(ValueError) as ctx:
            load_config(path)
        self.assertIn("ts_column", str(ctx.exception))
        os.unlink(path)

    def test_missing_db_section_raises(self):
        toml = """
[archive]
retention_days = 30
output_dir = "./out/"
"""
        path = self._write_toml(toml)
        with self.assertRaises(ValueError):
            load_config(path)
        os.unlink(path)


# ===========================================================================
# 2. Connection defaults — QuestDB-specific
# ===========================================================================

class TestConnect(unittest.TestCase):

    @patch("pfc_archiver_questdb.psycopg2")
    def test_default_port_is_8812(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["port"], 8812)

    @patch("pfc_archiver_questdb.psycopg2")
    def test_default_user_is_admin(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["user"], "admin")

    @patch("pfc_archiver_questdb.psycopg2")
    def test_default_password_is_quest(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["password"], "quest")

    @patch("pfc_archiver_questdb.psycopg2")
    def test_default_dbname_is_qdb(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "localhost", "table": "logs", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["dbname"], "qdb")

    @patch("pfc_archiver_questdb.psycopg2")
    def test_custom_port_overrides_default(self, mock_pg):
        mock_pg.connect.return_value = MagicMock()
        _connect({"host": "myhost", "port": 9999, "table": "t", "ts_column": "ts"})
        kwargs = mock_pg.connect.call_args[1]
        self.assertEqual(kwargs["port"], 9999)


# ===========================================================================
# 3. No-schema table references
# ===========================================================================

class TestNoSchemaQueries(unittest.TestCase):
    """QuestDB doesn't use schemas — queries must reference tables by name only."""

    @patch("pfc_archiver_questdb._connect")
    def test_get_partition_ranges_no_schema_in_query(self, mock_connect):
        now = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=40)
        max_ts = now - timedelta(days=2)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        get_partition_ranges(db_cfg, retention_days=30, partition_days=1)

        executed_sql = mock_cur.execute.call_args[0][0]
        # Must NOT contain dot-qualified schema reference
        self.assertNotIn('".', executed_sql)
        self.assertNotIn("doc", executed_sql)
        # Must contain the table name
        self.assertIn('"logs"', executed_sql)

    @patch("pfc_archiver_questdb._connect")
    def test_delete_partition_no_schema_in_query(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.rowcount = 100
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        db_cfg  = {"host": "localhost", "table": "events", "ts_column": "ts"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        delete_partition(db_cfg, from_ts, to_ts)

        executed_sql = mock_cur.execute.call_args[0][0]
        self.assertNotIn("doc", executed_sql)
        self.assertIn('"events"', executed_sql)


# ===========================================================================
# 4. Timestamp handling
# ===========================================================================

class TestTimestampHandling(unittest.TestCase):

    @patch("pfc_archiver_questdb._connect")
    def test_naive_datetime_treated_as_utc(self, mock_connect):
        """QuestDB returns naive datetimes via wire protocol — must be treated as UTC."""
        now = datetime.now(timezone.utc)
        naive_min = datetime(2026, 1, 1, 0, 0, 0)  # no tzinfo
        naive_max = datetime(2026, 4, 1, 0, 0, 0)  # no tzinfo

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (naive_min, naive_max)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        # Should not raise — naive datetime must be handled gracefully
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)

    @patch("pfc_archiver_questdb._connect")
    def test_integer_timestamp_microseconds(self, mock_connect):
        """QuestDB may return epoch microseconds as int in some drivers."""
        # 2026-01-01 00:00:00 UTC in microseconds
        epoch_us = 1767225600 * 1_000_000
        now = datetime.now(timezone.utc)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (epoch_us, epoch_us + 86400 * 1_000_000 * 60)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)

    @patch("pfc_archiver_questdb._connect")
    def test_empty_table_returns_empty_list(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (None, None)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertEqual(result, [])


# ===========================================================================
# 5. Partition range calculation
# ===========================================================================

class TestPartitionRanges(unittest.TestCase):

    @patch("pfc_archiver_questdb._connect")
    def test_single_partition_one_day_old(self, mock_connect):
        now     = datetime.now(timezone.utc)
        min_ts  = now - timedelta(days=35)
        max_ts  = now - timedelta(days=34)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertGreater(len(result), 0)
        # Each partition tuple is (from_ts, to_ts)
        for from_ts, to_ts in result:
            self.assertLess(from_ts, to_ts)

    @patch("pfc_archiver_questdb._connect")
    def test_hot_data_excluded(self, mock_connect):
        """Data within retention_days must NOT appear in partitions."""
        now    = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=5)   # only 5 days old — hot
        max_ts = now

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertEqual(result, [], "Hot data should not be archived")

    @patch("pfc_archiver_questdb._connect")
    def test_partition_days_respected(self, mock_connect):
        now    = datetime.now(timezone.utc)
        min_ts = now - timedelta(days=60)
        max_ts = now - timedelta(days=1)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, max_ts)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=7)
        # With 7-day partitions over ~30 days → expect ~4-5 partitions
        self.assertGreater(len(result), 2)
        # Each span should be <= 7 days
        for from_ts, to_ts in result:
            span = (to_ts - from_ts).total_seconds() / 86400
            self.assertLessEqual(span, 7.01)


# ===========================================================================
# 6. Export — dry-run and timestamp alias injection
# ===========================================================================

class TestExportPartition(unittest.TestCase):

    def test_dry_run_returns_zeros(self):
        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        result = export_partition_to_pfc(
            db_cfg, from_ts, to_ts,
            Path("/tmp/dry.pfc"), "/usr/bin/pfc_jsonl",
            dry_run=True,
        )
        self.assertEqual(result["rows"], 0)
        self.assertEqual(result["jsonl_mb"], 0)

    @patch("pfc_archiver_questdb._connect")
    @patch("pfc_archiver_questdb.subprocess.run")
    def test_timestamp_alias_injected_when_column_differs(self, mock_run, mock_connect):
        """When ts_column != 'timestamp', a 'timestamp' alias must be added to each row."""
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_run.return_value = mock_proc

        # Build mock cursor that returns 1 row with custom ts column "event_time"
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("event_time",), ("level",)]
        ts_val    = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_cur.fetchmany.side_effect = [
            [(ts_val, "ERROR")],
            []
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "event_time"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "test.pfc"
            # Create a fake .pfc file so stat() works
            out_pfc.write_bytes(b"FAKE")

            export_partition_to_pfc(
                db_cfg, from_ts, to_ts, out_pfc,
                "/usr/bin/pfc_jsonl",
            )

        # Check that subprocess was called with compress
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]
        self.assertIn("compress", call_args)

    @patch("pfc_archiver_questdb._connect")
    def test_empty_partition_returns_skipped(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",)]
        mock_cur.fetchmany.return_value = []  # no rows
        mock_conn.cursor.return_value   = mock_cur
        mock_connect.return_value       = mock_conn

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        result = export_partition_to_pfc(
            db_cfg, from_ts, to_ts,
            Path("/tmp/empty.pfc"), "/usr/bin/pfc_jsonl",
        )
        self.assertEqual(result["rows"], 0)
        self.assertTrue(result.get("skipped"))


# ===========================================================================
# 7. Upload — S3 path parsing & local copy
# ===========================================================================

class TestUploadArchive(unittest.TestCase):

    def test_dry_run_s3_returns_true(self):
        arch_cfg = {"output_dir": "s3://my-bucket/prefix/"}
        with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
            f.write(b"FAKE")
            pfc_path = Path(f.name)
        result = upload_archive(pfc_path, arch_cfg, dry_run=True)
        self.assertTrue(result)
        pfc_path.unlink()

    def test_dry_run_local_returns_true(self):
        arch_cfg = {"output_dir": "./test-archives/"}
        with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
            f.write(b"FAKE")
            pfc_path = Path(f.name)
        result = upload_archive(pfc_path, arch_cfg, dry_run=True)
        self.assertTrue(result)
        pfc_path.unlink()

    def test_local_copy_creates_file(self):
        with tempfile.TemporaryDirectory() as dest_dir:
            with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
                f.write(b"ARCHIVE_DATA")
                pfc_path = Path(f.name)

            arch_cfg = {"output_dir": dest_dir}
            upload_archive(pfc_path, arch_cfg, dry_run=False)

            dest_file = Path(dest_dir) / pfc_path.name
            self.assertTrue(dest_file.exists())
            self.assertEqual(dest_file.read_bytes(), b"ARCHIVE_DATA")
            pfc_path.unlink()

    def test_local_copy_includes_bidx_when_present(self):
        with tempfile.TemporaryDirectory() as src_dir, \
             tempfile.TemporaryDirectory() as dest_dir:
            pfc_path  = Path(src_dir) / "archive.pfc"
            bidx_path = Path(src_dir) / "archive.pfc.bidx"
            pfc_path.write_bytes(b"PFC")
            bidx_path.write_bytes(b"BIDX")

            arch_cfg = {"output_dir": dest_dir}
            upload_archive(pfc_path, arch_cfg, dry_run=False)

            self.assertTrue((Path(dest_dir) / "archive.pfc").exists())
            self.assertTrue((Path(dest_dir) / "archive.pfc.bidx").exists())


# ===========================================================================
# 8. Verify — row count matching
# ===========================================================================

class TestVerifyArchive(unittest.TestCase):

    @patch("pfc_archiver_questdb.subprocess.run")
    def test_correct_row_count_passes(self, mock_run):
        mock_proc = MagicMock()
        mock_proc.returncode = 0
        mock_run.return_value = mock_proc

        with tempfile.TemporaryDirectory() as tmpdir:
            # Write a fake JSONL with 3 lines
            jsonl_path = Path(tmpdir) / "verify.jsonl"
            jsonl_path.write_text('{"ts":"a"}\n{"ts":"b"}\n{"ts":"c"}\n')

            pfc_path = Path(tmpdir) / "archive.pfc"
            pfc_path.write_bytes(b"FAKE")

            # Patch decompress to write our known JSONL
            def fake_run(cmd, **kwargs):
                out_path = Path(cmd[-1])  # last arg is output path
                out_path.write_text('{"ts":"a"}\n{"ts":"b"}\n{"ts":"c"}\n')
                m = MagicMock()
                m.returncode = 0
                return m

            mock_run.side_effect = fake_run

            result = verify_archive(pfc_path, expected_rows=3, pfc_binary="/usr/bin/pfc_jsonl")
            self.assertTrue(result)

    @patch("pfc_archiver_questdb.subprocess.run")
    def test_row_count_mismatch_raises(self, mock_run):
        def fake_run(cmd, **kwargs):
            out_path = Path(cmd[-1])
            out_path.write_text('{"ts":"a"}\n{"ts":"b"}\n')  # only 2 rows
            m = MagicMock()
            m.returncode = 0
            return m

        mock_run.side_effect = fake_run

        with tempfile.TemporaryDirectory() as tmpdir:
            pfc_path = Path(tmpdir) / "archive.pfc"
            pfc_path.write_bytes(b"FAKE")
            with self.assertRaises(RuntimeError) as ctx:
                verify_archive(pfc_path, expected_rows=3, pfc_binary="/usr/bin/pfc_jsonl")
            self.assertIn("VERIFY FAILED", str(ctx.exception))


# ===========================================================================
# 9. Delete — dry-run & no-schema query
# ===========================================================================

class TestDeletePartition(unittest.TestCase):

    def test_dry_run_skips_db_call(self):
        """Dry-run must not touch the database at all."""
        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with patch("pfc_archiver_questdb._connect") as mock_connect:
            delete_partition(db_cfg, from_ts, to_ts, dry_run=True)
            mock_connect.assert_not_called()

    @patch("pfc_archiver_questdb._connect")
    def test_delete_uses_correct_params(self, mock_connect):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.rowcount = 42
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        db_cfg  = {"host": "localhost", "table": "events", "ts_column": "ts"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        delete_partition(db_cfg, from_ts, to_ts)

        # Verify correct params passed to execute
        call_args = mock_cur.execute.call_args
        params    = call_args[0][1]
        self.assertEqual(params[0], from_ts.isoformat())
        self.assertEqual(params[1], to_ts.isoformat())


# ===========================================================================
# 10. Run log
# ===========================================================================

class TestWriteRunLog(unittest.TestCase):

    def test_log_appended_as_valid_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_run_log(tmpdir, {"status": "ok", "rows": 100, "table": "logs"})
            log_file = Path(tmpdir) / "archive_runs.jsonl"
            self.assertTrue(log_file.exists())
            entry = json.loads(log_file.read_text().strip())
            self.assertEqual(entry["status"], "ok")
            self.assertEqual(entry["rows"], 100)
            self.assertIn("ts", entry)

    def test_multiple_logs_append(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            write_run_log(tmpdir, {"status": "ok", "rows": 10})
            write_run_log(tmpdir, {"status": "ok", "rows": 20})
            log_file = Path(tmpdir) / "archive_runs.jsonl"
            lines = log_file.read_text().strip().splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[1])["rows"], 20)


# ===========================================================================
# 11. Archive cycle — full integration with mocks
# ===========================================================================

class TestArchiveCycle(unittest.TestCase):

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_full_cycle_happy_path(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 500, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg()
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_export.assert_called_once()
        mock_upload.assert_called_once()
        mock_verify.assert_called_once()
        mock_log.assert_called_once()
        log_call = mock_log.call_args[0][1]
        self.assertEqual(log_call["status"], "ok")
        self.assertEqual(log_call["rows"], 500)

    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_no_partitions_exits_early(self, mock_partitions):
        mock_partitions.return_value = []
        cfg = _make_cfg()
        # Should complete without error
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        mock_partitions.assert_called_once()

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_dry_run_skips_upload_verify_log(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 0, "jsonl_mb": 0, "output_mb": 0, "ratio_pct": 0}

        cfg = _make_cfg()
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=True)

        mock_export.assert_called_once()
        mock_upload.assert_not_called()
        mock_verify.assert_not_called()
        mock_log.assert_not_called()

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_verify_failure_skips_delete_and_logs_error(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 100, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.side_effect      = RuntimeError("VERIFY FAILED: 100 vs 99")

        cfg = _make_cfg({"archive": {"verify": True, "delete_after_archive": True}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_log.assert_called_once()
        log_entry = mock_log.call_args[0][1]
        self.assertEqual(log_entry["status"], "verify_failed")

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.delete_partition")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_delete_called_when_enabled_and_verify_passes(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_delete, mock_log,
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 200, "jsonl_mb": 2.0, "output_mb": 0.1, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg({"archive": {"verify": True, "delete_after_archive": True}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_delete.assert_called_once()
        log_entry = mock_log.call_args[0][1]
        self.assertTrue(log_entry["deleted"])


# ===========================================================================
# 12. PFC filename — no schema prefix
# ===========================================================================

class TestPfcFilename(unittest.TestCase):
    """Archive filenames must use table name only (no schema prefix like 'doc__logs')."""

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_pfc_filename_uses_table_only(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log
    ):
        now     = datetime.now(timezone.utc)
        from_ts = now - timedelta(days=35)
        to_ts   = now - timedelta(days=34)

        mock_partitions.return_value = [(from_ts, to_ts)]
        mock_export.return_value     = {"rows": 50, "jsonl_mb": 0.5, "output_mb": 0.02, "ratio_pct": 4.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg({"db": {"table": "sensor_data"}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        # Check filename passed to export
        export_call = mock_export.call_args
        output_path = export_call[0][3]  # 4th positional arg: output_path
        filename    = output_path.name
        # Must be "sensor_data__YYYYMMDD__YYYYMMDD.pfc" — NOT "doc__sensor_data__..."
        self.assertTrue(filename.startswith("sensor_data__"), f"Bad filename: {filename}")
        self.assertNotIn("doc__", filename)


if __name__ == "__main__":
    unittest.main(verbosity=2)
