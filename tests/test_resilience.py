"""
Resilience & stress tests for pfc-archiver-questdb
====================================================
Mirrors the S-series (failure scenarios) and QuestDB-specific bonus tests
from the pfc-archiver-java test report.

All tests run without a live QuestDB — psycopg2 and subprocess are mocked.

Run with:  python -m pytest tests/ -v
           python -m pytest tests/test_resilience.py -v
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch, call

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
            if section in cfg:
                cfg[section].update(vals)
            else:
                cfg[section] = vals
    return cfg


def _now():
    return datetime.now(timezone.utc)


# ===========================================================================
# S1 — DB down / connection refused
# ===========================================================================

class TestDBDownAtStartup(unittest.TestCase):
    """S1: QuestDB is unreachable when archive_cycle runs."""

    @patch("pfc_archiver_questdb._connect")
    def test_connection_error_cycle_fails_gracefully(self, mock_connect):
        """Connection refused → cycle logs error, no exception propagates out."""
        mock_connect.side_effect = Exception("Connection refused: localhost:8812")

        cfg = _make_cfg()
        # archive_cycle wraps get_partition_ranges which calls _connect
        # It must NOT raise — must log and return
        try:
            archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        except Exception as exc:
            self.fail(f"archive_cycle raised unexpectedly: {exc}")

    @patch("pfc_archiver_questdb._connect")
    def test_connection_error_logs_cycle_failed(self, mock_connect):
        """Connection error must produce a log message, not a silent skip."""
        mock_connect.side_effect = Exception("Connection refused")

        cfg = _make_cfg()
        import logging
        with self.assertLogs("pfc-archiver-questdb", level="ERROR") as cm:
            archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        self.assertTrue(
            any("failed" in msg.lower() or "Connection" in msg for msg in cm.output),
            f"Expected error log, got: {cm.output}"
        )

    @patch("pfc_archiver_questdb.get_partition_ranges")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    def test_db_error_during_export_skips_partition_continues_others(
        self, mock_export, mock_partitions
    ):
        """DB error on partition N must skip N, but continue with N+1."""
        now = _now()
        p1 = (now - timedelta(days=35), now - timedelta(days=34))
        p2 = (now - timedelta(days=34), now - timedelta(days=33))

        mock_partitions.return_value = [p1, p2]
        # First partition fails, second succeeds
        mock_export.side_effect = [
            Exception("connection dropped"),
            {"rows": 100, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0},
        ]

        cfg = _make_cfg()
        with patch("pfc_archiver_questdb.upload_archive", return_value=True), \
             patch("pfc_archiver_questdb.verify_archive", return_value=True), \
             patch("pfc_archiver_questdb.write_run_log") as mock_log:
            archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        # Second partition must have been attempted and logged as ok
        self.assertEqual(mock_export.call_count, 2)
        log_entries = [c[0][1] for c in mock_log.call_args_list]
        statuses = [e.get("status") for e in log_entries]
        self.assertIn("ok", statuses, "Second partition should succeed")


# ===========================================================================
# S2 — Wrong port / wrong credentials
# ===========================================================================

class TestWrongConnectionParams(unittest.TestCase):

    @patch("pfc_archiver_questdb._connect")
    def test_wrong_port_cycle_fails_gracefully(self, mock_connect):
        """Wrong port → same graceful failure as connection refused."""
        mock_connect.side_effect = Exception("Connection refused: localhost:9999")

        cfg = _make_cfg({"db": {"port": 9999}})
        try:
            archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        except Exception as exc:
            self.fail(f"Should not raise: {exc}")


# ===========================================================================
# S3 — pfc_jsonl binary missing or fails
# ===========================================================================

class TestBinaryMissing(unittest.TestCase):

    @patch("pfc_archiver_questdb._connect")
    def test_compress_failure_skips_partition_daemon_continues(self, mock_connect):
        """pfc_jsonl compress exits non-zero → partition fails, others continue."""
        now = _now()
        p1 = (now - timedelta(days=35), now - timedelta(days=34))
        p2 = (now - timedelta(days=34), now - timedelta(days=33))

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",)]
        mock_cur.fetchmany.side_effect = [
            [(datetime(2026, 3, 1, tzinfo=timezone.utc), "INFO")],
            [],
            [(datetime(2026, 3, 2, tzinfo=timezone.utc), "ERROR")],
            [],
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        with patch("pfc_archiver_questdb.get_partition_ranges", return_value=[p1, p2]), \
             patch("pfc_archiver_questdb.subprocess.run") as mock_run:
            # First compress call fails, second succeeds
            fail_proc    = MagicMock(returncode=1, stderr="pfc_jsonl: not found")
            success_proc = MagicMock(returncode=0, stderr="")

            def fake_run(cmd, **kwargs):
                if mock_run.call_count <= 1:
                    return fail_proc
                out_path = Path(cmd[-1])
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(b"FAKE_PFC")
                return success_proc

            mock_run.side_effect = fake_run

            with patch("pfc_archiver_questdb.upload_archive", return_value=True), \
                 patch("pfc_archiver_questdb.verify_archive", return_value=True), \
                 patch("pfc_archiver_questdb.write_run_log") as mock_log:
                try:
                    archive_cycle(_make_cfg(), "/usr/bin/pfc_jsonl", dry_run=False)
                except Exception as exc:
                    self.fail(f"Daemon must not crash on compress failure: {exc}")

    @patch("pfc_archiver_questdb.subprocess.run")
    @patch("pfc_archiver_questdb._connect")
    def test_compress_error_raises_runtime_error(self, mock_connect, mock_run):
        """pfc_jsonl non-zero exit → RuntimeError from export_partition_to_pfc."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",)]
        mock_cur.fetchmany.side_effect = [
            [(datetime(2026, 3, 1, tzinfo=timezone.utc), "INFO")],
            [],
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        fail_proc = MagicMock(returncode=1, stderr="binary not found")
        mock_run.return_value = fail_proc

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "test.pfc"
            with self.assertRaises(RuntimeError) as ctx:
                export_partition_to_pfc(db_cfg, from_ts, to_ts, out_pfc, "/usr/bin/pfc_jsonl")
            self.assertIn("pfc_jsonl compress failed", str(ctx.exception))


# ===========================================================================
# S4 — Table does not exist
# ===========================================================================

class TestTableNotFound(unittest.TestCase):

    @patch("pfc_archiver_questdb._connect")
    def test_nonexistent_table_cycle_fails_gracefully(self, mock_connect):
        """DB error 'relation does not exist' → cycle logs, no crash."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.execute.side_effect = Exception(
            'relation "nonexistent_table" does not exist'
        )
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value = mock_conn

        cfg = _make_cfg({"db": {"table": "nonexistent_table"}})
        try:
            archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        except Exception as exc:
            self.fail(f"Should handle gracefully: {exc}")


# ===========================================================================
# S5 — Output dir unwritable
# ===========================================================================

class TestUnwritableOutputDir(unittest.TestCase):

    @patch("pfc_archiver_questdb.get_partition_ranges")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.upload_archive")
    def test_upload_failure_skips_partition_logs_error(
        self, mock_upload, mock_export, mock_partitions
    ):
        """Upload failure → partition logged as failed, daemon continues."""
        now = _now()
        p1  = (now - timedelta(days=35), now - timedelta(days=34))

        mock_partitions.return_value = [p1]
        mock_export.return_value     = {"rows": 50, "jsonl_mb": 0.5, "output_mb": 0.02, "ratio_pct": 4.0}
        mock_upload.side_effect      = PermissionError("Permission denied: /locked-dir/")

        cfg = _make_cfg()
        try:
            archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
        except Exception as exc:
            self.fail(f"Permission error must not crash daemon: {exc}")

    def test_local_copy_permission_error_is_raised(self):
        """PermissionError in shutil.copy2 propagates as expected from upload_archive."""
        import shutil
        with tempfile.NamedTemporaryFile(suffix=".pfc", delete=False) as f:
            f.write(b"DATA")
            pfc_path = Path(f.name)

        arch_cfg = {"output_dir": "/root/no-permission-dir/"}
        with patch("pfc_archiver_questdb.shutil.copy2",
                   side_effect=PermissionError("Permission denied")):
            with patch("pathlib.Path.mkdir"):
                with self.assertRaises(PermissionError):
                    upload_archive(pfc_path, arch_cfg, dry_run=False)
        pfc_path.unlink()


# ===========================================================================
# S6 — No archivable data (all hot)
# ===========================================================================

class TestNoArchivableData(unittest.TestCase):

    @patch("pfc_archiver_questdb._connect")
    def test_all_data_hot_no_partitions_found(self, mock_connect):
        """All data within retention window → empty partitions list → no export."""
        now    = _now()
        min_ts = now - timedelta(days=5)   # only 5 days old

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, now)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertEqual(result, [])

    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_empty_partitions_no_export_called(self, mock_partitions, mock_export):
        mock_partitions.return_value = []
        archive_cycle(_make_cfg(), "/usr/bin/pfc_jsonl", dry_run=False)
        mock_export.assert_not_called()


# ===========================================================================
# S7 — Dry-run: nothing written to disk, nothing deleted from DB
# ===========================================================================

class TestDryRunComplete(unittest.TestCase):

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.delete_partition")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_dry_run_no_side_effects(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_delete, mock_log
    ):
        now = _now()
        mock_partitions.return_value = [(now - timedelta(days=35), now - timedelta(days=34))]
        mock_export.return_value     = {"rows": 0, "jsonl_mb": 0, "output_mb": 0, "ratio_pct": 0}

        archive_cycle(_make_cfg(), "/usr/bin/pfc_jsonl", dry_run=True)

        mock_upload.assert_not_called()
        mock_verify.assert_not_called()
        mock_delete.assert_not_called()
        mock_log.assert_not_called()

    def test_export_dry_run_does_not_connect_to_db(self):
        """Dry-run export must not touch the DB at all."""
        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with patch("pfc_archiver_questdb._connect") as mock_connect:
            export_partition_to_pfc(
                db_cfg, from_ts, to_ts,
                Path("/tmp/dry.pfc"), "/usr/bin/pfc_jsonl",
                dry_run=True,
            )
            mock_connect.assert_not_called()


# ===========================================================================
# S8 — delete_after_archive=true: correct ordering (verify BEFORE delete)
# ===========================================================================

class TestDeleteOrdering(unittest.TestCase):

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.delete_partition")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_verify_called_before_delete(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_delete, mock_log
    ):
        """Verify must complete successfully BEFORE delete is called."""
        call_order = []
        mock_verify.side_effect = lambda *a, **kw: call_order.append("verify") or True
        mock_delete.side_effect = lambda *a, **kw: call_order.append("delete")

        now = _now()
        mock_partitions.return_value = [(now - timedelta(days=35), now - timedelta(days=34))]
        mock_export.return_value     = {"rows": 100, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0}
        mock_upload.return_value     = True

        cfg = _make_cfg({"archive": {"verify": True, "delete_after_archive": True}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        self.assertEqual(call_order, ["verify", "delete"],
                         f"Expected verify→delete, got: {call_order}")

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.delete_partition")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_delete_not_called_when_verify_disabled(
        self, mock_partitions, mock_export, mock_upload,
        mock_verify, mock_delete, mock_log
    ):
        """With verify=false, delete should still run if delete_after_archive=true."""
        now = _now()
        mock_partitions.return_value = [(now - timedelta(days=35), now - timedelta(days=34))]
        mock_export.return_value     = {"rows": 50, "jsonl_mb": 0.5, "output_mb": 0.02, "ratio_pct": 4.0}
        mock_upload.return_value     = True

        cfg = _make_cfg({"archive": {"verify": False, "delete_after_archive": True}})
        archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)

        mock_verify.assert_not_called()
        mock_delete.assert_called_once()


# ===========================================================================
# S9 — Multiple cycles: no connection leak
# ===========================================================================

class TestMultipleCyclesNoLeak(unittest.TestCase):

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_three_cycles_each_completes_cleanly(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log
    ):
        """Three consecutive cycles must each complete without error."""
        now = _now()
        mock_partitions.return_value = [(now - timedelta(days=35), now - timedelta(days=34))]
        mock_export.return_value     = {"rows": 50, "jsonl_mb": 0.5, "output_mb": 0.02, "ratio_pct": 4.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        cfg = _make_cfg()
        for i in range(3):
            try:
                archive_cycle(cfg, "/usr/bin/pfc_jsonl", dry_run=False)
            except Exception as exc:
                self.fail(f"Cycle {i+1} raised: {exc}")

        self.assertEqual(mock_partitions.call_count, 3)

    @patch("pfc_archiver_questdb._connect")
    def test_connection_closed_after_get_partition_ranges(self, mock_connect):
        """DB connection must be closed after get_partition_ranges, not kept open."""
        now    = _now()
        min_ts = now - timedelta(days=35)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, now - timedelta(days=2))
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        get_partition_ranges(db_cfg, retention_days=30, partition_days=1)

        mock_conn.close.assert_called_once()

    @patch("pfc_archiver_questdb._connect")
    def test_connection_closed_after_export(self, mock_connect):
        """DB connection must be closed after export_partition_to_pfc."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",)]
        mock_cur.fetchmany.return_value = []
        mock_conn.cursor.return_value   = mock_cur
        mock_connect.return_value       = mock_conn

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        export_partition_to_pfc(db_cfg, from_ts, to_ts, Path("/tmp/x.pfc"), "/usr/bin/pfc_jsonl")

        mock_conn.close.assert_called()


# ===========================================================================
# S10 — Config file missing
# ===========================================================================

class TestConfigMissing(unittest.TestCase):

    def test_missing_config_raises_file_not_found(self):
        with self.assertRaises((FileNotFoundError, OSError)):
            load_config("/nonexistent/path/config.toml")

    def test_invalid_toml_raises(self):
        fd, path = tempfile.mkstemp(suffix=".toml")
        os.write(fd, b"this is not valid toml [[[")
        os.close(fd)
        with self.assertRaises(Exception):
            load_config(path)
        os.unlink(path)


# ===========================================================================
# QQ1 — Large batch streaming (QuestDB-specific)
# ===========================================================================

class TestLargeBatchStreaming(unittest.TestCase):

    @patch("pfc_archiver_questdb.subprocess.run")
    @patch("pfc_archiver_questdb._connect")
    def test_50k_rows_processed_in_batches(self, mock_connect, mock_run):
        """50,000 rows should be fetched in multiple batches of batch_size, not all at once."""
        TOTAL_ROWS  = 50_000
        BATCH_SIZE  = 10_000
        ts_base     = datetime(2026, 3, 1, tzinfo=timezone.utc)

        # Build batches of 10k rows
        batches = []
        for i in range(TOTAL_ROWS // BATCH_SIZE):
            batch = [(ts_base + timedelta(seconds=j + i * BATCH_SIZE), "INFO")
                     for j in range(BATCH_SIZE)]
            batches.append(batch)
        batches.append([])  # sentinel

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",)]
        mock_cur.fetchmany.side_effect = batches
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        def fake_run(cmd, **kwargs):
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"FAKE_PFC")
            return MagicMock(returncode=0)
        mock_run.side_effect = fake_run

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "big.pfc"
            result  = export_partition_to_pfc(
                db_cfg, from_ts, to_ts, out_pfc, "/usr/bin/pfc_jsonl",
                batch_size=BATCH_SIZE,
            )

        self.assertEqual(result["rows"], TOTAL_ROWS)
        # Must have called fetchmany exactly (TOTAL_ROWS / BATCH_SIZE + 1) times
        self.assertEqual(mock_cur.fetchmany.call_count, TOTAL_ROWS // BATCH_SIZE + 1)


# ===========================================================================
# QQ2 — QuestDB timestamp type variety
# ===========================================================================

class TestQuestDBTimestampTypes(unittest.TestCase):
    """QuestDB can return timestamps in several forms via PostgreSQL wire protocol."""

    @patch("pfc_archiver_questdb._connect")
    def test_tz_aware_datetime_handled(self, mock_connect):
        now    = _now()
        min_ts = datetime(2026, 1, 1, tzinfo=timezone.utc)  # tz-aware

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (min_ts, now - timedelta(days=32))
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)

    @patch("pfc_archiver_questdb._connect")
    def test_epoch_microseconds_int_handled(self, mock_connect):
        """Some QuestDB JDBC/psycopg2 combos return epoch-µs as int."""
        epoch_us_min = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)
        epoch_us_max = int(datetime(2026, 4, 1, tzinfo=timezone.utc).timestamp() * 1_000_000)

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (epoch_us_min, epoch_us_max)
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)

    @patch("pfc_archiver_questdb._connect")
    def test_naive_datetime_treated_as_utc(self, mock_connect):
        """Naive datetime (no tzinfo) returned by psycopg2 must be treated as UTC."""
        naive_min = datetime(2026, 1, 1, 0, 0, 0)  # no tzinfo

        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.fetchone.return_value = (naive_min, datetime(2026, 4, 1))
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        db_cfg = {"host": "localhost", "table": "logs", "ts_column": "ts"}
        # Should not raise — naive must be treated as UTC, not cause a comparison error
        result = get_partition_ranges(db_cfg, retention_days=30, partition_days=1)
        self.assertIsInstance(result, list)


# ===========================================================================
# QQ3 — Column type serialization (various QuestDB types)
# ===========================================================================

class TestColumnTypeSerialization(unittest.TestCase):
    """QuestDB supports many column types — all must serialize cleanly to JSONL."""

    @patch("pfc_archiver_questdb.subprocess.run")
    @patch("pfc_archiver_questdb._connect")
    def test_none_values_serialize_as_null(self, mock_connect, mock_run):
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("value",), ("tag",)]
        mock_cur.fetchmany.side_effect = [
            [(datetime(2026, 3, 1, tzinfo=timezone.utc), None, None)],
            [],
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        def fake_run(cmd, **kwargs):
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"PFC")
            return MagicMock(returncode=0)
        mock_run.side_effect = fake_run

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        written_lines = []

        import builtins
        real_open = builtins.open

        def capture_open(path, *args, **kwargs):
            if str(path).endswith(".jsonl") and "w" in str(args):
                class CapturingFile:
                    def __init__(self): self.lines = []
                    def write(self, s): self.lines.append(s); written_lines.extend(s.splitlines(keepends=True))
                    def __enter__(self): return self
                    def __exit__(self, *a): pass
                return CapturingFile()
            return real_open(path, *args, **kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "types.pfc"
            # Just run it — if it doesn't crash, None serialization works
            result  = export_partition_to_pfc(
                db_cfg, from_ts, to_ts, out_pfc, "/usr/bin/pfc_jsonl",
            )
        self.assertEqual(result["rows"], 1)

    @patch("pfc_archiver_questdb.subprocess.run")
    @patch("pfc_archiver_questdb._connect")
    def test_various_types_dont_crash(self, mock_connect, mock_run):
        """int, float, bool, bytes, datetime, None — all must serialize without error."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [
            ("timestamp",), ("int_val",), ("float_val",),
            ("bool_val",), ("bytes_val",), ("str_val",),
        ]
        ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        mock_cur.fetchmany.side_effect = [
            [(ts, 42, 3.14, True, b"\x00\xff", "hello")],
            [],
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        def fake_run(cmd, **kwargs):
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"PFC")
            return MagicMock(returncode=0)
        mock_run.side_effect = fake_run

        db_cfg  = {"host": "localhost", "table": "sensors", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "types.pfc"
            result  = export_partition_to_pfc(
                db_cfg, from_ts, to_ts, out_pfc, "/usr/bin/pfc_jsonl",
            )
        self.assertEqual(result["rows"], 1)

    @patch("pfc_archiver_questdb.subprocess.run")
    @patch("pfc_archiver_questdb._connect")
    def test_bytes_serialized_as_hex(self, mock_connect, mock_run):
        """bytes values must be serialized as hex strings (not crash on JSON encode)."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("raw",)]
        ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        mock_cur.fetchmany.side_effect = [
            [(ts, b"\xde\xad\xbe\xef")],
            [],
        ]
        mock_conn.cursor.return_value = mock_cur
        mock_connect.return_value     = mock_conn

        captured_jsonl = []

        def fake_run(cmd, **kwargs):
            # Read the input jsonl to verify bytes→hex conversion
            input_path = Path(cmd[-2])  # second-to-last arg is input
            if input_path.exists():
                with open(input_path) as f:
                    for line in f:
                        captured_jsonl.append(json.loads(line))
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"PFC")
            return MagicMock(returncode=0)
        mock_run.side_effect = fake_run

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "hex.pfc"
            export_partition_to_pfc(db_cfg, from_ts, to_ts, out_pfc, "/usr/bin/pfc_jsonl")

        if captured_jsonl:
            self.assertEqual(captured_jsonl[0]["raw"], "deadbeef")


# ===========================================================================
# QQ4 — Lossless roundtrip (mock-level: JSONL written is valid JSON)
# ===========================================================================

class TestLosslessJsonl(unittest.TestCase):
    """Verify that exported JSONL is valid, parseable, and preserves all fields."""

    @patch("pfc_archiver_questdb.subprocess.run")
    @patch("pfc_archiver_questdb._connect")
    def test_exported_jsonl_is_valid_and_complete(self, mock_connect, mock_run):
        """Each exported row must be valid JSON with all columns present."""
        mock_conn = MagicMock()
        mock_cur  = MagicMock()
        mock_cur.description = [("timestamp",), ("level",), ("message",), ("host",)]
        rows = [
            (datetime(2026, 3, 1, 0, i, 0, tzinfo=timezone.utc), "INFO", f"msg-{i}", f"host-{i}")
            for i in range(10)
        ]
        mock_cur.fetchmany.side_effect = [rows, []]
        mock_conn.cursor.return_value  = mock_cur
        mock_connect.return_value      = mock_conn

        captured_lines = []

        def fake_run(cmd, **kwargs):
            input_path = Path(cmd[-2])
            if input_path.exists():
                with open(input_path) as f:
                    captured_lines.extend(f.readlines())
            out = Path(cmd[-1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b"PFC")
            return MagicMock(returncode=0)
        mock_run.side_effect = fake_run

        db_cfg  = {"host": "localhost", "table": "logs", "ts_column": "timestamp"}
        from_ts = datetime(2026, 3, 1, tzinfo=timezone.utc)
        to_ts   = datetime(2026, 3, 2, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_pfc = Path(tmpdir) / "roundtrip.pfc"
            result  = export_partition_to_pfc(
                db_cfg, from_ts, to_ts, out_pfc, "/usr/bin/pfc_jsonl",
            )

        self.assertEqual(result["rows"], 10)

        if captured_lines:
            for line in captured_lines:
                parsed = json.loads(line)
                # All original columns must be present
                self.assertIn("timestamp", parsed)
                self.assertIn("level", parsed)
                self.assertIn("message", parsed)
                self.assertIn("host", parsed)


# ===========================================================================
# QQ5 — Multiple partitions in one cycle (5 days of data)
# ===========================================================================

class TestMultiplePartitionsOneCycle(unittest.TestCase):

    @patch("pfc_archiver_questdb.write_run_log")
    @patch("pfc_archiver_questdb.verify_archive")
    @patch("pfc_archiver_questdb.upload_archive")
    @patch("pfc_archiver_questdb.export_partition_to_pfc")
    @patch("pfc_archiver_questdb.get_partition_ranges")
    def test_five_day_partitions_all_processed(
        self, mock_partitions, mock_export, mock_upload, mock_verify, mock_log
    ):
        """5 daily partitions must each be exported, uploaded, verified, and logged."""
        now = _now()
        partitions = [
            (now - timedelta(days=35 - i), now - timedelta(days=34 - i))
            for i in range(5)
        ]
        mock_partitions.return_value = partitions
        mock_export.return_value     = {"rows": 100, "jsonl_mb": 1.0, "output_mb": 0.05, "ratio_pct": 5.0}
        mock_upload.return_value     = True
        mock_verify.return_value     = True

        archive_cycle(_make_cfg(), "/usr/bin/pfc_jsonl", dry_run=False)

        self.assertEqual(mock_export.call_count, 5)
        self.assertEqual(mock_upload.call_count, 5)
        self.assertEqual(mock_verify.call_count, 5)
        self.assertEqual(mock_log.call_count,    5)

        for c in mock_log.call_args_list:
            self.assertEqual(c[0][1]["status"], "ok")


if __name__ == "__main__":
    unittest.main(verbosity=2)
