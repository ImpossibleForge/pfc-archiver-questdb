# Changelog — pfc-archiver-questdb

## v0.1.1 — 2026-04-30

### Fixed

- **File handle leak in `verify_archive`** — decompressed verification file was left
  unclosed after each partition, potentially exhausting OS file descriptors under
  sustained load. Fixed by using `with open()`.

- **DELETE not supported via PostgreSQL wire** — QuestDB 9.x does not support
  `DELETE FROM` through the PostgreSQL wire protocol. `delete_after_archive = true`
  now uses `ALTER TABLE DROP PARTITION` instead, which is the correct and more
  efficient approach for a time-series database: it removes the entire partition file
  atomically rather than row-by-row. Requires the table to be partitioned BY DAY
  (the archiver default).

---

## v0.1.0 — 2026-04-20

Initial release.

- Autonomous archive daemon for QuestDB via PostgreSQL wire protocol (port 8812)
- Full archive cycle: SCAN → EXPORT → COMPRESS → UPLOAD → VERIFY → DELETE → LOG
- Local storage and S3 output support
- Dry-run and --once modes
- Configurable retention window and partition size
- Row-count verification before optional delete
- TOML-based configuration
