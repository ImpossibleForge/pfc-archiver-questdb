# Changelog — pfc-archiver-questdb

## v0.1.0 — 2026-04-20

Initial release.

- Autonomous archive daemon for QuestDB via PostgreSQL wire protocol (port 8812)
- Full archive cycle: SCAN → EXPORT → COMPRESS → UPLOAD → VERIFY → DELETE → LOG
- Local storage and S3 output support
- Dry-run and --once modes
- Configurable retention window and partition size
- Row-count verification before optional delete
- TOML-based configuration
