# pfc-archiver-questdb

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![PFC-JSONL](https://img.shields.io/badge/PFC--JSONL-v3.4-green.svg)](https://github.com/ImpossibleForge/pfc-jsonl)
[![Version](https://img.shields.io/badge/pfc--archiver--questdb-v0.1.0-brightgreen.svg)](https://github.com/ImpossibleForge/pfc-archiver-questdb/releases)

A standalone daemon that runs alongside QuestDB, watches for data older than a configurable retention window, compresses it to PFC format, and writes it to local storage or S3 — automatically.

**Runs as a sidecar or cron job — no schema changes, no plugins, no QuestDB modifications.**

---

## How it works

Every `interval_seconds` (default: 3600), pfc-archiver-questdb runs one archive cycle:

```
SCAN  ->  EXPORT  ->  COMPRESS  ->  UPLOAD  ->  VERIFY  ->  (optional DELETE)  ->  LOG
```

1. **SCAN** — compute which time partitions in QuestDB are older than `retention_days`
2. **EXPORT** — read rows in `partition_days`-sized chunks via PostgreSQL wire protocol (port 8812)
3. **COMPRESS** — pipe through `pfc_jsonl compress` → `.pfc` + `.pfc.bidx` + `.pfc.idx`
4. **UPLOAD** — write to `output_dir` (local path or `s3://bucket/prefix/`)
5. **VERIFY** — decompress and count rows; must match exported count exactly
6. **DELETE** _(optional)_ — `DELETE WHERE ts >= from AND ts < to` (only if `delete_after_archive = true`, requires QuestDB 7.3+)
7. **LOG** — write a JSON run log to `log_dir`

---

## Supported databases

| Database | Protocol | Default port |
|----------|----------|-------------|
| QuestDB | PostgreSQL wire (psycopg2) | 8812 |

> **Note:** QuestDB's PostgreSQL wire endpoint is on port **8812**, not 5432.

---

## Install

```bash
pip install pfc-archiver-questdb

# Or from source
git clone https://github.com/ImpossibleForge/pfc-archiver-questdb
cd pfc-archiver-questdb
pip install -r requirements.txt
```

**The `pfc_jsonl` binary must be installed:**

```bash
# Linux x64:
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-linux-x64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl

# macOS (Apple Silicon M1–M4):
curl -L https://github.com/ImpossibleForge/pfc-jsonl/releases/latest/download/pfc_jsonl-macos-arm64 \
     -o /usr/local/bin/pfc_jsonl && chmod +x /usr/local/bin/pfc_jsonl
```

> **License note:** This tool requires the `pfc_jsonl` binary. `pfc_jsonl` is free for personal and open-source use — commercial use requires a separate license. See [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) for details.

> **macOS Intel (x64):** Binary coming soon.
> **Windows:** No native binary. Use WSL2 or a Linux machine.

**Python dependency:**

```bash
pip install psycopg2-binary
```

---

## Quick start

```bash
# 1. Copy the example config
cp config/questdb.toml my_config.toml

# 2. Edit the config
nano my_config.toml

# 3. Dry run (no writes, prints what would be archived)
python pfc_archiver_questdb.py --config my_config.toml --dry-run

# 4. Archive once and exit
python pfc_archiver_questdb.py --config my_config.toml --once

# 5. Run as a daemon (loops every interval_seconds)
python pfc_archiver_questdb.py --config my_config.toml
```

---

## Configuration

All config is TOML. A complete example is in `config/questdb.toml`.

```toml
[db]
db_type   = "questdb"
host      = "localhost"
port      = 8812          # QuestDB PostgreSQL wire port
user      = "admin"       # QuestDB default user
password  = "quest"       # QuestDB default password
dbname    = "qdb"
table     = "logs"        # table to archive (no schema prefix)
ts_column = "timestamp"   # designated timestamp column

[archive]
retention_days       = 30         # archive data older than this many days
partition_days       = 1          # export this many days per archive file
output_dir           = "./archives/"   # local path or s3://bucket/prefix/
verify               = true       # decompress + count rows after each archive
delete_after_archive = false      # DELETE rows from QuestDB after successful verify
log_dir              = "./archive_logs/"

[daemon]
interval_seconds = 3600           # how often to run (in daemon mode)
```

> **Important:** QuestDB does not use schemas. Reference tables by name only (e.g. `table = "logs"`, not `schema.logs`).

---

## Output format

Each archive cycle produces files named:

```
<table>__<YYYYMMDD>__<YYYYMMDD>.pfc
<table>__<YYYYMMDD>__<YYYYMMDD>.pfc.bidx
<table>__<YYYYMMDD>__<YYYYMMDD>.pfc.idx
```

The `.pfc` file is a PFC-JSONL archive. The `.bidx` and `.idx` files are block indexes that let DuckDB decompress only the relevant time window — without reading the whole file.

---

## Log format

Each completed cycle appends a JSON entry to `<log_dir>/archive_runs.jsonl`:

```json
{
  "ts":        "2026-04-14T18:00:00+00:00",
  "table":     "logs",
  "from_ts":   "2026-03-01T00:00:00+00:00",
  "to_ts":     "2026-03-02T00:00:00+00:00",
  "rows":      248721,
  "jsonl_mb":  42.3,
  "output_mb": 2.5,
  "ratio_pct": 5.9,
  "deleted":   false,
  "status":    "ok"
}
```

---

## Run as a systemd service

```ini
[Unit]
Description=pfc-archiver-questdb — PFC archive daemon for QuestDB
After=network.target

[Service]
Type=simple
User=pfc
WorkingDirectory=/opt/pfc-archiver-questdb
ExecStart=/usr/bin/python3 /opt/pfc-archiver-questdb/pfc_archiver_questdb.py --config /etc/pfc-archiver-questdb/questdb.toml
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable pfc-archiver-questdb
sudo systemctl start pfc-archiver-questdb
sudo journalctl -u pfc-archiver-questdb -f
```

---

## Run as a Docker sidecar

```yaml
# docker-compose.yml
services:
  questdb:
    image: questdb/questdb:latest
    ports:
      - "9000:9000"   # QuestDB Web Console
      - "8812:8812"   # PostgreSQL wire protocol
      - "9009:9009"   # InfluxDB line protocol

  pfc-archiver-questdb:
    image: ghcr.io/impossibleforge/pfc-archiver-questdb:latest
    volumes:
      - ./config/questdb.toml:/etc/pfc-archiver-questdb/config.toml
      - ./archives:/archives
      - ./archive_logs:/logs
    environment:
      - PFC_CONFIG=/etc/pfc-archiver-questdb/config.toml
    depends_on: [questdb]
```

---

## Querying cold archives

Once archived, your `.pfc` files are queryable directly from DuckDB:

```sql
INSTALL pfc FROM community;
LOAD pfc;
LOAD json;

-- Scan a single archive
SELECT *
FROM read_pfc_jsonl('./archives/logs__20260301__20260302.pfc')
LIMIT 100;

-- Time-window query (only decompresses the relevant blocks)
SELECT *
FROM read_pfc_jsonl(
    './archives/logs__20260301__20260302.pfc',
    ts_from = epoch(TIMESTAMPTZ '2026-03-01 14:00:00+00'),
    ts_to   = epoch(TIMESTAMPTZ '2026-03-01 15:00:00+00')
);
```

---

## Deleting archived data

`delete_after_archive = false` by default — pfc-archiver-questdb never modifies your QuestDB without explicit opt-in.

After confirming your archives are accessible via DuckDB, set `delete_after_archive = true` and restart. Only partitions that pass the row-count verify step will be deleted.

> **Requires QuestDB 7.3.0+** for DELETE support.

---

## Related Projects

| Project | Description |
|---------|-------------|
| [pfc-jsonl](https://github.com/ImpossibleForge/pfc-jsonl) | Core binary — compress, decompress, query |
| [pfc-duckdb](https://github.com/ImpossibleForge/pfc-duckdb) | DuckDB Community Extension (`INSTALL pfc FROM community`) |
| [pfc-archiver-cratedb](https://github.com/ImpossibleForge/pfc-archiver-cratedb) | Archive daemon for CrateDB |
| [pfc-migrate](https://github.com/ImpossibleForge/pfc-migrate) | One-shot JSONL export and archive conversion |
| [pfc-gateway](https://github.com/ImpossibleForge/pfc-gateway) | HTTP REST server for PFC archives |
| [pfc-vector](https://github.com/ImpossibleForge/pfc-vector) | High-performance Rust ingest daemon for Vector.dev and Telegraf |
| [pfc-otel-collector](https://github.com/ImpossibleForge/pfc-otel-collector) | OpenTelemetry OTLP/HTTP log exporter |

---

## License

MIT — see [LICENSE](LICENSE).

*Built by [ImpossibleForge](https://github.com/ImpossibleForge)*