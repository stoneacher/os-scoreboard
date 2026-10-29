# AGENTS.md

## Project Overview

**TU Graz OS-scoreboard-monitor** — scrapes the public [TU Graz Tacos/Sweb OS scoreboard](https://sweb.student.isec.tugraz.at/), normalizes team standings, stores time-series snapshots in InfluxDB 2 OSS, and visualizes them via Grafana. Deployed on a VPS since SS 2026.

## Architecture

```
TU Graz Scoreboard (public HTTP)
    ↓  fetch + retry (requests)
Python scraper (scraper/app/)
    ↓  parse + normalize
InfluxDB 2.0 (time-series)
    ↓  Flux queries
Grafana (dashboards)
```

Three Docker services: `scraper`, `influxdb`, `grafana`. The scraper exposes `/healthz` (JSON) and `/metrics` (Prometheus) via a lightweight HTTP server built into the process.

## Tech Stack

- **Python 3.12** — scraper application
- **beautifulsoup4 / lxml / pandas** — HTML parsing (pandas first, BS4 fallback)
- **influxdb-client** — time-series writes
- **pytest** — unit tests
- **Docker Compose** — service orchestration
- **Grafana + InfluxDB 2 OSS** — storage and visualization

No linter or autoformatter is enforced.

## Key Files

| Path | Purpose |
|------|---------|
| `scraper/app/main.py` | Entry point: HTTP health server + scraping loop + signal handling |
| `scraper/app/config.py` | `Settings` dataclass, all config from env vars |
| `scraper/app/scoreboard.py` | HTML fetch, parse, normalize, hash; `ScoreboardRow`, `TeamIdentity`, `QueueStatus` |
| `scraper/app/influx_writer.py` | InfluxDB client wrapper; writes `team_scoreboard`, `scraper_health`, `scoreboard_queue` |
| `scraper/app/logging_config.py` | JSON structured logging |
| `scraper/tests/test_scoreboard.py` | Unit tests for all parsers |
| `docker-compose.yml` | Production services |
| `docker-compose.local.yml` | Local dev overrides (exposes ports on 127.0.0.1) |
| `Makefile` | Common commands (see below) |
| `scripts/backup-volumes.sh` | Docker volume backup with retention cleanup |
| `scripts/bootstrap-influx.sh` | InfluxDB bucket initialization |
| `grafana/dashboards/` | Pre-provisioned dashboard JSONs |

## Common Commands

```bash
make up          # Build and start all services (detached)
make down        # Stop all services
make restart     # Force-recreate all containers
make logs        # Tail all service logs
make logs-scraper / logs-grafana / logs-influx  # Single-service logs
make test        # Build image then run pytest inside it
make build       # Build scraper image only
```

## Local Development

```bash
cp .env.example .env          # Fill in required vars
docker compose -f docker-compose.yml -f docker-compose.local.yml up -d --build
# Grafana: http://localhost:3000
# InfluxDB: http://localhost:8086
# Scraper health: http://localhost:8080/healthz
```

Stop:
```bash
docker compose -f docker-compose.yml -f docker-compose.local.yml down
```

## Tests

```bash
make test
# Runs: docker run --rm <image> python -m pytest -q /app/tests
```

Tests live in `scraper/tests/test_scoreboard.py` and cover percentage parsing, boolean/status parsing, queue status parsing, column normalization, team label parsing, and full row normalization.

## Deployment (VPS)

```bash
git pull
docker compose up -d --build
```

Grafana is exposed via a reverse proxy. InfluxDB stays on the internal Docker network only. See `scripts/backup-volumes.sh` for volume backups with configurable retention.

## Important Design Decisions

- **Dual HTML parser**: pandas `read_html` is tried first (fast), BS4 is the fallback.
- **Row deduplication**: SHA256 hash of each row prevents redundant InfluxDB writes when the scoreboard hasn't changed.
- **Heartbeat writes**: Even if the data is unchanged, a write is forced after `HEARTBEAT_INTERVAL_SECONDS` to keep time-series fresh.
- **Column aliasing**: `scoreboard.py` normalizes many column name variants (e.g. `"Fork / Exec"` → `"fork_exec"`, `"5d Avg."` → `"avg_5d"`).
- **Team label parsing**: Handles `"Name (ID)"`, `"ID only"`, `"Name ID"` (no parens), and `"???"` (anonymous).
- **Failed HTML archival**: On parse errors, the raw HTML is saved to disk (configurable path) for post-mortem debugging.
- **Queue state**: Detects running (`"37%"`) vs queued (`"3 (32 min.)"`) vs unknown states.

## Environment Variables

All config is loaded from environment by `scraper/app/config.py` into a frozen `Settings` dataclass. See `.env.example` for the full list. Key variables:

- `SCOREBOARD_URL` — target URL to scrape
- `SCRAPE_INTERVAL_SECONDS` — polling interval
- `INFLUXDB_URL` / `INFLUXDB_TOKEN` / `INFLUXDB_ORG` / `INFLUXDB_BUCKET`
- `HEARTBEAT_INTERVAL_SECONDS` — max age before a forced write (default 3600)
- `HEALTH_BIND_HOST` / `HEALTH_BIND_PORT` — health/metrics endpoint
- `FAILED_HTML_DIR` — directory to archive failed scrape responses
