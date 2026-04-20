from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from app.config import Settings, load_settings
from app.influx_writer import InfluxWriter
from app.logging_config import configure_logging
from app.scoreboard import fetch_scoreboard_html, parse_scoreboard_html, snapshot_hash

LOGGER = logging.getLogger(__name__)


@dataclass
class HealthState:
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_success_at: datetime | None = None
    last_attempt_at: datetime | None = None
    last_error: str | None = None
    rows_parsed: int = 0
    rows_written: int = 0
    snapshot_hash: str | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)

    def snapshot(self) -> dict[str, object]:
        with self.lock:
            return {
                "started_at": self.started_at.isoformat(),
                "last_success_at": self.last_success_at.isoformat()
                if self.last_success_at
                else None,
                "last_attempt_at": self.last_attempt_at.isoformat()
                if self.last_attempt_at
                else None,
                "last_error": self.last_error,
                "rows_parsed": self.rows_parsed,
                "rows_written": self.rows_written,
                "snapshot_hash": self.snapshot_hash,
            }


def make_handler(state: HealthState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/healthz":
                self._json(200, state.snapshot())
            elif self.path == "/metrics":
                self._text(200, metrics_text(state.snapshot()))
            else:
                self._json(404, {"error": "not found"})

        def log_message(self, format: str, *args: object) -> None:
            return

        def _json(self, status: int, payload: dict[str, object]) -> None:
            body = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _text(self, status: int, body_text: str) -> None:
            body = body_text.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return Handler


def metrics_text(snapshot: dict[str, object]) -> str:
    last_success = snapshot.get("last_success_at")
    last_success_ts = (
        datetime.fromisoformat(str(last_success)).timestamp() if last_success else 0
    )
    last_error = 1 if snapshot.get("last_error") else 0
    return "\n".join(
        [
            "# HELP scoreboard_rows_parsed Last parsed row count.",
            "# TYPE scoreboard_rows_parsed gauge",
            f"scoreboard_rows_parsed {snapshot.get('rows_parsed') or 0}",
            "# HELP scoreboard_rows_written Last written row count.",
            "# TYPE scoreboard_rows_written gauge",
            f"scoreboard_rows_written {snapshot.get('rows_written') or 0}",
            "# HELP scoreboard_last_success_timestamp_seconds Last successful scrape time.",
            "# TYPE scoreboard_last_success_timestamp_seconds gauge",
            f"scoreboard_last_success_timestamp_seconds {last_success_ts}",
            "# HELP scoreboard_last_error Last scrape had an error.",
            "# TYPE scoreboard_last_error gauge",
            f"scoreboard_last_error {last_error}",
            "",
        ]
    )


def start_health_server(settings: Settings, state: HealthState) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(
        (settings.health_host, settings.health_port), make_handler(state)
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    LOGGER.info(
        "health server started",
        extra={"host": settings.health_host, "port": settings.health_port},
    )
    return server


def save_failed_html(settings: Settings, html: str, scrape_time: datetime) -> str | None:
    if not settings.save_failed_html:
        return None
    os.makedirs(settings.failed_html_dir, exist_ok=True)
    filename = f"failed-{scrape_time.strftime('%Y%m%dT%H%M%SZ')}.html"
    path = os.path.join(settings.failed_html_dir, filename)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(html)
    return path


def run_once(
    settings: Settings,
    writer: InfluxWriter,
    state: HealthState,
    previous_hash: str | None,
) -> str | None:
    scrape_time = datetime.now(timezone.utc)
    started = time.monotonic()
    html: str | None = None
    with state.lock:
        state.last_attempt_at = scrape_time

    LOGGER.info("scrape started", extra={"url": settings.scoreboard_url})
    try:
        html = fetch_scoreboard_html(
            settings.scoreboard_url,
            settings.http_timeout_seconds,
            settings.http_user_agent,
        )
        rows = parse_scoreboard_html(html, scrape_time)
        current_hash = snapshot_hash(rows)
        duration = time.monotonic() - started

        skipped = settings.skip_identical_snapshots and current_hash == previous_hash
        rows_written = 0 if skipped else writer.write_rows(rows)
        writer.write_health(
            scrape_time=scrape_time,
            ok=True,
            duration_seconds=duration,
            rows_parsed=len(rows),
            rows_written=rows_written,
            snapshot_hash=current_hash,
            skipped_identical=skipped,
        )
        with state.lock:
            state.last_success_at = scrape_time
            state.last_error = None
            state.rows_parsed = len(rows)
            state.rows_written = rows_written
            state.snapshot_hash = current_hash

        LOGGER.info(
            "scrape finished",
            extra={
                "duration_seconds": round(duration, 3),
                "rows_parsed": len(rows),
                "rows_written": rows_written,
                "snapshot_hash": current_hash,
                "skipped_identical": skipped,
            },
        )
        return current_hash
    except Exception as exc:
        duration = time.monotonic() - started
        failed_path = save_failed_html(settings, html, scrape_time) if html else None
        LOGGER.exception(
            "scrape failed",
            extra={
                "duration_seconds": round(duration, 3),
                "failed_html_path": failed_path,
            },
        )
        try:
            writer.write_health(
                scrape_time=scrape_time,
                ok=False,
                duration_seconds=duration,
                rows_parsed=0,
                rows_written=0,
                error=str(exc),
            )
        except Exception:
            LOGGER.warning("failed to write scraper health point", exc_info=True)
        with state.lock:
            state.last_error = str(exc)
            state.rows_parsed = 0
            state.rows_written = 0
        return previous_hash


def main() -> None:
    settings = load_settings()
    configure_logging(settings.log_level)
    state = HealthState()
    server = start_health_server(settings, state)
    stop_event = threading.Event()

    def stop(_signum: int, _frame: object) -> None:
        stop_event.set()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    writer = InfluxWriter(
        settings.influx_url,
        settings.influx_token,
        settings.influx_org,
        settings.influx_bucket,
    )
    previous_hash: str | None = None
    try:
        while not stop_event.is_set():
            previous_hash = run_once(settings, writer, state, previous_hash)
            stop_event.wait(settings.scrape_interval_seconds)
    finally:
        writer.close()
        server.shutdown()


if __name__ == "__main__":
    main()
