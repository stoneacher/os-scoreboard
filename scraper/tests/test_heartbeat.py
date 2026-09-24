from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import app.main as main
from app.config import Settings
from app.main import HealthState, heartbeat_slot, run_once

FIXTURE_HTML = (Path(__file__).parent / "fixtures" / "scoreboard_sample.html").read_text(encoding="utf-8")

# Huge interval keeps the slot constant for the duration of a test run.
SETTINGS = Settings(
    scoreboard_url="http://example.invalid",
    scrape_interval_seconds=300,
    http_timeout_seconds=5,
    http_user_agent="test",
    influx_url="http://example.invalid",
    influx_token="token",
    influx_org="org",
    influx_bucket="bucket",
    log_level="INFO",
    save_failed_html=False,
    skip_identical_snapshots=True,
    heartbeat_interval_seconds=10**9,
    failed_html_dir="/tmp",
    health_host="127.0.0.1",
    health_port=0,
    gitlab_api_url=None,
    gitlab_project_id=None,
    gitlab_api_token=None,
    gitlab_poll_interval_seconds=120,
)


class FakeWriter:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.written: list[int] = []

    def write_rows(self, rows):
        if self.fail:
            raise RuntimeError("influx down")
        self.written.append(len(rows))
        return len(rows)

    def write_queue_snapshot(self, scrape_time, snapshot) -> None:
        pass

    def write_health(self, **kwargs) -> None:
        pass


@pytest.fixture(autouse=True)
def fake_fetch(monkeypatch):
    monkeypatch.setattr(main, "fetch_scoreboard_html", lambda *args: FIXTURE_HTML)


def current_slot() -> int:
    return heartbeat_slot(datetime.now(timezone.utc), SETTINGS.heartbeat_interval_seconds)


def test_heartbeat_slot_is_wall_clock_aligned() -> None:
    half_past = datetime(2026, 10, 1, 12, 30, tzinfo=timezone.utc)
    assert heartbeat_slot(half_past, 1800) == heartbeat_slot(half_past + timedelta(minutes=29, seconds=59), 1800)
    assert heartbeat_slot(half_past + timedelta(minutes=30), 1800) == heartbeat_slot(half_past, 1800) + 1
    assert heartbeat_slot(half_past - timedelta(seconds=1), 1800) == heartbeat_slot(half_past, 1800) - 1


def test_first_run_writes_all_rows() -> None:
    writer = FakeWriter()
    hashes, slot = run_once(SETTINGS, writer, HealthState(), {}, None)
    assert writer.written == [len(hashes)] and len(hashes) > 0
    assert slot == current_slot()


def test_unchanged_rows_skipped_within_slot() -> None:
    writer = FakeWriter()
    hashes, slot = run_once(SETTINGS, writer, HealthState(), {}, None)
    run_once(SETTINGS, writer, HealthState(), hashes, slot)
    assert writer.written[-1] == 0


def test_new_slot_writes_every_team() -> None:
    writer = FakeWriter()
    hashes, slot = run_once(SETTINGS, writer, HealthState(), {}, None)
    run_once(SETTINGS, writer, HealthState(), hashes, slot - 1)
    assert writer.written[-1] == len(hashes)


def test_failed_write_keeps_heartbeat_pending() -> None:
    hashes, slot = run_once(SETTINGS, FakeWriter(), HealthState(), {}, None)
    _, slot_after_failure = run_once(SETTINGS, FakeWriter(fail=True), HealthState(), hashes, slot - 1)
    assert slot_after_failure == slot - 1


def test_skip_disabled_writes_everything() -> None:
    settings = replace(SETTINGS, skip_identical_snapshots=False)
    writer = FakeWriter()
    hashes, slot = run_once(settings, writer, HealthState(), {}, None)
    run_once(settings, writer, HealthState(), hashes, slot)
    assert writer.written[-1] == len(hashes)
