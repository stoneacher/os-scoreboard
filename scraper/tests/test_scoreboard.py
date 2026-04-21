from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from app.scoreboard import (
    _normalize_column_name,
    build_queue_snapshot,
    normalize_row,
    parse_percentage,
    parse_queue_status,
    parse_scoreboard_html,
    parse_status_bool,
    parse_team_label,
    snapshot_hash,
)


def test_percentage_parsing() -> None:
    assert parse_percentage("92.6%") == 92.6
    assert parse_percentage(" 10,5 % ") == 10.5
    assert parse_percentage("-") is None
    assert parse_percentage("not a number") is None


def test_bool_status_parsing() -> None:
    assert parse_status_bool("OK") is True
    assert parse_status_bool("FAIL") is False
    assert parse_status_bool("unknown") is None


def test_queue_status_parsing() -> None:
    running = parse_queue_status("37%")
    assert running.state == "running"
    assert running.running_percent == 37.0
    assert running.position is None
    assert running.wait_minutes is None

    queued = parse_queue_status("3 (32 min.)")
    assert queued.state == "queued"
    assert queued.running_percent is None
    assert queued.position == 3
    assert queued.wait_minutes == 32.0


def test_column_normalization() -> None:
    assert _normalize_column_name("#") == "rank"
    assert _normalize_column_name("5d Avg.") == "avg_5d"
    assert _normalize_column_name("Fork / Exec") == "fork_exec"
    assert _normalize_column_name("SubmissionI1") == "submission1"
    assert _normalize_column_name("Q") == "queue_raw"
    assert _normalize_column_name("➙") == "_skip"


def test_named_team_parsing() -> None:
    team = parse_team_label("Foo (B2)")
    assert team.team_label == "Foo (B2)"
    assert team.team_name == "Foo"
    assert team.team_id == "B2"
    assert team.visibility_mode == "named"
    assert team.is_anonymous is False
    assert team.has_stable_identity is True
    assert team.display_name == "Foo (B2)"


def test_anonymous_team_parsing() -> None:
    team = parse_team_label("???")
    assert team.team_name is None
    assert team.team_id is None
    assert team.visibility_mode == "anonymous"
    assert team.is_anonymous is True
    assert team.has_stable_identity is False
    assert team.display_name == "???"


def test_id_only_team_parsing() -> None:
    team = parse_team_label("C2")
    assert team.team_name is None
    assert team.team_id == "C2"
    assert team.visibility_mode == "id_only"
    assert team.is_anonymous is False
    assert team.has_stable_identity is True
    assert team.display_name == "C2"


def test_row_normalization() -> None:
    scrape_time = datetime(2026, 4, 20, tzinfo=timezone.utc)
    row = normalize_row(
        {
            "rank": "1",
            "group": "Foo (B2)",
            "build": "OK",
            "boot": "FAIL",
            "score": "92.6%",
            "threads": "91%",
            "fork_exec": "80%",
            "other": "77%",
            "avg_5d": "88%",
            "up_count": "42",
            "tw": "73.6%",
            "perf": "70%",
            "page_date_raw": "20.04. 19:30",
            "mrc_date_raw": "20.04. 19:21",
            "submission1": "FAIL",
            "queue_raw": "3 (32 min.)",
        },
        scrape_time,
    )
    assert row.rank == 1
    assert row.team_id == "B2"
    assert row.display_name == "Foo (B2)"
    assert row.build_ok is True
    assert row.boot_ok is False
    assert row.score == 92.6
    assert row.up_count == 42
    assert row.queue_state == "queued"
    assert row.queue_position == 3
    assert row.queue_wait_minutes == 32.0


def test_html_parsing_fixture() -> None:
    scrape_time = datetime(2026, 4, 20, tzinfo=timezone.utc)
    fixture = Path(__file__).parent / "fixtures" / "scoreboard_sample.html"
    rows = parse_scoreboard_html(fixture.read_text(encoding="utf-8"), scrape_time)
    assert len(rows) == 2
    assert rows[0].team_id == "B2"
    assert rows[0].score == 92.6
    assert rows[0].queue_state == "running"
    assert rows[0].queue_running_percent == 37.0
    assert all(row.visibility_mode != "anonymous" for row in rows)
    assert rows[1].visibility_mode == "id_only"
    assert rows[1].score is None
    assert rows[1].queue_state == "queued"
    assert rows[1].queue_position == 3
    assert rows[1].queue_wait_minutes == 32.0
    queue_snapshot = build_queue_snapshot(rows)
    assert queue_snapshot.running_count == 1
    assert queue_snapshot.queued_count == 1
    assert queue_snapshot.max_queue_position == 3
    assert queue_snapshot.max_queue_wait_minutes == 32.0
    assert len(snapshot_hash(rows)) == 64
