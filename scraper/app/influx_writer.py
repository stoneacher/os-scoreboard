from __future__ import annotations

from datetime import datetime
from typing import Any

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from app.scoreboard import ScoreboardRow


class InfluxWriter:
    def __init__(self, url: str, token: str, org: str, bucket: str) -> None:
        self.bucket = bucket
        self.org = org
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.write_api = self.client.write_api(write_options=SYNCHRONOUS)

    def close(self) -> None:
        self.client.close()

    def write_rows(self, rows: list[ScoreboardRow]) -> int:
        points = [self._row_to_point(row) for row in rows]
        if points:
            self.write_api.write(bucket=self.bucket, org=self.org, record=points)
        return len(points)

    def write_health(
        self,
        scrape_time: datetime,
        ok: bool,
        duration_seconds: float,
        rows_parsed: int,
        rows_written: int,
        snapshot_hash: str | None = None,
        error: str | None = None,
        skipped_identical: bool = False,
    ) -> None:
        point = (
            Point("scraper_health")
            .tag("status", "ok" if ok else "error")
            .field("ok", ok)
            .field("duration_seconds", float(duration_seconds))
            .field("rows_parsed", int(rows_parsed))
            .field("rows_written", int(rows_written))
            .field("skipped_identical", skipped_identical)
            .time(scrape_time, WritePrecision.NS)
        )
        if snapshot_hash:
            point.field("snapshot_hash", snapshot_hash)
        if error:
            point.field("error", error[:500])
        self.write_api.write(bucket=self.bucket, org=self.org, record=point)

    def _row_to_point(self, row: ScoreboardRow) -> Point:
        point = (
            Point("team_scoreboard")
            .tag("visibility_mode", row.visibility_mode)
            .tag("display_name", row.display_name)
            .time(row.scrape_time, WritePrecision.NS)
        )
        if row.team_id:
            point.tag("team_id", row.team_id)
        if row.submission1 and len(row.submission1) <= 64:
            point.tag("submission1", row.submission1)

        fields: dict[str, Any] = {
            "team_label": row.team_label,
            "is_anonymous": row.is_anonymous,
            "has_stable_identity": row.has_stable_identity,
            "rank": row.rank,
            "team_name": row.team_name,
            "score": row.score,
            "threads": row.threads,
            "fork_exec": row.fork_exec,
            "other": row.other,
            "avg_5d": row.avg_5d,
            "up_count": row.up_count,
            "tw": row.tw,
            "perf": row.perf,
            "build_ok": row.build_ok,
            "boot_ok": row.boot_ok,
            "page_date_raw": row.page_date_raw,
            "mrc_date_raw": row.mrc_date_raw,
        }
        for key, value in fields.items():
            if value is not None:
                point.field(key, value)
        return point
