"""Re-tag team_scoreboard points that were stored without a team_id.

Teams whose ID is embedded in their name (e.g. "SummerO<b>F6</b>7") were
stored as visibility_mode=unknown with no team_id tag before the parser
learned to read the <b> marker. This copies those points into the series
the fixed scraper writes today, so team_id-grouped panels get their history.

Runs inside the scraper image (needs app.scoreboard and the INFLUX_* env):

    docker compose run --rm --no-deps -e PYTHONPATH=/app -v ./scripts:/scripts:ro \\
        scraper python /scripts/relabel_teams.py --map "SummerOF67=F6" ...

Without flags it only reports. --apply writes the copies; --delete-old removes
the old unknown series, but only for labels whose copies are all present.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from app.scoreboard import TEAM_ID_RE, parse_team_label

MEASUREMENT = "team_scoreboard"
TAG_KEYS = ("visibility_mode", "display_name", "team_id", "submission1", "submission_type")
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--map", action="append", default=[], metavar="LABEL=ID",
                        help="old unknown label and the team ID it belongs to (repeatable)")
    parser.add_argument("--apply", action="store_true", help="write the re-tagged copies")
    parser.add_argument("--delete-old", action="store_true", help="delete the old unknown series after verifying")
    return parser.parse_args()


def parse_mapping(entries: list[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for entry in entries:
        label, sep, team_id = entry.rpartition("=")
        team_id = team_id.strip().upper()
        if not sep or not label or not TEAM_ID_RE.match(team_id):
            sys.exit(f"invalid --map {entry!r}, expected LABEL=ID like 'SummerOF67=F6'")
        mapping[label] = team_id
    return mapping


def unknown_labels(query_api, bucket: str, org: str) -> dict[str, int]:
    query = f"""
from(bucket: {json.dumps(bucket)})
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r.visibility_mode == "unknown" and r._field == "score")
  |> group(columns: ["display_name"])
  |> count()
"""
    return {r.values["display_name"]: r.get_value() for r in query_api.query_stream(query, org=org)}


def load_points(query_api, bucket: str, org: str, display_name: str, visibility_mode: str | None = None):
    """Return {(time, tags): fields} for every point of one display_name."""
    mode_filter = f" and r.visibility_mode == {json.dumps(visibility_mode)}" if visibility_mode else ""
    query = f"""
from(bucket: {json.dumps(bucket)})
  |> range(start: 0)
  |> filter(fn: (r) => r._measurement == "{MEASUREMENT}" and r.display_name == {json.dumps(display_name)}{mode_filter})
"""
    points: dict[tuple, dict] = defaultdict(dict)
    for record in query_api.query_stream(query, org=org):
        tags = tuple((key, record.values[key]) for key in TAG_KEYS if record.values.get(key) is not None)
        points[(record.get_time(), tags)][record.get_field()] = record.get_value()
    return points


def retagged(points, label: str, team_id: str) -> list[Point]:
    identity = parse_team_label(label, team_id)
    result = []
    for (time, tags), fields in points.items():
        new_tags = dict(tags)
        new_tags.update(
            visibility_mode=identity.visibility_mode,
            display_name=identity.display_name,
            team_id=identity.team_id,
        )
        new_fields = dict(fields)
        new_fields.update(team_name=identity.team_name, has_stable_identity=identity.has_stable_identity)
        point = Point(MEASUREMENT).time(time, WritePrecision.NS)
        for key, value in new_tags.items():
            point.tag(key, value)
        for key, value in new_fields.items():
            if value is not None:
                point.field(key, value)
        result.append(point)
    return result


def main() -> None:
    args = parse_args()
    mapping = parse_mapping(args.map)
    bucket, org = os.environ["INFLUX_BUCKET"], os.environ["INFLUX_ORG"]
    client = InfluxDBClient(url=os.environ["INFLUX_URL"], token=os.environ["INFLUX_TOKEN"], org=org, timeout=120_000)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    labels = unknown_labels(query_api, bucket, org)
    if not labels:
        print("no unknown team series found")
        return
    for label, count in sorted(labels.items()):
        target = mapping.get(label)
        new_name = parse_team_label(label, target).display_name if target else "-- no --map given, skipped"
        print(f"{label!r:28} {count:6} points -> {new_name}")
    for label in sorted(set(mapping) - set(labels)):
        print(f"warning: --map label {label!r} has no unknown series")

    for label, team_id in sorted(mapping.items()):
        if label not in labels:
            continue
        old = load_points(query_api, bucket, org, label, visibility_mode="unknown")
        new_name = parse_team_label(label, team_id).display_name

        if args.apply:
            points = retagged(old, label, team_id)
            for start in range(0, len(points), 5000):
                write_api.write(bucket=bucket, org=org, record=points[start:start + 5000])
            print(f"{label!r}: wrote {len(points)} points as {new_name!r}")

        if args.delete_old:
            copied_times = {time for time, _ in load_points(query_api, bucket, org, new_name)}
            missing = {time for time, _ in old} - copied_times
            if missing:
                print(f"{label!r}: NOT deleting, {len(missing)} timestamps missing under {new_name!r} (run --apply first)")
                continue
            escaped = label.replace("\\", "\\\\").replace('"', '\\"')
            client.delete_api().delete(
                EPOCH,
                datetime.now(timezone.utc),
                f'_measurement="{MEASUREMENT}" AND visibility_mode="unknown" AND display_name="{escaped}"',
                bucket=bucket,
                org=org,
            )
            print(f"{label!r}: deleted old unknown series")

    if not (args.apply or args.delete_old):
        print("\ndry run only; add --apply to write copies, then --delete-old once the dashboards look right")
    client.close()


if __name__ == "__main__":
    main()
