from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from io import StringIO
from typing import Any

import pandas as pd
import requests
from bs4 import BeautifulSoup

LOGGER = logging.getLogger(__name__)

EXPECTED_COLUMNS = {
    "rank",
    "group",
    "build",
    "boot",
    "score",
    "threads",
    "fork_exec",
    "other",
    "avg_5d",
    "up_count",
    "tw",
    "perf",
    "page_date_raw",
    "mrc_date_raw",
    "submission1",
}

TEAM_ID_RE = re.compile(r"^[A-Z][0-9]+$", re.IGNORECASE)
NAMED_TEAM_RE = re.compile(r"^(?P<name>.*?)\s*\((?P<team_id>[A-Z][0-9]+)\)\s*$", re.I)


@dataclass(frozen=True)
class TeamIdentity:
    team_label: str
    team_name: str | None
    team_id: str | None
    visibility_mode: str
    is_anonymous: bool
    has_stable_identity: bool
    display_name: str


@dataclass(frozen=True)
class ScoreboardRow:
    scrape_time: datetime
    rank: int | None
    team_label: str
    team_name: str | None
    team_id: str | None
    visibility_mode: str
    is_anonymous: bool
    has_stable_identity: bool
    display_name: str
    build_ok: bool | None
    boot_ok: bool | None
    score: float | None
    threads: float | None
    fork_exec: float | None
    other: float | None
    avg_5d: float | None
    up_count: int | None
    tw: float | None
    perf: float | None
    page_date_raw: str | None
    mrc_date_raw: str | None
    submission1: str | None


def fetch_scoreboard_html(
    url: str,
    timeout_seconds: int,
    user_agent: str,
    attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> str:
    headers = {"User-Agent": user_agent}
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout_seconds)
            response.raise_for_status()
            return response.text
        except requests.RequestException as exc:
            last_error = exc
            LOGGER.warning(
                "scoreboard fetch failed",
                extra={"attempt": attempt, "attempts": attempts, "url": url},
                exc_info=True,
            )
            if attempt < attempts:
                time.sleep(backoff_seconds * (2 ** (attempt - 1)))
    raise RuntimeError(f"failed to fetch scoreboard after {attempts} attempts") from last_error


def parse_scoreboard_html(html: str, scrape_time: datetime) -> list[ScoreboardRow]:
    raw_rows = _rows_from_pandas(html)
    if raw_rows is None:
        raw_rows = _rows_from_beautifulsoup(html)
    rows = [normalize_row(row, scrape_time) for row in raw_rows]
    rows = [row for row in rows if row.team_label and not row.is_anonymous]
    if not rows:
        raise ValueError("scoreboard table was found but no data rows were parsed")
    return rows


def _rows_from_pandas(html: str) -> list[dict[str, Any]] | None:
    try:
        tables = pd.read_html(StringIO(html), flavor="lxml")
    except Exception:
        LOGGER.debug("pandas.read_html failed; falling back to BeautifulSoup", exc_info=True)
        return None

    best: tuple[int, pd.DataFrame] | None = None
    for table in tables:
        normalized = [_normalize_column_name(str(col)) for col in table.columns]
        score = len(EXPECTED_COLUMNS.intersection(normalized))
        if best is None or score > best[0]:
            best = (score, table)

    if best is None or best[0] < 4:
        return None

    table = best[1].copy()
    table.columns = [_normalize_column_name(str(col)) for col in table.columns]
    table = table.loc[:, [col != "_skip" for col in table.columns]]
    table = table.rename(columns=_dedupe_columns(table.columns))

    rows: list[dict[str, Any]] = []
    for _, record in table.iterrows():
        row = {
            str(key): clean_text(value)
            for key, value in record.to_dict().items()
            if str(key) in EXPECTED_COLUMNS
        }
        if row:
            rows.append(row)
    return rows


def _rows_from_beautifulsoup(html: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html, "lxml")
    tables = soup.find_all("table")
    if not tables:
        raise ValueError("no tables found in HTML")

    best_table = None
    best_headers: list[str] = []
    best_score = -1
    for table in tables:
        header_cells = table.find_all("th")
        headers = [_normalize_column_name(cell.get_text(" ", strip=True)) for cell in header_cells]
        score = len(EXPECTED_COLUMNS.intersection(headers))
        if table.get("id") == "highscore":
            score += 5
        if score > best_score:
            best_table = table
            best_headers = headers
            best_score = score

    if best_table is None or best_score < 4:
        raise ValueError("could not identify the standings table")

    header_pairs = [
        (idx, name)
        for idx, name in enumerate(best_headers)
        if name != "_skip" and name in EXPECTED_COLUMNS
    ]
    deduped = _dedupe_columns([name for _, name in header_pairs])

    rows: list[dict[str, Any]] = []
    for tr in best_table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if not cells:
            continue
        row: dict[str, Any] = {}
        for cell_idx, header_name in header_pairs:
            if cell_idx >= len(cells):
                continue
            key = deduped[header_name]
            if key == "_skip":
                continue
            row[key] = clean_text(cells[cell_idx].get_text(" ", strip=True))
        if row:
            rows.append(row)
    return rows


def _dedupe_columns(columns: list[str] | pd.Index) -> dict[str, str]:
    result: dict[str, str] = {}
    seen: set[str] = set()
    for column in columns:
        if column not in EXPECTED_COLUMNS or column in seen:
            result[column] = "_skip"
            continue
        result[column] = column
        seen.add(column)
    return result


def _normalize_column_name(value: str) -> str:
    text = clean_text(value).lower()
    compact = re.sub(r"[^a-z0-9#]+", "", text)
    aliases = {
        "#": "rank",
        "rank": "rank",
        "group": "group",
        "team": "group",
        "build": "build",
        "compile": "build",
        "boot": "boot",
        "score": "score",
        "threads": "threads",
        "thread": "threads",
        "forkexec": "fork_exec",
        "fork": "fork_exec",
        "other": "other",
        "5davg": "avg_5d",
        "5dayavg": "avg_5d",
        "avg": "avg_5d",
        "average": "avg_5d",
        "up": "up_count",
        "mp": "up_count",
        "tw": "tw",
        "perf": "perf",
        "performance": "perf",
        "date": "page_date_raw",
        "mrcdate": "mrc_date_raw",
        "rdate": "mrc_date_raw",
        "submission1": "submission1",
        "submissioni1": "submission1",
        "submissionl1": "submission1",
        "tagged": "submission1",
        "q": "_skip",
        "queued": "_skip",
    }
    if compact in aliases:
        return aliases[compact]
    if "fork" in compact and "exec" in compact:
        return "fork_exec"
    if "mrc" in compact and "date" in compact:
        return "mrc_date_raw"
    if "submission" in compact:
        return "submission1"
    if text in {"➙", "->", "trend"}:
        return "_skip"
    return compact or "_skip"


def normalize_row(row: dict[str, Any], scrape_time: datetime) -> ScoreboardRow:
    team = parse_team_label(row.get("group"))
    return ScoreboardRow(
        scrape_time=scrape_time,
        rank=parse_int(row.get("rank")),
        team_label=team.team_label,
        team_name=team.team_name,
        team_id=team.team_id,
        visibility_mode=team.visibility_mode,
        is_anonymous=team.is_anonymous,
        has_stable_identity=team.has_stable_identity,
        display_name=team.display_name,
        build_ok=parse_status_bool(row.get("build")),
        boot_ok=parse_status_bool(row.get("boot")),
        score=parse_percentage(row.get("score")),
        threads=parse_percentage(row.get("threads")),
        fork_exec=parse_percentage(row.get("fork_exec")),
        other=parse_percentage(row.get("other")),
        avg_5d=parse_percentage(row.get("avg_5d")),
        up_count=parse_int(row.get("up_count")),
        tw=parse_percentage(row.get("tw")),
        perf=parse_percentage(row.get("perf")),
        page_date_raw=empty_to_none(row.get("page_date_raw")),
        mrc_date_raw=empty_to_none(row.get("mrc_date_raw")),
        submission1=empty_to_none(row.get("submission1")),
    )


def parse_team_label(value: Any) -> TeamIdentity:
    label = clean_text(value)
    if label == "???":
        return TeamIdentity(label, None, None, "anonymous", True, False, "???")

    named = NAMED_TEAM_RE.match(label)
    if named:
        team_name = clean_text(named.group("name")) or None
        team_id = named.group("team_id").upper()
        display_name = f"{team_name} ({team_id})" if team_name else team_id
        return TeamIdentity(label, team_name, team_id, "named", False, True, display_name)

    if TEAM_ID_RE.match(label):
        team_id = label.upper()
        return TeamIdentity(label, None, team_id, "id_only", False, True, team_id)

    return TeamIdentity(label, None, None, "unknown", False, False, label)


def parse_percentage(value: Any) -> float | None:
    text = clean_text(value)
    if not text or text in {"-", "nan", "None"}:
        return None
    text = text.replace("%", "").replace(",", ".")
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(match.group(0)) if match else None


def parse_int(value: Any) -> int | None:
    text = clean_text(value)
    if not text or text in {"-", "nan", "None"}:
        return None
    match = re.search(r"-?\d+", text.replace(",", ""))
    return int(match.group(0)) if match else None


def parse_status_bool(value: Any) -> bool | None:
    text = clean_text(value).lower()
    if text in {"ok", "pass", "passed", "success", "true", "yes", "1"}:
        return True
    if text in {"fail", "failed", "error", "false", "no", "0"}:
        return False
    return None


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).replace("\xa0", " ")
    return re.sub(r"\s+", " ", text).strip()


def empty_to_none(value: Any) -> str | None:
    text = clean_text(value)
    return text if text else None


def snapshot_hash(rows: list[ScoreboardRow]) -> str:
    payload = []
    for row in rows:
        item = asdict(row)
        item.pop("scrape_time", None)
        payload.append(item)
    encoded = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()
