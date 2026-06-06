from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    from app.config import Settings
    from app.influx_writer import InfluxWriter

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class GitCommit:
    sha: str
    short_sha: str
    author: str
    message: str
    committed_at: datetime


def fetch_commits(
    api_url: str,
    project_id: str,
    token: str,
    since: str,
    timeout_seconds: int = 10,
    attempts: int = 3,
    backoff_seconds: float = 1.0,
) -> list[dict]:
    url = f"{api_url}/projects/{project_id}/repository/commits"
    params = {"all": "true", "since": since, "per_page": 100}
    headers = {"PRIVATE-TOKEN": token}
    last_exc: Exception | None = None
    for attempt in range(attempts):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=timeout_seconds)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            last_exc = exc
            if attempt < attempts - 1:
                time.sleep(backoff_seconds * (2 ** attempt))
    raise RuntimeError(f"gitlab fetch failed after {attempts} attempts") from last_exc


def parse_commits(raw: list[dict]) -> list[GitCommit]:
    commits = []
    for item in raw:
        committed_at = datetime.fromisoformat(item["committed_date"])
        commits.append(
            GitCommit(
                sha=item["id"],
                short_sha=item["id"][:8],
                author=item["author_name"],
                message=item["title"],
                committed_at=committed_at,
            )
        )
    commits.sort(key=lambda c: c.committed_at)
    return commits


def _filter_new_commits(commits: list[GitCommit], last_seen_sha: str | None) -> list[GitCommit]:
    if last_seen_sha is None:
        return commits
    for i, commit in enumerate(commits):
        if commit.sha == last_seen_sha:
            return commits[i + 1:]
    return commits


class GitLabPoller:
    def __init__(self, settings: Settings, writer: InfluxWriter) -> None:
        self._settings = settings
        self._writer = writer
        self._last_seen_sha: str | None = None
        self._since: str = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()

    def poll_once(self) -> None:
        try:
            raw = fetch_commits(
                self._settings.gitlab_api_url,
                self._settings.gitlab_project_id,
                self._settings.gitlab_api_token,
                self._since,
            )
            commits = parse_commits(raw)
            new_commits = _filter_new_commits(commits, self._last_seen_sha)
            if not new_commits:
                LOGGER.debug("gitlab poll: no new commits")
                return
            written = self._writer.write_git_commits(
                new_commits,
                project_id=self._settings.gitlab_project_id,
            )
            self._last_seen_sha = new_commits[-1].sha
            self._since = new_commits[-1].committed_at.isoformat()
            LOGGER.info(
                "gitlab poll finished",
                extra={"new_commits": written, "last_sha": self._last_seen_sha[:8]},
            )
        except Exception:
            LOGGER.exception("gitlab poll failed")


def run_poller(settings: Settings, writer: InfluxWriter, stop_event: threading.Event) -> None:
    if not settings.gitlab_enabled:
        LOGGER.warning("gitlab poller disabled: GITLAB_API_TOKEN/URL/PROJECT_ID not set")
        return
    poller = GitLabPoller(settings, writer)
    LOGGER.info("gitlab poller started", extra={"project_id": settings.gitlab_project_id})
    while not stop_event.is_set():
        poller.poll_once()
        stop_event.wait(settings.gitlab_poll_interval_seconds)
