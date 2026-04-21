from __future__ import annotations

import os
from dataclasses import dataclass

def _required_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        raise ValueError(f"missing required environment variable: {name}")
    return value.strip()


def _bool_env(name: str) -> bool:
    value = _required_env(name).lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"invalid boolean value for {name}: {value}")


def _int_env(name: str) -> int:
    value = _required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"invalid integer value for {name}: {value}") from exc


@dataclass(frozen=True)
class Settings:
    scoreboard_url: str
    scrape_interval_seconds: int
    http_timeout_seconds: int
    http_user_agent: str
    influx_url: str
    influx_token: str
    influx_org: str
    influx_bucket: str
    log_level: str
    save_failed_html: bool
    skip_identical_snapshots: bool
    failed_html_dir: str
    health_host: str
    health_port: int


def load_settings() -> Settings:
    return Settings(
        scoreboard_url=_required_env("SCOREBOARD_URL"),
        scrape_interval_seconds=_int_env("SCRAPE_INTERVAL_SECONDS"),
        http_timeout_seconds=_int_env("HTTP_TIMEOUT_SECONDS"),
        http_user_agent=_required_env("HTTP_USER_AGENT"),
        influx_url=_required_env("INFLUX_URL"),
        influx_token=_required_env("INFLUX_TOKEN"),
        influx_org=_required_env("INFLUX_ORG"),
        influx_bucket=_required_env("INFLUX_BUCKET"),
        log_level=_required_env("LOG_LEVEL"),
        save_failed_html=_bool_env("SAVE_FAILED_HTML"),
        skip_identical_snapshots=_bool_env("SKIP_IDENTICAL_SNAPSHOTS"),
        failed_html_dir=_required_env("FAILED_HTML_DIR"),
        health_host=_required_env("HEALTH_HOST"),
        health_port=_int_env("HEALTH_PORT"),
    )
