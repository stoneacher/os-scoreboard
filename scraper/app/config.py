from __future__ import annotations

import os
from dataclasses import dataclass


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or value.strip() == "":
        return default
    return int(value)


@dataclass(frozen=True)
class Settings:
    scoreboard_url: str = "https://sweb.student.isec.tugraz.at/index.php"
    scrape_interval_seconds: int = 300
    http_timeout_seconds: int = 20
    http_user_agent: str = "uni-scoreboard-monitor/1.0"
    influx_url: str = "http://influxdb:8086"
    influx_token: str = "change-this-long-random-token-before-deploying"
    influx_org: str = "uni-scoreboard"
    influx_bucket: str = "scoreboard"
    log_level: str = "INFO"
    save_failed_html: bool = True
    skip_identical_snapshots: bool = False
    failed_html_dir: str = "/app/failed_html"
    health_host: str = "0.0.0.0"
    health_port: int = 8000


def load_settings() -> Settings:
    return Settings(
        scoreboard_url=os.getenv("SCOREBOARD_URL", Settings.scoreboard_url),
        scrape_interval_seconds=_int_env(
            "SCRAPE_INTERVAL_SECONDS", Settings.scrape_interval_seconds
        ),
        http_timeout_seconds=_int_env(
            "HTTP_TIMEOUT_SECONDS", Settings.http_timeout_seconds
        ),
        http_user_agent=os.getenv("HTTP_USER_AGENT", Settings.http_user_agent),
        influx_url=os.getenv("INFLUX_URL", Settings.influx_url),
        influx_token=os.getenv("INFLUX_TOKEN", Settings.influx_token),
        influx_org=os.getenv("INFLUX_ORG", Settings.influx_org),
        influx_bucket=os.getenv("INFLUX_BUCKET", Settings.influx_bucket),
        log_level=os.getenv("LOG_LEVEL", Settings.log_level),
        save_failed_html=_bool_env("SAVE_FAILED_HTML", Settings.save_failed_html),
        skip_identical_snapshots=_bool_env(
            "SKIP_IDENTICAL_SNAPSHOTS", Settings.skip_identical_snapshots
        ),
        failed_html_dir=os.getenv("FAILED_HTML_DIR", Settings.failed_html_dir),
        health_host=os.getenv("HEALTH_HOST", Settings.health_host),
        health_port=_int_env("HEALTH_PORT", Settings.health_port),
    )
