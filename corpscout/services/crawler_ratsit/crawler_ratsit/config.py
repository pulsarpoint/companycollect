import re
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from stat import S_IMODE
from typing import Self

from crawler_ratsit.constants import DEFAULT_TASK_QUEUE


@dataclass(frozen=True)
class TemporalSettings:
    temporal_address: str
    temporal_namespace: str
    temporal_task_queue: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> Self:
        return cls(
            temporal_address=_value(
                environment,
                "TEMPORAL_ADDRESS",
                fallback="127.0.0.1:7233",
            ),
            temporal_namespace=_value(
                environment,
                "TEMPORAL_NAMESPACE",
                fallback="default",
            ),
            temporal_task_queue=_value(
                environment,
                "RATSIT_TEMPORAL_TASK_QUEUE",
                fallback=DEFAULT_TASK_QUEUE,
            ),
        )


@dataclass(frozen=True)
class WorkerSettings:
    temporal: TemporalSettings
    cloakbrowser_license_key: str | None
    content_selector: str
    page_timeout_ms: int
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    s3_region: str
    s3_bucket: str
    s3_prefix: str
    clickhouse_host: str
    clickhouse_http_port: int
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_database: str
    clickhouse_secure: bool
    max_concurrent_activities: int

    @classmethod
    def from_environment(cls, environment: Mapping[str, str]) -> Self:
        return cls(
            temporal=TemporalSettings.from_environment(environment),
            cloakbrowser_license_key=(
                environment.get("CLOAKBROWSER_LICENSE_KEY", "").strip() or None
            ),
            content_selector=_value(
                environment,
                "RATSIT_CONTENT_SELECTOR",
                fallback="main .main-inner",
            ),
            page_timeout_ms=_positive_int(
                environment,
                "RATSIT_PAGE_TIMEOUT_MS",
                fallback="60000",
            ),
            s3_endpoint=_required(environment, "CORPSCOUT_S3_ENDPOINT"),
            s3_access_key=_required(environment, "CORPSCOUT_S3_ACCESS_KEY"),
            s3_secret_key=_required(environment, "CORPSCOUT_S3_SECRET_KEY"),
            s3_region=_value(
                environment,
                "CORPSCOUT_S3_REGION",
                fallback="us-east-1",
            ),
            s3_bucket=_value(
                environment,
                "RATSIT_S3_BUCKET",
                fallback="source-sweden-ratsit",
            ),
            s3_prefix=_prefix(environment),
            clickhouse_host=_value(
                environment,
                "CLICKHOUSE_HOST",
                fallback="127.0.0.1",
            ),
            clickhouse_http_port=_positive_int(
                environment,
                "CLICKHOUSE_HTTP_PORT",
                fallback="8123",
            ),
            clickhouse_user=_value(
                environment,
                "CLICKHOUSE_USER",
                fallback="default",
            ),
            clickhouse_password=environment.get("CLICKHOUSE_PASSWORD", ""),
            clickhouse_database=_value(
                environment,
                "CLICKHOUSE_DATABASE",
                fallback="corpscout",
            ),
            clickhouse_secure=_boolean(
                environment,
                "CLICKHOUSE_SECURE",
                fallback="false",
            ),
            max_concurrent_activities=_positive_int(
                environment,
                "RATSIT_MAX_CONCURRENT_ACTIVITIES",
                fallback="1",
            ),
        )


@dataclass(frozen=True)
class BrowserSettings:
    browser_id: str
    proxy_url: str | None


@dataclass(frozen=True)
class ProcessSettings:
    state_directory: Path
    headless: bool
    per_browser_activities_per_second: float
    task_queue_activities_per_second: float
    browsers: tuple[BrowserSettings, ...]

    @classmethod
    def from_file(cls, path: Path) -> Self:
        _require_private_file(path)
        try:
            document = tomllib.loads(path.read_text(encoding="utf-8"))
        except tomllib.TOMLDecodeError as error:
            raise ValueError(f"invalid process config {path}: {error}") from error
        except OSError as error:
            raise ValueError(f"cannot read process config {path}: {error}") from error

        process = _table(document, "process")
        state_directory = Path(
            _toml_string(process, "state_directory", table="process")
        )
        if not state_directory.is_absolute():
            raise ValueError("process.state_directory must be an absolute path")

        limits = _table(document, "limits")
        browsers = _enabled_browsers(document)
        if not browsers:
            raise ValueError("process config must enable at least one browser")

        return cls(
            state_directory=state_directory,
            headless=_toml_boolean(process, "headless", table="process"),
            per_browser_activities_per_second=_toml_positive_float(
                limits,
                "per_browser_activities_per_second",
                table="limits",
            ),
            task_queue_activities_per_second=_toml_positive_float(
                limits,
                "task_queue_activities_per_second",
                table="limits",
            ),
            browsers=browsers,
        )

def _required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name, "").strip()
    if not value:
        raise ValueError(f"{name} is required")
    return value


def _value(
    environment: Mapping[str, str],
    name: str,
    *,
    fallback: str,
) -> str:
    value = environment.get(name, fallback).strip()
    if not value:
        raise ValueError(f"{name} must not be blank")
    return value


def _positive_int(
    environment: Mapping[str, str],
    name: str,
    *,
    fallback: str,
) -> int:
    raw_value = _value(environment, name, fallback=fallback)
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer") from error
    if value < 1:
        raise ValueError(f"{name} must be positive")
    return value


def _boolean(
    environment: Mapping[str, str],
    name: str,
    *,
    fallback: str,
) -> bool:
    value = _value(environment, name, fallback=fallback).lower()
    if value in {"1", "true", "yes"}:
        return True
    if value in {"0", "false", "no"}:
        return False
    raise ValueError(f"{name} must be true or false")


def _prefix(environment: Mapping[str, str]) -> str:
    value = _value(
        environment,
        "RATSIT_S3_PREFIX",
        fallback="raw",
    ).strip("/")
    if not value:
        raise ValueError("RATSIT_S3_PREFIX must contain a path segment")
    return value


def _require_private_file(path: Path) -> None:
    try:
        file_stat = path.stat()
    except OSError as error:
        raise ValueError(f"cannot inspect process config {path}: {error}") from error
    if not path.is_file():
        raise ValueError(f"process config {path} must be a regular file")
    if S_IMODE(file_stat.st_mode) & 0o077:
        raise ValueError(f"process config {path} must have mode 0600 or stricter")


def _table(document: Mapping[str, object], name: str) -> Mapping[str, object]:
    value = document.get(name)
    if not isinstance(value, dict):
        raise ValueError(f"process config requires a [{name}] table")
    return value


def _toml_string(
    table_value: Mapping[str, object],
    name: str,
    *,
    table: str,
) -> str:
    value = table_value.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{table}.{name} must be a non-blank string")
    return value.strip()


def _toml_boolean(
    table_value: Mapping[str, object],
    name: str,
    *,
    table: str,
) -> bool:
    value = table_value.get(name)
    if not isinstance(value, bool):
        raise ValueError(f"{table}.{name} must be true or false")
    return value


def _toml_positive_float(
    table_value: Mapping[str, object],
    name: str,
    *,
    table: str,
) -> float:
    raw_value = table_value.get(name)
    if isinstance(raw_value, bool) or not isinstance(raw_value, int | float):
        raise ValueError(f"{table}.{name} must be a number")
    value = float(raw_value)
    if value <= 0:
        raise ValueError(f"{table}.{name} must be positive")
    return value


def _enabled_browsers(
    document: Mapping[str, object],
) -> tuple[BrowserSettings, ...]:
    raw_browsers = document.get("browsers")
    if not isinstance(raw_browsers, list):
        raise ValueError("process config requires one or more [[browsers]] entries")

    enabled_browsers: list[BrowserSettings] = []
    seen_ids: set[str] = set()
    for index, raw_browser in enumerate(raw_browsers):
        table_name = f"browsers[{index}]"
        if not isinstance(raw_browser, dict):
            raise ValueError(f"{table_name} must be a table")

        browser_id = _toml_string(raw_browser, "id", table=table_name)
        if re.fullmatch(r"[a-z0-9][a-z0-9_-]*", browser_id) is None:
            raise ValueError(
                f"{table_name}.id must use lowercase letters, digits, '-' or '_'"
            )
        if browser_id in seen_ids:
            raise ValueError(f"browser id {browser_id!r} is duplicated")
        seen_ids.add(browser_id)

        enabled = raw_browser.get("enabled", True)
        if not isinstance(enabled, bool):
            raise ValueError(f"{table_name}.enabled must be true or false")
        if not enabled:
            continue

        proxy_url_value = raw_browser.get("proxy_url")
        if proxy_url_value is None:
            proxy_url = None
        elif isinstance(proxy_url_value, str) and proxy_url_value.strip():
            proxy_url = proxy_url_value.strip()
        else:
            raise ValueError(f"{table_name}.proxy_url must be a non-blank string")

        enabled_browsers.append(
            BrowserSettings(
                browser_id=browser_id,
                proxy_url=proxy_url,
            )
        )
    return tuple(enabled_browsers)
