from collections.abc import Mapping
from dataclasses import dataclass
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
    cdp_url: str
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
            cdp_url=_value(
                environment,
                "RATSIT_CDP_URL",
                fallback="http://127.0.0.1:9222",
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
