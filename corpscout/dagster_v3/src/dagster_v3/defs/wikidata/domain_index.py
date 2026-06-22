from __future__ import annotations

import json
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from pydantic import field_validator

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.wikidata.source import (
    DEFAULT_WIKIDATA_REQUEST_DELAY_SECONDS,
    DEFAULT_WIKIDATA_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_WIKIDATA_USER_AGENT,
    query_hash,
    response_bindings,
)

WIKIDATA_DOMAIN_INDEX_RAW_BUCKET = "source-wikidata-domain-index"
DEFAULT_WIKIDATA_DOMAIN_INDEX_PAGE_SIZE = 10_000


class WikidataDomainIndexRawPullConfig(dg.Config):
    page_size: int = DEFAULT_WIKIDATA_DOMAIN_INDEX_PAGE_SIZE
    max_pages: int | None = None
    request_timeout_seconds: int = DEFAULT_WIKIDATA_REQUEST_TIMEOUT_SECONDS
    request_delay_seconds: float = DEFAULT_WIKIDATA_REQUEST_DELAY_SECONDS
    user_agent: str = DEFAULT_WIKIDATA_USER_AGENT

    @field_validator("page_size", "request_timeout_seconds")
    @classmethod
    def validate_positive_int(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("value must be greater than zero")
        return value

    @field_validator("max_pages")
    @classmethod
    def validate_optional_positive_int(cls, value: int | None) -> int | None:
        if value is not None and value <= 0:
            raise ValueError("value must be greater than zero")
        return value

    @field_validator("request_delay_seconds")
    @classmethod
    def validate_non_negative_delay(cls, value: float) -> float:
        if value < 0:
            raise ValueError("request_delay_seconds must be zero or greater")
        return value

    @field_validator("user_agent")
    @classmethod
    def validate_required_string(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("value must not be blank")
        return stripped


def build_wikidata_domain_index_query(
    *,
    limit: int,
    offset: int,
) -> str:
    return f"""
SELECT
  ?entity
  ?website
WHERE {{
  ?entity wdt:P856 ?website .
}}
LIMIT {limit}
OFFSET {offset}
""".strip()


def pull_wikidata_domain_index_raw_objects(
    *,
    client: Any,
    object_store: ObjectStoreResource,
    config: WikidataDomainIndexRawPullConfig,
    run_id: str,
    retrieved_at: str,
    sleep: Callable[[float], None],
) -> dg.MaterializeResult:
    object_store.ensure_bucket(WIKIDATA_DOMAIN_INDEX_RAW_BUCKET)
    retrieved_date = retrieved_at[:10]
    first_query = build_wikidata_domain_index_query(limit=config.page_size, offset=0)
    current_query_hash = query_hash(first_query)
    page_count = 0
    row_count = 0
    object_keys: list[str] = []
    last_offset = 0

    while config.max_pages is None or page_count < config.max_pages:
        offset = page_count * config.page_size
        query = build_wikidata_domain_index_query(
            limit=config.page_size,
            offset=offset,
        )
        payload = client.fetch(query, user_agent=config.user_agent)
        bindings = response_bindings(payload)
        if not bindings:
            break

        page_count += 1
        row_count += len(bindings)
        last_offset = offset
        object_key = wikidata_domain_index_page_object_key(
            retrieved_date=retrieved_date,
            run_id=run_id,
            page_number=page_count,
        )
        object_store.write_json(
            object_key,
            json.dumps(payload, sort_keys=True, separators=(",", ":")),
            bucket=WIKIDATA_DOMAIN_INDEX_RAW_BUCKET,
        )
        object_keys.append(object_key)

        if len(bindings) < config.page_size:
            break
        if config.request_delay_seconds > 0:
            sleep(config.request_delay_seconds)

    manifest_key = wikidata_domain_index_manifest_object_key(
        retrieved_date=retrieved_date,
        run_id=run_id,
    )
    manifest = {
        "source": "wikidata",
        "query_mode": "official_website_domain_index",
        "run_id": run_id,
        "retrieved_date": retrieved_date,
        "page_size": config.page_size,
        "query_hash": current_query_hash,
        "row_count": row_count,
        "page_count": page_count,
        "last_offset": last_offset,
        "started_at": retrieved_at,
        "completed_at": datetime.now(UTC).isoformat(),
        "objects": object_keys,
    }
    object_store.write_json(
        manifest_key,
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        bucket=WIKIDATA_DOMAIN_INDEX_RAW_BUCKET,
    )
    deleted_old_raw_object_count = delete_old_wikidata_domain_index_raw_objects(
        object_store=object_store,
        run_id=run_id,
    )

    return dg.MaterializeResult(
        metadata={
            "bucket": WIKIDATA_DOMAIN_INDEX_RAW_BUCKET,
            "page_count": page_count,
            "row_count": row_count,
            "manifest_key": manifest_key,
            "deleted_old_raw_object_count": deleted_old_raw_object_count,
            "retrieved_at": retrieved_at,
        }
    )


def wikidata_domain_index_page_object_key(
    *,
    retrieved_date: str,
    run_id: str,
    page_number: int,
) -> str:
    return (
        f"raw/run_id={run_id}/retrieved_date={retrieved_date}/"
        f"page={page_number:06d}.json"
    )


def wikidata_domain_index_manifest_object_key(
    *,
    retrieved_date: str,
    run_id: str,
) -> str:
    return f"raw/run_id={run_id}/retrieved_date={retrieved_date}/manifest.json"


def delete_old_wikidata_domain_index_raw_objects(
    *,
    object_store: ObjectStoreResource,
    run_id: str,
) -> int:
    raw_prefix = "raw/"
    current_run_prefix = f"{raw_prefix}run_id={run_id}/"
    stale_keys = [
        key
        for key in object_store.list_keys(
            raw_prefix,
            bucket=WIKIDATA_DOMAIN_INDEX_RAW_BUCKET,
        )
        if not key.startswith(current_run_prefix)
    ]
    return object_store.delete_keys(
        stale_keys,
        bucket=WIKIDATA_DOMAIN_INDEX_RAW_BUCKET,
    )
