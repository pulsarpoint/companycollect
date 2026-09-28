"""Central parent registration at API admission and durable publication boundaries."""

from datetime import UTC, datetime
from urllib.parse import parse_qs, unquote, urlsplit

from clickhouse_driver import Client
from corpscout_identity.observations import register_crawl_results
from corpscout_identity.registration import (
    WebsiteObservation,
    identify_website,
    register_websites,
)
from corpscout_identity.urls import website_reference


def connect(environment: dict[str, str]) -> Client:
    value = environment.get("CLICKHOUSE_NATIVE_URL") or environment.get(
        "CLICKHOUSE_MIGRATE_URL"
    )
    if not value:
        raise ValueError("Set CLICKHOUSE_NATIVE_URL or CLICKHOUSE_MIGRATE_URL")
    parsed = urlsplit(value)
    if parsed.scheme not in {"clickhouse", "clickhouses"} or parsed.hostname is None:
        raise ValueError("Expected a native ClickHouse connection URL")
    options = parse_qs(parsed.query)
    secure = parsed.scheme == "clickhouses" or options.get("secure", ["false"])[
        0
    ].lower() in {"true", "1"}
    return Client(
        host=parsed.hostname,
        port=parsed.port or (9440 if secure else 9000),
        user=options.get("username", [unquote(parsed.username or "default")])[0],
        password=options.get("password", [unquote(parsed.password or "")])[0],
        database="corpscout",
        secure=secure,
        connect_timeout=10,
        send_receive_timeout=120,
    )


def registration_connection(environment: dict[str, str]) -> tuple[Client, str]:
    native = environment.get("CLICKHOUSE_NATIVE_URL")
    processing = environment.get("PROCESSING_PG_URL")
    if not native or not processing:
        raise ValueError(
            "Configure CLICKHOUSE_NATIVE_URL and PROCESSING_PG_URL for central identity registration"
        )
    return connect(environment), processing


def register_requests(
    environment: dict[str, str], requests: list[dict], *, run_id: str
) -> None:
    now = datetime.now(UTC)
    observations = []
    for request in requests:
        website_reference(request["url"], request["website_id"])
        observations.append(
            WebsiteObservation(identify_website(request["url"]), now, None, None)
        )
    client, processing = registration_connection(environment)
    try:
        register_websites(
            client,
            observations,
            source="crawler_requests",
            run_id=run_id,
            processing_url=processing,
        )
    finally:
        client.disconnect()


def register_results(environment: dict[str, str], results: list[dict]) -> None:
    client, processing = registration_connection(environment)
    try:
        register_crawl_results(
            client,
            results,
            source="website_company_lookup_results",
            run_id="crawler-publication",
            processing_url=processing,
        )
    finally:
        client.disconnect()
