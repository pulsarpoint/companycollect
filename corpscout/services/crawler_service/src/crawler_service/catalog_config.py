"""The catalog-loading boundary shared by the crawler and its MCP server."""

from pathlib import Path

from clickhouse_driver import Client
from clickhouse_driver.errors import Error as ClickHouseError

from crawler_service.technology_catalog import TechnologyCatalog, sync_catalog


def load_catalog(
    environment: dict[str, str], cache_path: Path | None, offline: bool
) -> TechnologyCatalog:
    if offline:
        if cache_path is None:
            raise ValueError("--offline-catalog requires --technology-catalog")
        return TechnologyCatalog.read(cache_path)
    if not environment.get("CLICKHOUSE_HOST"):
        raise ValueError(
            "Configure CLICKHOUSE_HOST for catalog sync, or provide --technology-catalog with --offline-catalog"
        )
    client = Client(
        host=environment["CLICKHOUSE_HOST"],
        port=int(environment.get("CLICKHOUSE_NATIVE_PORT", "9000")),
        user=environment.get("CLICKHOUSE_USER", "default"),
        password=environment.get("CLICKHOUSE_PASSWORD", ""),
        secure=environment.get("CLICKHOUSE_SECURE", "false").lower()
        in {"1", "true", "yes"},
        connect_timeout=10,
        send_receive_timeout=30,
        settings={"readonly": 1, "max_execution_time": 20},
    )
    try:
        return sync_catalog(
            client,
            cache_path or Path(".cache/company-research/technology-catalog.json"),
        )
    except (ClickHouseError, OSError, EOFError) as error:
        raise ValueError(
            "ClickHouse catalog sync failed; no catalog service or crawl was started"
        ) from error
    finally:
        client.disconnect()
