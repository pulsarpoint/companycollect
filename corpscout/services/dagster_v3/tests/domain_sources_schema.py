"""Migration-owned central schema for disposable ClickHouse tests."""

from pathlib import Path

MIGRATIONS = Path(__file__).parents[3] / "clickhouse/migrations"


def central_schema_sql() -> str:
    base = "\n".join((MIGRATIONS / name).read_text() for name in (
        "000441_corpscout_domains_inventory.up.sql",
        "000442_corpscout_websites_and_pages.up.sql",
        "000443_corpscout_domains_search.up.sql",
    ))
    # Schema-only fixtures do not create the country fold. The full migration is
    # exercised separately, including its parent/source backfill and joined view.
    sources = (MIGRATIONS / "000462_corpscout_domains_sources.up.sql").read_text()
    return base + sources[:sources.index("-- Create missing parents")]


def execute_sql(client, sql: str) -> None:
    for statement in sql.split(";"):
        if statement.strip():
            client.execute(statement)


def compact_schema_sql() -> str:
    sql = (MIGRATIONS / "000467_corpscout_compact_domain_sources.up.sql").read_text()
    return sql[:sql.index("-- Association status")]
