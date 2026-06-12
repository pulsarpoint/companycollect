"""Load parsed PRH XBRL rows into ClickHouse via the platform resource."""

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_xbrl.tables import TABLE_COLUMNS


def load_rows(clickhouse: ClickHouseResource, rows_by_table: dict[str, list[dict]]) -> dict[str, int]:
    client = clickhouse.client()
    counts: dict[str, int] = {}
    for table, rows in rows_by_table.items():
        clickhouse.insert_rows(client, table, TABLE_COLUMNS[table], rows)
        counts[table] = len(rows)
    return counts
