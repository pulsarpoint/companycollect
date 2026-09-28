"""Derived table contributions; only explicit registered tables can be queried."""

from datetime import UTC, datetime
from uuid import uuid4

import dagster as dg
from corpscout_identity.coordination import inventory_publication_lock
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.domains.registration import PUBLISH_POOL

TABLE = "corpscout.domains_sources"
COLUMNS = ("domain_id", "source_table", "first_seen_at", "last_seen_at", "updated_at", "source_run_id")
# Table names from index rows are data, never SQL identifiers. Register a new
# contributor here and explicitly extend the country readers in a migration.
CONTRIBUTORS = {
    "se_company_domain": "SELECT domain_id,first_seen_at,last_seen_at FROM corpscout.se_company_domain FINAL",
    "commoncrawl_domains": "SELECT domain_id,first_seen_at,last_seen_at FROM corpscout.domains WHERE has(sources,'commoncrawl')",
    "commoncrawl_domain_graph_nodes": "SELECT domain_id,first_seen_at,last_seen_at FROM corpscout.domains WHERE has(sources,'commoncrawl_graph')",
}
SETTINGS = {
    "max_threads": 4, "max_execution_time": 3600, "async_insert": 0,
    "max_memory_usage": 4 * 1024**3,
    "max_bytes_before_external_group_by": 256 * 1024**2,
    "max_bytes_before_external_sort": 256 * 1024**2,
}


class DomainSourcesConfig(dg.Config):
    source_tables: list[str] = Field(default_factory=lambda: list(CONTRIBUTORS))


def require_compact_index(client, table: str) -> None:
    columns = tuple(row[0] for row in client.execute(
        "SELECT name FROM system.columns WHERE database='corpscout' AND table=%(table)s ORDER BY position",
        {"table": table.removeprefix("corpscout.")},
    ))
    if columns != COLUMNS:
        raise ValueError(f"{table} is not the compact index; complete the coordinated index cutover first")


def publish_source_index(client, *, source_tables: list[str], run_id: str, log) -> dict:
    if not source_tables or len(set(source_tables)) != len(source_tables):
        raise ValueError("Select each contributing source table once")
    unknown = set(source_tables) - CONTRIBUTORS.keys()
    if unknown:
        raise ValueError(f"Unregistered domain contributors: {sorted(unknown)}")
    require_compact_index(client, TABLE)
    counts = {}
    # The Dagster publication pool serializes index builds. External identity
    # registrars never write this index. Only final publication needs their guard.
    for source in source_tables:
        suffix = uuid4().hex
        stage = f"{TABLE}_stage_{suffix}"
        prefix = f"domain-sources:{suffix}:"
        params = {"source": source, "run": run_id, "stamp": datetime.now(UTC)}
        client.execute(f"CREATE TABLE {stage} AS {TABLE}")
        try:
            # Country partitions are small. Bound the much larger bulk sources
            # by the leading SHA-256 digit, without scanning any other partition.
            ranges = [("", "g")] if source == "se_company_domain" else list(zip("0123456789abcdef", "123456789abcdefg", strict=True))
            for lower, upper in ranges:
                scope = "domain_id >= %(lower)s AND domain_id < %(upper)s"
                page_params = {**params, "lower": lower, "upper": upper}
                client.execute(f"""INSERT INTO {stage} ({','.join(COLUMNS)})
                    SELECT domain_id,%(source)s,min(first_seen_at),max(last_seen_at),%(stamp)s,%(run)s
                    FROM (
                        SELECT domain_id,first_seen_at,last_seen_at FROM {TABLE} FINAL
                        WHERE source_table=%(source)s AND {scope}
                        UNION ALL
                        SELECT domain_id,first_seen_at,last_seen_at FROM ({CONTRIBUTORS[source]})
                        WHERE {scope}
                    ) GROUP BY domain_id
                """, page_params, settings=SETTINGS, query_id=f"{prefix}build-{lower}")
                # Never publish references to absent parents, including a broken
                # country summary. Retain the previous index if validation fails.
                client.execute(f"""SELECT throwIf(count()>0,'Domain contribution has no registered parent')
                    FROM {stage} WHERE {scope} AND domain_id NOT IN (
                        SELECT domain_id FROM corpscout.domains WHERE domain_id IN (
                            SELECT domain_id FROM {stage} WHERE {scope}))
                """, page_params, settings=SETTINGS, query_id=f"{prefix}validate-{lower}")
            [(count,)] = client.execute(f"SELECT count() FROM {stage}")
            if count:
                with inventory_publication_lock():
                    client.execute(f"ALTER TABLE {TABLE} REPLACE PARTITION %(source)s FROM {stage}",
                                   params, query_id=f"{prefix}publish")
            counts[source] = count
            log("Domain contributions: source=%s domains=%d", source, count)
        except BaseException:
            client.disconnect()
            client.execute("KILL QUERY WHERE startsWith(query_id,%(prefix)s) SYNC", {"prefix": prefix})
            raise
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")
    return {"contributions": counts, "source_run_id": run_id}


@dg.asset(
    group_name="domains", kinds={"clickhouse"}, pool=PUBLISH_POOL,
    deps=["domains", "se_company_domain_publish"],
    metadata={"dagster/table_name": TABLE},
    description="Derive one contribution per domain and source table. Preserve discovery history irrespective of company review or withdrawal. No crawling or LLM calls.",
)
def domains_sources(
    context: dg.AssetExecutionContext, config: DomainSourcesConfig, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        result = publish_source_index(client, source_tables=config.source_tables,
                                      run_id=context.run_id, log=context.log.info)
    return dg.MaterializeResult(metadata=result)
