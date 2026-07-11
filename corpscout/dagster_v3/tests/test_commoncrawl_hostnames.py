from contextlib import contextmanager
from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    CT_HOSTNAME_SHARD_COUNT,
    CT_HOSTNAME_SYNC_CHECKPOINT_SQL,
    CT_HOSTNAME_SYNC_STATE_SQL,
    CT_HOSTNAME_UPSERT_SQL,
    CT_HOSTNAME_WATERMARKS_SQL,
    commoncrawl_domain_hostnames,
    ct_hostname_shard_index,
)


class FakeProgress:
    written_rows = 37
    written_bytes = 4096


class FakeLastQuery:
    progress = FakeProgress()
    elapsed = 1.25


class FakeClickhouseClient:
    def __init__(
        self,
        *,
        state_rows: list[tuple] | None = None,
        watermark_rows: list[tuple] | None = None,
        fail_on_checkpoint: bool = False,
    ) -> None:
        self.executed: list[tuple[str, dict[str, object]]] = []
        self.last_query = FakeLastQuery()
        self.state_rows = state_rows if state_rows is not None else [(0, None, None)]
        self.watermark_rows = watermark_rows or [
            (
                datetime(2026, 7, 11, 10, 0, tzinfo=UTC),
                datetime(2026, 7, 11, 9, 0, tzinfo=UTC),
            )
        ]
        self.fail_on_checkpoint = fail_on_checkpoint

    def execute(self, sql: str, params: dict[str, object]):
        self.executed.append((sql, params))
        if sql == CT_HOSTNAME_SYNC_STATE_SQL:
            return self.state_rows
        if sql == CT_HOSTNAME_WATERMARKS_SQL:
            return self.watermark_rows
        if sql == CT_HOSTNAME_SYNC_CHECKPOINT_SQL and self.fail_on_checkpoint:
            raise RuntimeError("checkpoint unavailable")
        return []


class FakeClickhouseResource:
    def __init__(self, client: FakeClickhouseClient | None = None) -> None:
        self.client = client or FakeClickhouseClient()

    @contextmanager
    def get_connection(self):
        yield self.client


def test_hostname_asset_uses_ct_physical_hash_shards() -> None:
    partition_keys = COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS.get_partition_keys()

    assert isinstance(
        COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
        dg.StaticPartitionsDefinition,
    )
    assert len(partition_keys) == CT_HOSTNAME_SHARD_COUNT == 16
    assert partition_keys[0] == "shard_00"
    assert partition_keys[-1] == "shard_15"
    assert commoncrawl_domain_hostnames.partitions_def is (
        COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS
    )


def test_hostname_shard_key_validation() -> None:
    assert ct_hostname_shard_index("shard_07") == 7

    for partition_key in ("shard_16", "shard_-1", "07", "shard_x"):
        with pytest.raises(ValueError):
            ct_hostname_shard_index(partition_key)


def test_ct_hostname_query_enforces_membership_and_hostname_rules() -> None:
    assert "FROM ctlogs.hostnames" in CT_HOSTNAME_UPSERT_SQL
    assert "FROM corpscout.commoncrawl_domains" in CT_HOSTNAME_UPSERT_SQL
    assert "registered_domain IN" in CT_HOSTNAME_UPSERT_SQL
    assert "max(is_wildcard) AS is_wildcard" in CT_HOSTNAME_UPSERT_SQL
    assert "is_wildcard = 0" in CT_HOSTNAME_UPSERT_SQL
    assert "fqdn_normalized != domain_normalized" in CT_HOSTNAME_UPSERT_SQL
    assert "endsWith(fqdn_normalized, concat('.', domain_normalized))" in (
        CT_HOSTNAME_UPSERT_SQL
    )
    assert "position(fqdn_normalized, '*') = 0" in CT_HOSTNAME_UPSERT_SQL
    assert "toDateTime64(0, 3, 'UTC') AS last_resolved" in (CT_HOSTNAME_UPSERT_SQL)
    assert "max(last_not_after) AS last_not_after" in CT_HOSTNAME_UPSERT_SQL
    assert "max(last_ingested_at) AS last_ingested_at" in CT_HOSTNAME_UPSERT_SQL
    assert "%(is_bootstrap)s = 1" in CT_HOSTNAME_UPSERT_SQL
    assert "last_ingested_at > %(ct_ingested_after)s" in CT_HOSTNAME_UPSERT_SQL
    assert "resolved_at > %(domains_resolved_after)s" in CT_HOSTNAME_UPSERT_SQL
    assert "cityHash64(registered_domain) %% %(shard_count)s = %(shard_index)s" in (
        CT_HOSTNAME_UPSERT_SQL
    )
    assert "TRUNCATE" not in CT_HOSTNAME_UPSERT_SQL.upper()
    assert "DROP TABLE" not in CT_HOSTNAME_UPSERT_SQL.upper()


def test_hostname_asset_executes_one_append_only_shard() -> None:
    clickhouse = FakeClickhouseResource()

    result = commoncrawl_domain_hostnames(
        context=dg.build_asset_context(partition_key="shard_03"),
        clickhouse=clickhouse,
    )

    executed_sql = [sql for sql, _ in clickhouse.client.executed]
    assert executed_sql == [
        CT_HOSTNAME_SYNC_STATE_SQL,
        CT_HOSTNAME_WATERMARKS_SQL,
        CT_HOSTNAME_UPSERT_SQL,
        CT_HOSTNAME_SYNC_CHECKPOINT_SQL,
    ]
    upsert_parameters = clickhouse.client.executed[2][1]
    assert upsert_parameters["shard_count"] == 16
    assert upsert_parameters["shard_index"] == 3
    assert upsert_parameters["is_bootstrap"] == 1
    assert upsert_parameters["ct_ingested_after"] == datetime(1970, 1, 1, tzinfo=UTC)
    assert upsert_parameters["domains_resolved_after"] == datetime(
        1970, 1, 1, tzinfo=UTC
    )
    assert result.metadata == {
        "source_table": "ctlogs.hostnames",
        "membership_table": "corpscout.commoncrawl_domains",
        "destination_table": "corpscout.commoncrawl_domain_hostnames",
        "shard_index": 3,
        "shard_count": 16,
        "bootstrap": True,
        "ct_ingested_after": "1970-01-01T00:00:00+00:00",
        "ct_ingested_through": "2026-07-11T10:00:00+00:00",
        "domains_resolved_after": "1970-01-01T00:00:00+00:00",
        "domains_resolved_through": "2026-07-11T09:00:00+00:00",
        "written_rows": 37,
        "written_bytes": 4096,
        "elapsed_seconds": 1.25,
    }


def test_incremental_sync_uses_both_independent_cursors() -> None:
    ct_after = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    domains_after = datetime(2026, 7, 10, 13, 0, tzinfo=UTC)
    client = FakeClickhouseClient(
        state_rows=[(1, ct_after, domains_after)],
    )
    clickhouse = FakeClickhouseResource(client)

    commoncrawl_domain_hostnames(
        context=dg.build_asset_context(partition_key="shard_04"),
        clickhouse=clickhouse,
    )

    upsert_parameters = client.executed[2][1]
    assert upsert_parameters["is_bootstrap"] == 0
    assert upsert_parameters["ct_ingested_after"] == ct_after
    assert upsert_parameters["domains_resolved_after"] == domains_after


def test_checkpoint_failure_leaves_source_cursors_unadvanced() -> None:
    client = FakeClickhouseClient(fail_on_checkpoint=True)

    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        commoncrawl_domain_hostnames(
            context=dg.build_asset_context(partition_key="shard_05"),
            clickhouse=FakeClickhouseResource(client),
        )

    assert [sql for sql, _ in client.executed] == [
        CT_HOSTNAME_SYNC_STATE_SQL,
        CT_HOSTNAME_WATERMARKS_SQL,
        CT_HOSTNAME_UPSERT_SQL,
        CT_HOSTNAME_SYNC_CHECKPOINT_SQL,
    ]


def test_sync_query_is_append_only_and_preserves_axfr_rows() -> None:
    assert "TRUNCATE" not in CT_HOSTNAME_UPSERT_SQL.upper()
    assert "ALTER TABLE" not in CT_HOSTNAME_UPSERT_SQL.upper()
    assert "DELETE" not in CT_HOSTNAME_UPSERT_SQL.upper()
