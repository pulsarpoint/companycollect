from contextlib import contextmanager

import dagster as dg
import pytest

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    CT_HOSTNAME_SHARD_COUNT,
    CT_HOSTNAME_UPSERT_SQL,
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
    def __init__(self) -> None:
        self.executed: list[tuple[str, dict[str, int]]] = []
        self.last_query = FakeLastQuery()

    def execute(self, sql: str, params: dict[str, int]) -> None:
        self.executed.append((sql, params))


class FakeClickhouseResource:
    def __init__(self) -> None:
        self.client = FakeClickhouseClient()

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

    assert clickhouse.client.executed == [
        (
            CT_HOSTNAME_UPSERT_SQL,
            {"shard_count": 16, "shard_index": 3},
        )
    ]
    assert result.metadata == {
        "source_table": "ctlogs.hostnames",
        "membership_table": "corpscout.commoncrawl_domains",
        "destination_table": "corpscout.commoncrawl_domain_hostnames",
        "shard_index": 3,
        "shard_count": 16,
        "written_rows": 37,
        "written_bytes": 4096,
        "elapsed_seconds": 1.25,
    }
