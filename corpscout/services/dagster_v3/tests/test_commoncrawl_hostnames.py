import dagster as dg
import pytest

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    DNS_RECORD_OBSERVATIONS_KEY,
    DOMAIN_HOSTNAMES_KEY,
    HOSTNAME_SHARD_COUNT,
    domain_hostnames,
    hostname_shard_index,
)
from dagster_v3.defs.commoncrawl_hostnames.definitions import defs as hostname_defs


def test_hostname_source_shard_key_validation() -> None:
    partition_keys = COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS.get_partition_keys()

    assert isinstance(
        COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
        dg.StaticPartitionsDefinition,
    )
    assert len(partition_keys) == HOSTNAME_SHARD_COUNT == 16
    assert partition_keys[0] == "shard_00"
    assert partition_keys[-1] == "shard_15"
    assert hostname_shard_index("shard_07") == 7

    for partition_key in ("shard_16", "shard_-1", "07", "shard_x"):
        with pytest.raises(ValueError):
            hostname_shard_index(partition_key)


def test_domain_hostnames_is_an_external_dns_observation_view() -> None:
    asset_graph = hostname_defs.get_repository_def().asset_graph
    view = asset_graph.get(DOMAIN_HOSTNAMES_KEY)

    assert domain_hostnames.key == DOMAIN_HOSTNAMES_KEY
    assert view.parent_keys == {DNS_RECORD_OBSERVATIONS_KEY}
    assert view.is_external is True
    assert view.is_materializable is False
    assert view.is_observable is False
    assert view.partitions_def is COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS
    assert view.metadata["table"] == "corpscout.domain_hostnames"
    assert view.metadata["source_table"] == (
        "corpscout.commoncrawl_domain_dns_record_observations"
    )
    assert view.kinds == {"clickhouse", "dns", "view"}


def test_domain_hostnames_has_no_materialization_automation() -> None:
    repository = hostname_defs.get_repository_def()

    assert hostname_defs.sensors is None
    assert hostname_defs.schedules is None
    assert all(
        DOMAIN_HOSTNAMES_KEY not in job.asset_layer.executable_asset_keys
        for job in repository.get_all_jobs()
    )
