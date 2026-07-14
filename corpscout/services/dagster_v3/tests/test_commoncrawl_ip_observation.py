from collections.abc import Iterator
from contextlib import contextmanager

import dagster as dg
import pytest
from dagster._core.definitions.data_version import StaleStatus
from dagster._utils.test.data_versions import get_stale_status_resolver

from dagster_v3.defs.commoncrawl_geoip.assets import commoncrawl_ip_geoip
from dagster_v3.defs.commoncrawl_ip import (
    COMMONCRAWL_IP_ADDRESSES_KEY,
    COMMONCRAWL_IP_PARTITIONS,
)
from dagster_v3.defs.commoncrawl_ip_observation import (
    COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL,
    commoncrawl_ip_addresses_observation,
    commoncrawl_ip_addresses_observation_job,
    commoncrawl_ip_count_from_query,
    commoncrawl_ip_membership_data_version,
    defs as observation_defs,
)
from dagster_v3.defs.commoncrawl_rdap.assets import commoncrawl_ip_rdap_networks


class FakeQueryClient:
    def __init__(self, ip_count: int) -> None:
        self.ip_count = ip_count
        self.queries: list[tuple[str, dict[str, int]]] = []

    def execute(
        self,
        query: str,
        parameters: dict[str, int],
    ) -> list[tuple[int]]:
        self.queries.append((query, parameters))
        return [(self.ip_count,)]


class FakeClickhouseResource:
    def __init__(self, client: FakeQueryClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeQueryClient]:
        yield self.client


def test_commoncrawl_ip_source_uses_stable_bucket_partitions() -> None:
    spec = commoncrawl_ip_addresses_observation.get_asset_spec(
        COMMONCRAWL_IP_ADDRESSES_KEY
    )

    assert spec.partitions_def is COMMONCRAWL_IP_PARTITIONS
    assert "FROM corpscout.commoncrawl_ip_addresses FINAL" in (
        COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL
    )
    assert "WHERE bucket = %(bucket_index)s" in COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL
    assert "last_seen" not in COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL
    assert "commoncrawl_ip_geoip" not in COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL
    assert "rdap_" not in COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL


def test_commoncrawl_ip_membership_version_changes_only_with_count() -> None:
    empty = commoncrawl_ip_membership_data_version(0)
    original = commoncrawl_ip_membership_data_version(42)

    assert empty.value == "unique-ip-count-v1:0"
    assert original == commoncrawl_ip_membership_data_version(42)
    assert original != commoncrawl_ip_membership_data_version(43)


@pytest.mark.parametrize("ip_count", (True, 42.0, "42"))
def test_commoncrawl_ip_membership_version_rejects_non_integer_counts(
    ip_count: object,
) -> None:
    with pytest.raises(TypeError, match="must be an integer"):
        commoncrawl_ip_membership_data_version(ip_count)  # type: ignore[arg-type]


def test_commoncrawl_ip_membership_version_rejects_negative_count() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        commoncrawl_ip_membership_data_version(-1)


@pytest.mark.parametrize(
    "rows",
    (
        [],
        [(1,), (2,)],
        [(1, 2)],
    ),
)
def test_commoncrawl_ip_count_requires_one_aggregate_value(
    rows: list[tuple[int, ...]],
) -> None:
    with pytest.raises(RuntimeError, match="expected one row containing one count"):
        commoncrawl_ip_count_from_query(rows)


def test_manual_observation_job_records_one_bucket_without_materialization() -> None:
    client = FakeQueryClient(ip_count=42)
    repository = dg.Definitions.merge(
        observation_defs,
        dg.Definitions(
            resources={"clickhouse": FakeClickhouseResource(client)},
        ),
    ).get_repository_def()
    job = repository.get_job(commoncrawl_ip_addresses_observation_job.name)
    selection = commoncrawl_ip_addresses_observation_job.selection

    assert job.partitions_def is COMMONCRAWL_IP_PARTITIONS
    assert selection.resolve(repository.asset_graph) == {
        COMMONCRAWL_IP_ADDRESSES_KEY
    }
    assert selection.resolve_checks(repository.asset_graph) == set()

    result = job.execute_in_process(partition_key="bucket_007")

    assert result.success is True
    assert result.get_asset_materialization_events() == []
    observations = result.get_asset_observation_events()
    assert len(observations) == 1
    assert observations[0].asset_key == COMMONCRAWL_IP_ADDRESSES_KEY
    assert observations[0].partition == "bucket_007"
    observation = observations[0].event_specific_data.asset_observation
    assert observation.data_version == "unique-ip-count-v1:42"
    metadata = observation.metadata
    assert metadata["partition_key"].value == "bucket_007"
    assert metadata["bucket_index"].value == 7
    assert metadata["ip_count"].value == 42
    assert client.queries == [
        (COMMONCRAWL_IP_MEMBERSHIP_COUNT_SQL, {"bucket_index": 7})
    ]


def test_project_registers_one_manual_observable_source() -> None:
    from dagster_v3.definitions import defs as load_project_defs

    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    source = asset_graph.get(COMMONCRAWL_IP_ADDRESSES_KEY)

    assert source.is_external is True
    assert source.is_observable is True
    assert source.is_materializable is False
    assert source.partitions_def is COMMONCRAWL_IP_PARTITIONS
    assert source.backfill_policy == dg.BackfillPolicy.multi_run(
        max_partitions_per_run=1
    )
    assert source.automation_condition is None
    assert repository.get_job(commoncrawl_ip_addresses_observation_job.name)
    assert commoncrawl_ip_addresses_observation_job.selection.resolve(asset_graph) == {
        COMMONCRAWL_IP_ADDRESSES_KEY
    }
    assert (
        commoncrawl_ip_addresses_observation_job.selection.resolve_checks(asset_graph)
        == set()
    )
    assert observation_defs.sensors is None
    assert observation_defs.schedules is None

    for child in (commoncrawl_ip_geoip, commoncrawl_ip_rdap_networks):
        child_node = asset_graph.get(child.key)
        assert child_node.partitions_def is COMMONCRAWL_IP_PARTITIONS
        assert COMMONCRAWL_IP_ADDRESSES_KEY in child_node.parent_keys
        assert isinstance(
            asset_graph.get_partition_mapping(
                child.key,
                COMMONCRAWL_IP_ADDRESSES_KEY,
            ),
            dg.IdentityPartitionMapping,
        )

    assert all(
        commoncrawl_ip_addresses_observation_job.name not in sensor.name
        for sensor in repository.sensor_defs
    )
    assert all(
        commoncrawl_ip_addresses_observation_job.name not in schedule.name
        for schedule in repository.schedule_defs
    )


def test_changed_source_bucket_only_stales_matching_child_partition() -> None:
    tested_partitions = ("bucket_007", "bucket_193")
    versions = {
        "bucket_007": "unique-ip-count-v1:7",
        "bucket_193": "unique-ip-count-v1:193",
    }
    source_spec = dg.AssetSpec(
        "test_commoncrawl_ip_addresses",
        partitions_def=COMMONCRAWL_IP_PARTITIONS,
    )

    @dg.multi_observable_source_asset(specs=[source_spec])
    def test_commoncrawl_ip_source(
        context: dg.AssetExecutionContext,
    ) -> dg.ObserveResult:
        return dg.ObserveResult(
            asset_key=source_spec.key,
            data_version=dg.DataVersion(versions[context.partition_key]),
        )

    @dg.asset(
        name="test_commoncrawl_ip_child",
        deps=[source_spec.key],
        partitions_def=COMMONCRAWL_IP_PARTITIONS,
    )
    def test_commoncrawl_ip_child() -> dg.MaterializeResult:
        return dg.MaterializeResult()

    observe_job = dg.define_asset_job(
        "test_observe_commoncrawl_ip_source",
        selection=dg.AssetSelection.assets(source_spec.key),
    )
    materialize_job = dg.define_asset_job(
        "test_materialize_commoncrawl_ip_child",
        selection=dg.AssetSelection.assets(test_commoncrawl_ip_child),
    )
    test_defs = dg.Definitions(
        assets=[test_commoncrawl_ip_source, test_commoncrawl_ip_child],
        jobs=[observe_job, materialize_job],
    )
    observe = test_defs.resolve_job_def(observe_job.name)
    materialize = test_defs.resolve_job_def(materialize_job.name)

    with dg.DagsterInstance.ephemeral() as instance:
        for partition_key in tested_partitions:
            assert observe.execute_in_process(
                instance=instance,
                partition_key=partition_key,
            ).success
            assert materialize.execute_in_process(
                instance=instance,
                partition_key=partition_key,
            ).success

        assert observe.execute_in_process(
            instance=instance,
            partition_key="bucket_007",
        ).success
        unchanged = get_stale_status_resolver(
            instance,
            [test_commoncrawl_ip_source, test_commoncrawl_ip_child],
        )
        assert all(
            unchanged.get_status(test_commoncrawl_ip_child.key, partition_key)
            is StaleStatus.FRESH
            for partition_key in tested_partitions
        )

        versions["bucket_007"] = "unique-ip-count-v1:8"
        assert observe.execute_in_process(
            instance=instance,
            partition_key="bucket_007",
        ).success

        changed = get_stale_status_resolver(
            instance,
            [test_commoncrawl_ip_source, test_commoncrawl_ip_child],
        )
        assert (
            changed.get_status(test_commoncrawl_ip_child.key, "bucket_007")
            is StaleStatus.STALE
        )
        assert (
            changed.get_status(test_commoncrawl_ip_child.key, "bucket_193")
            is StaleStatus.FRESH
        )
