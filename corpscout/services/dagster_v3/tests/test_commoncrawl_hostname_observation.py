from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta, timezone

import dagster as dg
import pytest
from dagster._core.definitions.data_version import StaleStatus
from dagster._utils.test.data_versions import get_stale_status_resolver

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    COMMONCRAWL_DOMAINS_KEY,
    COMMONCRAWL_HOSTNAME_SOURCE_KEYS,
    CTLOGS_HOSTNAMES_KEY,
    DNS_RECORD_OBSERVATIONS_KEY,
    DOMAIN_HOSTNAMES_KEY,
    EPOCH,
    HOSTNAME_SHARD_COUNT,
    HOSTNAME_SOURCE_WATERMARKS_SQL,
    domain_hostnames,
)
from dagster_v3.defs.commoncrawl_hostnames.definitions import defs as hostname_defs
from dagster_v3.defs.commoncrawl_hostnames.observation import (
    COMMONCRAWL_HOSTNAME_SOURCE_VERSION_SCHEME,
    commoncrawl_hostname_sources_observation,
    commoncrawl_hostname_sources_observation_job,
    hostname_source_data_version,
    hostname_source_watermarks_from_query,
    normalize_hostname_source_watermark,
)


class FakeQueryClient:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.queries: list[tuple[str, dict[str, int]]] = []

    def execute(
        self,
        query: str,
        parameters: dict[str, int],
    ) -> list[tuple[object, ...]]:
        self.queries.append((query, parameters))
        return self.rows


class FakeClickhouseResource:
    def __init__(self, client: FakeQueryClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self) -> Iterator[FakeQueryClient]:
        yield self.client


def test_hostname_sources_and_view_use_stable_shard_partitions() -> None:
    assert commoncrawl_hostname_sources_observation.can_subset is True

    for source_key in COMMONCRAWL_HOSTNAME_SOURCE_KEYS:
        spec = commoncrawl_hostname_sources_observation.get_asset_spec(source_key)
        assert spec.partitions_def is COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS

    assert domain_hostnames.partitions_def is (
        COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS
    )
    assert "max(last_ingested_at)" in HOSTNAME_SOURCE_WATERMARKS_SQL
    assert "max(resolved_at)" in HOSTNAME_SOURCE_WATERMARKS_SQL
    assert "max(loaded_at)" in HOSTNAME_SOURCE_WATERMARKS_SQL
    assert "record_type IN ('A', 'AAAA', 'CNAME')" in (
        HOSTNAME_SOURCE_WATERMARKS_SQL
    )
    assert "source = 'axfr'" not in HOSTNAME_SOURCE_WATERMARKS_SQL


def test_hostname_source_watermarks_are_normalized_and_merge_stable() -> None:
    naive = datetime(2026, 7, 14, 8, 30)
    offset = datetime(
        2026,
        7,
        14,
        10,
        30,
        tzinfo=timezone(timedelta(hours=2)),
    )

    assert (
        normalize_hostname_source_watermark(
            None,
            watermark_column="loaded_at",
        )
        == EPOCH
    )
    assert normalize_hostname_source_watermark(
        naive,
        watermark_column="resolved_at",
    ) == datetime(2026, 7, 14, 8, 30, tzinfo=UTC)
    assert normalize_hostname_source_watermark(
        offset,
        watermark_column="last_ingested_at",
    ) == datetime(2026, 7, 14, 8, 30, tzinfo=UTC)

    original = hostname_source_data_version("resolved_at", naive)
    equivalent = hostname_source_data_version("resolved_at", offset)
    changed = hostname_source_data_version(
        "resolved_at",
        datetime(2026, 7, 14, 8, 31, tzinfo=UTC),
    )

    assert original.value == (
        f"{COMMONCRAWL_HOSTNAME_SOURCE_VERSION_SCHEME}:"
        "resolved_at:2026-07-14T08:30:00.000+00:00"
    )
    assert original == equivalent
    assert original != changed


@pytest.mark.parametrize(
    "rows",
    (
        [],
        [(None, None)],
        [(None, None, None), (None, None, None)],
    ),
)
def test_hostname_source_query_requires_exactly_three_watermarks(
    rows: list[tuple[object, ...]],
) -> None:
    with pytest.raises(RuntimeError, match="one row containing three watermarks"):
        hostname_source_watermarks_from_query(rows)


def test_hostname_source_query_rejects_non_datetime_watermarks() -> None:
    with pytest.raises(TypeError, match="non-datetime resolved_at watermark"):
        hostname_source_watermarks_from_query([(None, "not-a-date", None)])


def test_manual_observation_job_records_three_sources_without_materialization() -> None:
    ct_watermark = datetime(2026, 7, 14, 8, 0, tzinfo=UTC)
    domains_watermark = datetime(2026, 7, 14, 9, 0, tzinfo=UTC)
    dns_watermark = datetime(2026, 7, 14, 10, 0, tzinfo=UTC)
    client = FakeQueryClient([(ct_watermark, domains_watermark, dns_watermark)])
    repository = dg.Definitions.merge(
        hostname_defs,
        dg.Definitions(
            resources={"clickhouse": FakeClickhouseResource(client)},
        ),
    ).get_repository_def()
    job = repository.get_job(commoncrawl_hostname_sources_observation_job.name)
    selection = commoncrawl_hostname_sources_observation_job.selection

    assert job.partitions_def is COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS
    assert selection.resolve(repository.asset_graph) == set(
        COMMONCRAWL_HOSTNAME_SOURCE_KEYS
    )
    assert selection.resolve_checks(repository.asset_graph) == set()

    result = job.execute_in_process(partition_key="shard_07")

    assert result.success is True
    assert result.get_asset_materialization_events() == []
    observations = {
        event.asset_key: event.event_specific_data.asset_observation
        for event in result.get_asset_observation_events()
    }
    assert set(observations) == set(COMMONCRAWL_HOSTNAME_SOURCE_KEYS)
    assert all(
        observation.partition == "shard_07" for observation in observations.values()
    )
    assert observations[CTLOGS_HOSTNAMES_KEY].data_version == (
        "source-watermark-v1:last_ingested_at:2026-07-14T08:00:00.000+00:00"
    )
    assert observations[COMMONCRAWL_DOMAINS_KEY].data_version == (
        "source-watermark-v1:resolved_at:2026-07-14T09:00:00.000+00:00"
    )
    assert observations[DNS_RECORD_OBSERVATIONS_KEY].data_version == (
        "source-watermark-v1:loaded_at:2026-07-14T10:00:00.000+00:00"
    )
    dns_metadata = observations[DNS_RECORD_OBSERVATIONS_KEY].metadata
    assert dns_metadata["source_table"].value == (
        "corpscout.commoncrawl_domain_dns_record_observations"
    )
    assert dns_metadata["observation_scope"].value == (
        "A, AAAA, and CNAME observations in the shard"
    )
    assert dns_metadata["shard_index"].value == 7
    assert client.queries == [
        (
            HOSTNAME_SOURCE_WATERMARKS_SQL,
            {"shard_count": HOSTNAME_SHARD_COUNT, "shard_index": 7},
        )
    ]


def test_multi_observer_emits_only_selected_source_keys() -> None:
    client = FakeQueryClient(
        [
            (
                datetime(2026, 7, 14, 8, 0, tzinfo=UTC),
                datetime(2026, 7, 14, 9, 0, tzinfo=UTC),
                datetime(2026, 7, 14, 10, 0, tzinfo=UTC),
            )
        ]
    )
    subset_job = dg.define_asset_job(
        "test_observe_only_commoncrawl_domains",
        selection=dg.AssetSelection.assets(COMMONCRAWL_DOMAINS_KEY).without_checks(),
    )
    defs = dg.Definitions(
        assets=[commoncrawl_hostname_sources_observation],
        jobs=[subset_job],
        resources={"clickhouse": FakeClickhouseResource(client)},
    )

    result = defs.resolve_job_def(subset_job.name).execute_in_process(
        partition_key="shard_03"
    )

    observations = result.get_asset_observation_events()
    assert result.success is True
    assert len(observations) == 1
    assert observations[0].asset_key == COMMONCRAWL_DOMAINS_KEY
    assert client.queries == [
        (
            HOSTNAME_SOURCE_WATERMARKS_SQL,
            {"shard_count": HOSTNAME_SHARD_COUNT, "shard_index": 3},
        )
    ]


def test_project_registers_manual_hostname_observation_without_automation() -> None:
    from dagster_v3.definitions import defs as load_project_defs

    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    view_node = asset_graph.get(DOMAIN_HOSTNAMES_KEY)

    assert repository.get_job(commoncrawl_hostname_sources_observation_job.name)
    assert commoncrawl_hostname_sources_observation_job.selection.resolve(
        asset_graph
    ) == set(COMMONCRAWL_HOSTNAME_SOURCE_KEYS)
    assert (
        commoncrawl_hostname_sources_observation_job.selection.resolve_checks(
            asset_graph
        )
        == set()
    )
    assert hostname_defs.sensors is None
    assert hostname_defs.schedules is None

    for source_key in COMMONCRAWL_HOSTNAME_SOURCE_KEYS:
        source = asset_graph.get(source_key)
        assert source.is_external is True
        assert source.is_observable is True
        assert source.is_materializable is False
        assert source.partitions_def is COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS
        assert source.backfill_policy == dg.BackfillPolicy.multi_run(
            max_partitions_per_run=1
        )
        assert source.automation_condition is None

    assert view_node.parent_keys == {DNS_RECORD_OBSERVATIONS_KEY}
    assert view_node.is_external is True
    assert view_node.is_observable is False
    assert view_node.is_materializable is False
    assert isinstance(
        asset_graph.get_partition_mapping(
            DOMAIN_HOSTNAMES_KEY,
            DNS_RECORD_OBSERVATIONS_KEY,
        ),
        dg.IdentityPartitionMapping,
    )

    assert all(
        commoncrawl_hostname_sources_observation_job.name not in sensor.name
        for sensor in repository.sensor_defs
    )
    assert all(
        commoncrawl_hostname_sources_observation_job.name not in schedule.name
        for schedule in repository.schedule_defs
    )


def test_changed_source_shard_only_stales_matching_child_partition() -> None:
    tested_partitions = ("shard_03", "shard_11")
    source_specs = [
        dg.AssetSpec(
            key=f"test_hostname_source_{index}",
            partitions_def=COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
        )
        for index in range(3)
    ]
    versions = {
        (spec.key, partition_key): "version-1"
        for spec in source_specs
        for partition_key in tested_partitions
    }

    @dg.multi_observable_source_asset(
        specs=source_specs,
        can_subset=True,
    )
    def test_hostname_sources(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.ObserveResult]:
        for spec in source_specs:
            if spec.key not in context.selected_asset_keys:
                continue
            yield dg.ObserveResult(
                asset_key=spec.key,
                data_version=dg.DataVersion(
                    versions[(spec.key, context.partition_key)]
                ),
            )

    @dg.asset(
        name="test_hostname_child",
        deps=[spec.key for spec in source_specs],
        partitions_def=COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    )
    def test_hostname_child() -> dg.MaterializeResult:
        return dg.MaterializeResult()

    observe_all_job = dg.define_asset_job(
        "test_observe_all_hostname_sources",
        selection=dg.AssetSelection.assets(*[spec.key for spec in source_specs]),
    )
    observe_first_job = dg.define_asset_job(
        "test_observe_first_hostname_source",
        selection=dg.AssetSelection.assets(source_specs[0].key),
    )
    materialize_job = dg.define_asset_job(
        "test_materialize_hostname_child",
        selection=dg.AssetSelection.assets(test_hostname_child),
    )
    test_defs = dg.Definitions(
        assets=[test_hostname_sources, test_hostname_child],
        jobs=[observe_all_job, observe_first_job, materialize_job],
    )
    observe_all = test_defs.resolve_job_def(observe_all_job.name)
    observe_first = test_defs.resolve_job_def(observe_first_job.name)
    materialize = test_defs.resolve_job_def(materialize_job.name)

    with dg.DagsterInstance.ephemeral() as instance:
        for partition_key in tested_partitions:
            assert observe_all.execute_in_process(
                instance=instance,
                partition_key=partition_key,
            ).success
            assert materialize.execute_in_process(
                instance=instance,
                partition_key=partition_key,
            ).success

        versions[(source_specs[0].key, "shard_03")] = "version-2"
        assert observe_first.execute_in_process(
            instance=instance,
            partition_key="shard_03",
        ).success

        resolver = get_stale_status_resolver(
            instance,
            [test_hostname_sources, test_hostname_child],
        )
        assert (
            resolver.get_status(test_hostname_child.key, "shard_03")
            is StaleStatus.STALE
        )
        assert (
            resolver.get_status(test_hostname_child.key, "shard_11")
            is StaleStatus.FRESH
        )
