import dagster as dg

DFP_CLICKHOUSE_ASSET_NAMES = (
    "brazil_fin_cvm_dfp_documents_clickhouse",
    "brazil_fin_cvm_dfp_statement_rows_clickhouse",
    "brazil_fin_cvm_dfp_capital_composition_clickhouse",
    "brazil_fin_cvm_dfp_auditor_reports_clickhouse",
)

ITR_CLICKHOUSE_ASSET_NAMES = (
    "brazil_fin_cvm_itr_documents_clickhouse",
    "brazil_fin_cvm_itr_statement_rows_clickhouse",
    "brazil_fin_cvm_itr_capital_composition_clickhouse",
    "brazil_fin_cvm_itr_auditor_reports_clickhouse",
)


def test_brazil_fin_cvm_dfp_raw_archive_asset_has_expected_partitions() -> None:
    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_companies_clickhouse,
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_raw_csv_s3,
        brazil_fin_cvm_dfp_auditor_reports_clickhouse,
        brazil_fin_cvm_dfp_capital_composition_clickhouse,
        brazil_fin_cvm_dfp_documents_clickhouse,
        brazil_fin_cvm_dfp_raw_archives_s3,
        brazil_fin_cvm_dfp_raw_duckdb,
        brazil_fin_cvm_dfp_statement_rows_clickhouse,
        brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
        brazil_fin_cvm_itr_auditor_reports_clickhouse,
        brazil_fin_cvm_itr_capital_composition_clickhouse,
        brazil_fin_cvm_itr_documents_clickhouse,
        brazil_fin_cvm_itr_raw_archives_s3,
        brazil_fin_cvm_itr_raw_duckdb,
        brazil_fin_cvm_itr_statement_rows_clickhouse,
        brazil_fin_cvm_itr_statement_rows_usd_duckdb,
    )

    partitions_def = brazil_fin_cvm_dfp_raw_archives_s3.partitions_def
    itr_partitions_def = brazil_fin_cvm_itr_raw_archives_s3.partitions_def

    assert isinstance(partitions_def, dg.StaticPartitionsDefinition)
    assert partitions_def.get_partition_keys() == [
        str(year) for year in range(2010, 2027)
    ]
    assert isinstance(itr_partitions_def, dg.StaticPartitionsDefinition)
    assert itr_partitions_def.get_partition_keys() == [
        str(year) for year in range(2011, 2027)
    ]
    assert brazil_fin_cvm_dfp_raw_duckdb.partitions_def is partitions_def
    assert brazil_fin_cvm_dfp_statement_rows_usd_duckdb.partitions_def is partitions_def
    assert brazil_fin_cvm_dfp_documents_clickhouse.partitions_def is None
    assert brazil_fin_cvm_dfp_statement_rows_clickhouse.partitions_def is None
    assert brazil_fin_cvm_dfp_capital_composition_clickhouse.partitions_def is None
    assert brazil_fin_cvm_dfp_auditor_reports_clickhouse.partitions_def is None
    assert brazil_fin_cvm_itr_raw_duckdb.partitions_def is itr_partitions_def
    assert (
        brazil_fin_cvm_itr_statement_rows_usd_duckdb.partitions_def
        is itr_partitions_def
    )
    assert brazil_fin_cvm_itr_documents_clickhouse.partitions_def is None
    assert brazil_fin_cvm_itr_statement_rows_clickhouse.partitions_def is None
    assert brazil_fin_cvm_itr_capital_composition_clickhouse.partitions_def is None
    assert brazil_fin_cvm_itr_auditor_reports_clickhouse.partitions_def is None
    assert brazil_fin_cvm_dfp_raw_archives_s3.op.pool is None
    assert brazil_fin_cvm_dfp_raw_duckdb.op.pool is None
    assert brazil_fin_cvm_dfp_statement_rows_usd_duckdb.op.pool is None
    assert brazil_fin_cvm_dfp_documents_clickhouse.op.pool is None
    assert brazil_fin_cvm_dfp_statement_rows_clickhouse.op.pool is None
    assert brazil_fin_cvm_dfp_capital_composition_clickhouse.op.pool is None
    assert brazil_fin_cvm_dfp_auditor_reports_clickhouse.op.pool is None
    assert brazil_fin_cvm_itr_raw_archives_s3.op.pool is None
    assert brazil_fin_cvm_itr_raw_duckdb.op.pool is None
    assert brazil_fin_cvm_itr_statement_rows_usd_duckdb.op.pool is None
    assert brazil_fin_cvm_itr_documents_clickhouse.op.pool is None
    assert brazil_fin_cvm_itr_statement_rows_clickhouse.op.pool is None
    assert brazil_fin_cvm_itr_capital_composition_clickhouse.op.pool is None
    assert brazil_fin_cvm_itr_auditor_reports_clickhouse.op.pool is None
    assert brazil_fin_cvm_companies_duckdb.partitions_def is None
    assert brazil_fin_cvm_companies_clickhouse.partitions_def is None
    assert brazil_fin_cvm_companies_raw_csv_s3.partitions_def is None
    assert brazil_fin_cvm_companies_raw_csv_s3.op.pool is None
    assert brazil_fin_cvm_companies_duckdb.op.pool == "brazil_fin_cvm_duckdb"
    assert brazil_fin_cvm_companies_clickhouse.op.pool == "brazil_fin_cvm_duckdb"
    assert (
        brazil_fin_cvm_dfp_raw_archives_s3.group_names_by_key[
            dg.AssetKey("brazil_fin_cvm_dfp_raw_archives_s3")
        ]
        == "brazil_fin_cvm"
    )
    assert (
        brazil_fin_cvm_companies_duckdb.group_names_by_key[
            dg.AssetKey("brazil_fin_cvm_companies_duckdb")
        ]
        == "brazil_fin_cvm"
    )


def test_brazil_fin_cvm_dfp_raw_backfill_job_selects_raw_archive_asset() -> None:
    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_dfp_raw_archives_s3,
        brazil_fin_cvm_dfp_raw_backfill_job,
        brazil_fin_cvm_dfp_raw_duckdb,
        brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
        brazil_fin_cvm_itr_raw_archives_s3,
        brazil_fin_cvm_itr_raw_backfill_job,
        brazil_fin_cvm_itr_raw_duckdb,
    )
    from dagster_v3.defs.brazil_financial.cvm.source import (
        BrazilCvmDfpResource,
        BrazilCvmItrResource,
    )
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    resolved = dg.Definitions(
        assets=[
            brazil_fin_cvm_dfp_raw_archives_s3,
            brazil_fin_cvm_dfp_raw_duckdb,
            brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
        ],
        jobs=[brazil_fin_cvm_dfp_raw_backfill_job],
        resources={
            "brazil_fin_cvm_dfp": BrazilCvmDfpResource(),
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
            "clickhouse": ClickhouseResource(host="localhost"),
        },
    ).resolve_job_def("brazil_fin_cvm_dfp_raw_backfill_job")

    assert resolved.name == "brazil_fin_cvm_dfp_raw_backfill_job"

    itr_resolved = dg.Definitions(
        assets=[
            brazil_fin_cvm_itr_raw_archives_s3,
            brazil_fin_cvm_itr_raw_duckdb,
        ],
        jobs=[brazil_fin_cvm_itr_raw_backfill_job],
        resources={
            "brazil_fin_cvm_itr": BrazilCvmItrResource(),
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
        },
    ).resolve_job_def("brazil_fin_cvm_itr_raw_backfill_job")

    assert itr_resolved.name == "brazil_fin_cvm_itr_raw_backfill_job"


def test_brazil_fin_cvm_dfp_statement_rows_usd_depends_on_raw_duckdb_asset() -> None:
    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_dfp_raw_archives_s3,
        brazil_fin_cvm_dfp_raw_duckdb,
        brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
    )
    from dagster_v3.defs.brazil_financial.cvm.source import BrazilCvmDfpResource
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    repository = dg.Definitions(
        assets=[
            brazil_fin_cvm_dfp_raw_archives_s3,
            brazil_fin_cvm_dfp_raw_duckdb,
            brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
        ],
        resources={
            "brazil_fin_cvm_dfp": BrazilCvmDfpResource(),
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
        },
    ).get_repository_def()
    asset = repository.asset_graph.get(brazil_fin_cvm_dfp_statement_rows_usd_duckdb.key)

    assert asset.parent_keys == {dg.AssetKey("brazil_fin_cvm_dfp_raw_duckdb")}


def test_brazil_fin_cvm_dfp_clickhouse_table_assets_depend_on_usd_duckdb_asset() -> (
    None
):
    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_companies_clickhouse,
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_raw_csv_s3,
        brazil_fin_cvm_dfp_auditor_reports_clickhouse,
        brazil_fin_cvm_dfp_capital_composition_clickhouse,
        brazil_fin_cvm_dfp_documents_clickhouse,
        brazil_fin_cvm_dfp_raw_archives_s3,
        brazil_fin_cvm_dfp_raw_duckdb,
        brazil_fin_cvm_dfp_statement_rows_clickhouse,
        brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
    )
    from dagster_v3.defs.brazil_financial.cvm.source import BrazilCvmDfpResource
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    repository = dg.Definitions(
        assets=[
            brazil_fin_cvm_companies_raw_csv_s3,
            brazil_fin_cvm_companies_duckdb,
            brazil_fin_cvm_companies_clickhouse,
            brazil_fin_cvm_dfp_raw_archives_s3,
            brazil_fin_cvm_dfp_raw_duckdb,
            brazil_fin_cvm_dfp_statement_rows_usd_duckdb,
            brazil_fin_cvm_dfp_documents_clickhouse,
            brazil_fin_cvm_dfp_statement_rows_clickhouse,
            brazil_fin_cvm_dfp_capital_composition_clickhouse,
            brazil_fin_cvm_dfp_auditor_reports_clickhouse,
        ],
        resources={
            "brazil_fin_cvm_dfp": BrazilCvmDfpResource(),
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
            "clickhouse": ClickhouseResource(host="localhost"),
        },
    ).get_repository_def()
    for asset_name in DFP_CLICKHOUSE_ASSET_NAMES:
        asset = repository.asset_graph.get(dg.AssetKey(asset_name))
        assert asset.parent_keys == {
            dg.AssetKey("brazil_fin_cvm_dfp_statement_rows_usd_duckdb"),
            dg.AssetKey("brazil_fin_cvm_companies_clickhouse"),
        }


def test_brazil_fin_cvm_itr_clickhouse_table_assets_depend_on_usd_and_companies_assets() -> (
    None
):
    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_companies_clickhouse,
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_raw_csv_s3,
        brazil_fin_cvm_itr_auditor_reports_clickhouse,
        brazil_fin_cvm_itr_capital_composition_clickhouse,
        brazil_fin_cvm_itr_documents_clickhouse,
        brazil_fin_cvm_itr_raw_archives_s3,
        brazil_fin_cvm_itr_raw_duckdb,
        brazil_fin_cvm_itr_statement_rows_clickhouse,
        brazil_fin_cvm_itr_statement_rows_usd_duckdb,
    )
    from dagster_v3.defs.brazil_financial.cvm.source import BrazilCvmItrResource
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    repository = dg.Definitions(
        assets=[
            brazil_fin_cvm_companies_raw_csv_s3,
            brazil_fin_cvm_companies_duckdb,
            brazil_fin_cvm_companies_clickhouse,
            brazil_fin_cvm_itr_raw_archives_s3,
            brazil_fin_cvm_itr_raw_duckdb,
            brazil_fin_cvm_itr_statement_rows_usd_duckdb,
            brazil_fin_cvm_itr_documents_clickhouse,
            brazil_fin_cvm_itr_statement_rows_clickhouse,
            brazil_fin_cvm_itr_capital_composition_clickhouse,
            brazil_fin_cvm_itr_auditor_reports_clickhouse,
        ],
        resources={
            "brazil_fin_cvm_itr": BrazilCvmItrResource(),
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
            "clickhouse": ClickhouseResource(host="localhost"),
        },
    ).get_repository_def()
    for asset_name in ITR_CLICKHOUSE_ASSET_NAMES:
        asset = repository.asset_graph.get(dg.AssetKey(asset_name))
        assert asset.parent_keys == {
            dg.AssetKey("brazil_fin_cvm_itr_statement_rows_usd_duckdb"),
            dg.AssetKey("brazil_fin_cvm_companies_clickhouse"),
        }


def test_brazil_fin_cvm_companies_clickhouse_depends_on_duckdb_asset() -> None:
    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_companies_clickhouse,
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_raw_csv_s3,
    )
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    repository = dg.Definitions(
        assets=[
            brazil_fin_cvm_companies_raw_csv_s3,
            brazil_fin_cvm_companies_duckdb,
            brazil_fin_cvm_companies_clickhouse,
        ],
        resources={
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
            "clickhouse": ClickhouseResource(host="localhost"),
        },
    ).get_repository_def()
    asset = repository.asset_graph.get(brazil_fin_cvm_companies_clickhouse.key)

    assert asset.parent_keys == {dg.AssetKey("brazil_fin_cvm_companies_duckdb")}


def test_brazil_fin_cvm_companies_duckdb_depends_on_raw_csv_asset() -> None:
    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_companies_duckdb,
        brazil_fin_cvm_companies_raw_csv_s3,
    )
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    repository = dg.Definitions(
        assets=[
            brazil_fin_cvm_companies_raw_csv_s3,
            brazil_fin_cvm_companies_duckdb,
        ],
        resources={
            "brazil_fin_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
        },
    ).get_repository_def()
    asset = repository.asset_graph.get(brazil_fin_cvm_companies_duckdb.key)

    assert asset.parent_keys == {dg.AssetKey("brazil_fin_cvm_companies_raw_csv_s3")}


def test_brazil_fin_cvm_financial_metrics_are_clickhouse_views_not_assets() -> None:
    from dagster_v3.defs.brazil_financial.cvm import assets

    assert not hasattr(assets, "brazil_fin_cvm_financial_metrics_duckdb")
    assert not hasattr(assets, "brazil_fin_cvm_financial_metrics_clickhouse")


class FakeBrazilCvmDfpResource:
    def __init__(self) -> None:
        self.requested_years: list[str] = []

    def sync_year_archive(
        self,
        *,
        year: str,
        object_store: object,
        log_info: object | None = None,
    ) -> object:
        from dagster_v3.defs.brazil_financial.cvm.source import (
            BrazilCvmDfpArchiveSyncResult,
        )

        self.requested_years.append(year)
        return BrazilCvmDfpArchiveSyncResult(
            year=year,
            source_url=f"https://example.test/dfp_cia_aberta_{year}.zip",
            archive_key=f"brazil_cvm/dfp/raw_archives/year={year}/archive.zip",
            metadata_key=f"brazil_cvm/dfp/raw_archives/year={year}/metadata.json",
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=None,
            sha256=None,
            content_type="",
            source_last_modified="",
            synced_at="2026-07-04T00:00:00+00:00",
        )


def test_brazil_fin_cvm_dfp_raw_archive_asset_uses_partition_year() -> None:
    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_dfp_raw_archives_s3,
    )

    fake_resource = FakeBrazilCvmDfpResource()
    result = brazil_fin_cvm_dfp_raw_archives_s3(
        dg.build_asset_context(partition_key="2026"),
        brazil_fin_cvm_dfp=fake_resource,
        object_store=object(),
    )

    assert fake_resource.requested_years == ["2026"]
    assert result.metadata["year"] == "2026"
    assert result.metadata["reused_existing_archive"] is True
    assert result.metadata["downloaded"] is False


class FakeBrazilCvmItrResource:
    def __init__(self) -> None:
        self.requested_years: list[str] = []

    def sync_year_archive(
        self,
        *,
        year: str,
        object_store: object,
        log_info: object | None = None,
    ) -> object:
        from dagster_v3.defs.brazil_financial.cvm.source import (
            BrazilCvmItrArchiveSyncResult,
        )

        self.requested_years.append(year)
        return BrazilCvmItrArchiveSyncResult(
            year=year,
            source_url=f"https://example.test/itr_cia_aberta_{year}.zip",
            archive_key=f"brazil_cvm/itr/raw_archives/year={year}/archive.zip",
            metadata_key=f"brazil_cvm/itr/raw_archives/year={year}/metadata.json",
            downloaded=False,
            reused_existing_archive=True,
            size_bytes=None,
            sha256=None,
            content_type="",
            source_last_modified="",
            synced_at="2026-07-04T00:00:00+00:00",
        )


def test_brazil_fin_cvm_itr_raw_archive_asset_uses_partition_year() -> None:
    from dagster_v3.defs.brazil_financial.cvm.assets import (
        brazil_fin_cvm_itr_raw_archives_s3,
    )

    fake_resource = FakeBrazilCvmItrResource()
    result = brazil_fin_cvm_itr_raw_archives_s3(
        dg.build_asset_context(partition_key="2026"),
        brazil_fin_cvm_itr=fake_resource,
        object_store=object(),
    )

    assert fake_resource.requested_years == ["2026"]
    assert result.metadata["year"] == "2026"
    assert result.metadata["reused_existing_archive"] is True
    assert result.metadata["downloaded"] is False
