import dagster as dg


def test_brazil_cvm_dfp_raw_archive_asset_has_expected_partitions() -> None:
    from dagster_v3.defs.brazil_cvm.assets import (
        brazil_cvm_dfp_raw_archives_s3,
        brazil_cvm_dfp_raw_duckdb,
    )

    partitions_def = brazil_cvm_dfp_raw_archives_s3.partitions_def

    assert isinstance(partitions_def, dg.StaticPartitionsDefinition)
    assert partitions_def.get_partition_keys() == [
        str(year) for year in range(2010, 2027)
    ]
    assert brazil_cvm_dfp_raw_duckdb.partitions_def is partitions_def
    assert brazil_cvm_dfp_raw_archives_s3.op.pool is None
    assert brazil_cvm_dfp_raw_duckdb.op.pool == "brazil_cvm_duckdb"
    assert (
        brazil_cvm_dfp_raw_archives_s3.group_names_by_key[
            dg.AssetKey("brazil_cvm_dfp_raw_archives_s3")
        ]
        == "brazil_cvm"
    )


def test_brazil_cvm_dfp_raw_backfill_job_selects_raw_archive_asset() -> None:
    from dagster_v3.defs.brazil_cvm.assets import (
        brazil_cvm_dfp_raw_archives_s3,
        brazil_cvm_dfp_raw_backfill_job,
        brazil_cvm_dfp_raw_duckdb,
    )
    from dagster_v3.defs.brazil_cvm.source import BrazilCvmDfpResource
    from dagster_v3.defs.common.duckdb_resources import duckdb_resource
    from dagster_v3.defs.common.resources import ObjectStoreResource

    resolved = dg.Definitions(
        assets=[brazil_cvm_dfp_raw_archives_s3, brazil_cvm_dfp_raw_duckdb],
        jobs=[brazil_cvm_dfp_raw_backfill_job],
        resources={
            "brazil_cvm_dfp": BrazilCvmDfpResource(),
            "brazil_cvm_duckdb": duckdb_resource(":memory:"),
            "object_store": ObjectStoreResource(),
        },
    ).resolve_job_def("brazil_cvm_dfp_raw_backfill_job")

    assert resolved.name == "brazil_cvm_dfp_raw_backfill_job"


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
        from dagster_v3.defs.brazil_cvm.source import BrazilCvmDfpArchiveSyncResult

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


def test_brazil_cvm_dfp_raw_archive_asset_uses_partition_year() -> None:
    from dagster_v3.defs.brazil_cvm.assets import brazil_cvm_dfp_raw_archives_s3

    fake_resource = FakeBrazilCvmDfpResource()
    result = brazil_cvm_dfp_raw_archives_s3(
        dg.build_asset_context(partition_key="2026"),
        brazil_cvm_dfp=fake_resource,
        object_store=object(),
    )

    assert fake_resource.requested_years == ["2026"]
    assert result.metadata["year"] == "2026"
    assert result.metadata["reused_existing_archive"] is True
    assert result.metadata["downloaded"] is False
