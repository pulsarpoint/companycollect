import dagster as dg
import duckdb
import pytest


RAW_STAGE_ASSET_KEYS = {
    "brazil_comp_rfb_empresas_duckdb",
    "brazil_comp_rfb_estabelecimentos_duckdb",
    "brazil_comp_rfb_simples_duckdb",
    "brazil_comp_rfb_reference_duckdb",
}


class _FakeLog:
    def info(self, *_args, **_kwargs) -> None:
        pass


class _FakeContext:
    run_id = "run-1"
    partition_key = "2026-04-01"
    log = _FakeLog()


def _create_brazil_schema(database_path) -> None:
    from dagster_v3.defs.brazil_companies.rfb import tables

    database_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {tables.DLT_DATASET_NAME}")


def test_brazil_comp_rfb_assets_are_registered_with_stage_specific_pools() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    keys = {key.path[-1] for key in repo.asset_graph.get_all_asset_keys()}

    assert "brazil_comp_rfb_raw_archives_s3" in keys
    assert "brazil_comp_rfb_snapshot_files_duckdb" in keys
    assert "brazil_comp_rfb_raw_files_duckdb" not in keys
    assert RAW_STAGE_ASSET_KEYS.issubset(keys)
    assert "brazil_comp_rfb_socios_duckdb" in keys
    assert "brazil_comp_rfb_company_relations_duckdb" in keys
    assert "brazil_comp_rfb_companies_duckdb" in keys
    assert "brazil_comp_rfb_contact_info_duckdb" in keys
    assert "brazil_comp_rfb_websites_duckdb" in keys
    assert "brazil_comp_rfb_clickhouse_companies" in keys
    assert "brazil_comp_rfb_clickhouse_establishments" in keys
    assert "brazil_comp_rfb_clickhouse_company_contacts" in keys
    assert "brazil_comp_rfb_clickhouse_company_domains" in keys
    assert "brazil_comp_rfb_clickhouse_websites" in keys
    assert "brazil_comp_rfb_company_relations_clickhouse" in keys
    assert "brazil_comp_rfb_previous_partition_cleanup" in keys

    raw_archives_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_raw_archives_s3")
    ]
    snapshot_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_snapshot_files_duckdb")
    ]
    empresas_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_empresas_duckdb")
    ]
    estabelecimentos_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_estabelecimentos_duckdb")
    ]
    simples_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_simples_duckdb")
    ]
    socios_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_socios_duckdb")
    ]
    company_relations_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_company_relations_duckdb")
    ]
    reference_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_reference_duckdb")
    ]
    companies_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_companies_duckdb")
    ]
    contact_info_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_contact_info_duckdb")
    ]
    websites_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_websites_duckdb")
    ]
    clickhouse_companies_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_clickhouse_companies")
    ]
    clickhouse_establishments_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_clickhouse_establishments")
    ]
    clickhouse_company_contacts_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_clickhouse_company_contacts")
    ]
    clickhouse_company_domains_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_clickhouse_company_domains")
    ]
    clickhouse_websites_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_clickhouse_websites")
    ]
    clickhouse_company_relations_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_company_relations_clickhouse")
    ]
    cleanup_asset = repo.assets_defs_by_key[
        dg.AssetKey("brazil_comp_rfb_previous_partition_cleanup")
    ]
    # No DuckDB file is opened here, only object storage — unlike every other
    # asset in this module, it needs no pool.
    assert raw_archives_asset.op.pool is None
    assert snapshot_asset.op.pool == "brazil_comp_rfb_manifest_duckdb"
    assert empresas_asset.op.pool == "brazil_comp_rfb_empresas_duckdb"
    assert estabelecimentos_asset.op.pool == "brazil_comp_rfb_estabelecimentos_duckdb"
    assert simples_asset.op.pool == "brazil_comp_rfb_simples_duckdb"
    assert socios_asset.op.pool == "brazil_comp_rfb_socios_duckdb"
    assert company_relations_asset.op.pool == "brazil_comp_rfb_relations_duckdb"
    assert reference_asset.op.pool == "brazil_comp_rfb_reference_duckdb"
    assert companies_asset.op.pool == "brazil_comp_rfb_companies_duckdb"
    assert contact_info_asset.op.pool == "brazil_comp_rfb_contact_info_duckdb"
    assert websites_asset.op.pool == "brazil_comp_rfb_websites_duckdb"
    # Read-only exporters carry the pool of the stage file they read: a
    # DuckDB writer excludes readers across processes, so unpooled reads
    # collide with a concurrent stage rebuild (see data-source-guidelines).
    assert clickhouse_companies_asset.op.pool == "brazil_comp_rfb_companies_duckdb"
    assert (
        clickhouse_establishments_asset.op.pool == "brazil_comp_rfb_companies_duckdb"
    )
    assert (
        clickhouse_company_contacts_asset.op.pool
        == "brazil_comp_rfb_contact_info_duckdb"
    )
    assert (
        clickhouse_company_domains_asset.op.pool == "brazil_comp_rfb_websites_duckdb"
    )
    assert clickhouse_websites_asset.op.pool == "brazil_comp_rfb_websites_duckdb"
    assert (
        clickhouse_company_relations_asset.op.pool == "brazil_comp_rfb_relations_duckdb"
    )
    assert cleanup_asset.op.pool is None


def test_brazil_comp_rfb_company_relations_clickhouse_never_creates_relations_db(
    tmp_path, monkeypatch
) -> None:
    """M1: the relations export must use read_only_duckdb_connection like
    every sibling ClickHouse-export asset, not a plain
    duckdb_resource(...).get_connection(). A leaked read-write handle holds
    an exclusive lock that blocks retries, and read-write also silently
    *creates* relations.duckdb if the build never ran -- read-only refuses
    to open (let alone create) a file that doesn't exist."""
    from dagster_v3.defs.brazil_companies.rfb import assets

    monkeypatch.setattr(assets, "BRAZIL_COMP_RFB_DATA_ROOT", tmp_path / "brazil_rfb")
    stage_paths = assets.brazil_comp_rfb_stage_paths("2026-04")
    stage_paths.ensure_root()
    assert not stage_paths.relations.exists()

    class FakeClickhouse:
        pass

    with pytest.raises(Exception):
        assets.brazil_comp_rfb_company_relations_clickhouse.node_def.compute_fn.decorated_fn(
            _FakeContext(), FakeClickhouse()
        )

    assert not stage_paths.relations.exists()


def test_brazil_comp_rfb_assets_use_monthly_snapshot_partitions() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    expected_snapshot_assets = (
        "brazil_comp_rfb_raw_archives_s3",
        "brazil_comp_rfb_snapshot_files_duckdb",
        "brazil_comp_rfb_empresas_duckdb",
        "brazil_comp_rfb_estabelecimentos_duckdb",
        "brazil_comp_rfb_simples_duckdb",
        "brazil_comp_rfb_reference_duckdb",
        "brazil_comp_rfb_companies_duckdb",
        "brazil_comp_rfb_contact_info_duckdb",
        "brazil_comp_rfb_websites_duckdb",
        "brazil_comp_rfb_clickhouse_companies",
        "brazil_comp_rfb_clickhouse_establishments",
        "brazil_comp_rfb_clickhouse_company_contacts",
        "brazil_comp_rfb_clickhouse_company_domains",
        "brazil_comp_rfb_clickhouse_websites",
        "brazil_comp_rfb_company_relations_clickhouse",
        "brazil_comp_rfb_previous_partition_cleanup",
    )

    for asset_name in expected_snapshot_assets:
        node = repo.asset_graph.get(dg.AssetKey(asset_name))
        assert type(node.partitions_def).__name__ == "MonthlyPartitionsDefinition"
        assert node.partitions_def.get_first_partition_key() == "2026-04-01"


def test_brazil_comp_rfb_snapshot_config_uses_partition_for_snapshot_year_month() -> (
    None
):
    from dagster_v3.defs.brazil_companies.rfb.assets import BrazilCompRfbConfig

    fields = BrazilCompRfbConfig.model_fields

    assert "snapshot_year_month" not in fields
    assert "snapshot_month" not in fields
    assert fields["snapshot_base_url"].description is not None
    assert "dados-abertos-rf-cnpj.casadosdados.com.br" in (
        fields["snapshot_base_url"].description or ""
    )
    assert "YYYY-MM-DD" in (fields["snapshot_base_url"].description or "")
    assert "partition key controls the YYYY-MM snapshot" in (
        fields["snapshot_base_url"].description or ""
    )


def test_brazil_comp_rfb_stage_paths_are_snapshot_scoped() -> None:
    from dagster_v3.defs.brazil_companies.rfb import assets

    paths = assets.brazil_comp_rfb_stage_paths("2026-05")
    snapshot_root = assets.BRAZIL_COMP_RFB_DATA_ROOT / "2026-05"

    assert paths.manifest == snapshot_root / "manifest.duckdb"
    assert paths.empresas == snapshot_root / "empresas.duckdb"
    assert paths.estabelecimentos == snapshot_root / "estabelecimentos.duckdb"
    assert paths.simples == snapshot_root / "simples.duckdb"
    assert paths.reference == snapshot_root / "reference.duckdb"
    assert paths.companies == snapshot_root / "companies.duckdb"
    assert paths.contact_info == snapshot_root / "contact_info.duckdb"
    assert paths.websites == snapshot_root / "websites.duckdb"


def test_brazil_comp_rfb_raw_archives_s3_asset_uses_config_snapshot_base_url(
    monkeypatch,
) -> None:
    """I5: the manifest asset reads config.snapshot_base_url as its
    mirror-failover hatch, but the S3-sync asset previously took no config at
    all and always used DEFAULT_BASE_URL. Since the two mirrors use different
    archive naming (EMPRECSV.zip vs Empresas0.zip), overriding the mirror
    would upload under one scheme and the parse request the other."""
    from dagster_v3.defs.brazil_companies.rfb import assets

    calls = {}

    class FakeContext:
        run_id = "run-1"
        partition_key = "2024-02-01"

        class log:
            @staticmethod
            def info(*_args, **_kwargs) -> None:
                pass

    def fake_sync(**kwargs):
        calls["sync"] = kwargs

        class FakeResult:
            @staticmethod
            def metadata():
                return {"archive_count": 0}

        return FakeResult()

    monkeypatch.setattr(
        assets.source, "sync_snapshot_archives_to_object_store", fake_sync
    )

    fake_object_store = object()
    assets.brazil_comp_rfb_raw_archives_s3.node_def.compute_fn.decorated_fn(
        FakeContext(),
        assets.BrazilCompRfbConfig(snapshot_base_url="https://mirror.test/cnpj/"),
        fake_object_store,
    )

    assert calls["sync"]["snapshot_year_month"] == "2024-02"
    assert calls["sync"]["base_url"] == "https://mirror.test/cnpj/"
    assert calls["sync"]["object_store"] is fake_object_store


def test_brazil_comp_rfb_snapshot_asset_uses_partition_snapshot_year_month(
    monkeypatch,
) -> None:
    from dagster_v3.defs.brazil_companies.rfb import assets

    calls = {}

    class FakeContext:
        run_id = "run-1"
        partition_key = "2024-02-01"

    class FakeDlt:
        def run(self, **kwargs):
            calls["run"] = kwargs
            yield "materialization"

    def fake_source(**kwargs):
        calls["source"] = kwargs
        return "dlt-source"

    def fake_pipeline(database_path):
        calls["pipeline_database_path"] = database_path
        return "dlt-pipeline"

    monkeypatch.setattr(assets.source, "brazil_rfb_source", fake_source)
    monkeypatch.setattr(assets.source, "brazil_rfb_pipeline", fake_pipeline)

    fake_object_store = object()
    result = list(
        assets.brazil_comp_rfb_snapshot_files_duckdb.node_def.compute_fn.decorated_fn(
            FakeContext(),
            assets.BrazilCompRfbConfig(
                snapshot_base_url="https://mirror.test/cnpj/",
            ),
            FakeDlt(),
            fake_object_store,
        )
    )

    assert result == ["materialization"]
    assert calls["source"]["source_run_id"] == "run-1"
    assert calls["source"]["snapshot_year_month"] == "2024-02"
    assert calls["source"]["snapshot_base_url"] == "https://mirror.test/cnpj/"
    assert (
        calls["source"]["download_dir"]
        == assets.BRAZIL_COMP_RFB_DOWNLOAD_DIR / "2024-02"
    )
    assert calls["source"]["object_store"] is fake_object_store
    assert calls["pipeline_database_path"] == (
        assets.BRAZIL_COMP_RFB_DATA_ROOT / "2024-02" / "manifest.duckdb"
    )
    assert calls["run"]["dlt_source"] == "dlt-source"
    assert calls["run"]["dlt_pipeline"] == "dlt-pipeline"


def test_brazil_comp_rfb_snapshot_asset_reuses_existing_manifest(
    tmp_path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.brazil_companies.rfb import assets, source, tables

    monkeypatch.setattr(assets, "BRAZIL_COMP_RFB_DATA_ROOT", tmp_path / "brazil_rfb")
    stage_paths = assets.brazil_comp_rfb_stage_paths("2026-04")
    stage_paths.ensure_root()
    csv_paths = {}
    for family in source.DEFAULT_FAMILIES:
        csv_path = tmp_path / "downloads" / family / f"{family}.csv"
        csv_path.parent.mkdir(parents=True)
        csv_path.write_text("row\n")
        csv_paths[family] = csv_path
    _create_brazil_schema(stage_paths.manifest)
    with duckdb.connect(str(stage_paths.manifest)) as connection:
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE} (
                family varchar,
                archive_url varchar,
                archive_name varchar,
                archive_sha256 varchar,
                csv_member_name varchar,
                csv_path varchar,
                source_run_id varchar,
                retrieved_at timestamp
            )
            """
        )
        for family, csv_path in csv_paths.items():
            connection.execute(
                f"""
                insert into {tables.DLT_DATASET_NAME}.{tables.SNAPSHOT_FILES_TABLE}
                values (?, ?, ?, 'hash', ?, ?, 'old-run', now())
                """,
                [
                    family,
                    f"https://example.test/{family}.zip",
                    f"{family}.zip",
                    f"{family}.csv",
                    str(csv_path),
                ],
            )

    calls = {}

    class FakeDlt:
        def run(self, **kwargs):
            calls["run"] = kwargs
            yield "materialization"

    def fake_source(**kwargs):
        calls["source"] = kwargs
        return "dlt-source"

    def fake_pipeline(database_path):
        calls["pipeline_database_path"] = database_path
        return "dlt-pipeline"

    monkeypatch.setattr(assets.source, "brazil_rfb_source", fake_source)
    monkeypatch.setattr(assets.source, "brazil_rfb_pipeline", fake_pipeline)

    result = list(
        assets.brazil_comp_rfb_snapshot_files_duckdb.node_def.compute_fn.decorated_fn(
            _FakeContext(),
            assets.BrazilCompRfbConfig(
                snapshot_base_url="https://mirror.test/cnpj/",
            ),
            FakeDlt(),
            object(),
        )
    )

    assert result == ["materialization"]
    assert "manifest_rows" in calls["source"]
    assert calls["source"]["source_run_id"] == "run-1"
    assert {row["source_run_id"] for row in calls["source"]["manifest_rows"]} == {
        "run-1"
    }
    assert "snapshot_year_month" not in calls["source"]
    assert calls["pipeline_database_path"] == stage_paths.manifest


def test_brazil_comp_rfb_empresas_asset_reuses_existing_stage(
    tmp_path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.brazil_companies.rfb import assets, tables

    monkeypatch.setattr(assets, "BRAZIL_COMP_RFB_DATA_ROOT", tmp_path / "brazil_rfb")
    stage_paths = assets.brazil_comp_rfb_stage_paths("2026-04")
    _create_brazil_schema(stage_paths.empresas)
    with duckdb.connect(str(stage_paths.empresas)) as connection:
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.RAW_TABLE_BY_FAMILY["empresas"]} as
            select '12345678' as cnpj_basico
            """
        )

    def fail_load(*_args, **_kwargs):
        raise AssertionError("existing raw stage should not be rebuilt")

    monkeypatch.setattr(assets.staging, "load_raw_family_from_manifest", fail_load)

    result = assets.brazil_comp_rfb_empresas_duckdb.node_def.compute_fn.decorated_fn(
        _FakeContext()
    )

    assert result.metadata == {"empresas": 1, "reused_existing_stage": True}


def test_brazil_comp_rfb_companies_asset_reuses_existing_stage(
    tmp_path,
    monkeypatch,
) -> None:
    from dagster_v3.defs.brazil_companies.rfb import assets, tables

    monkeypatch.setattr(assets, "BRAZIL_COMP_RFB_DATA_ROOT", tmp_path / "brazil_rfb")
    stage_paths = assets.brazil_comp_rfb_stage_paths("2026-04")
    _create_brazil_schema(stage_paths.companies)
    with duckdb.connect(str(stage_paths.companies)) as connection:
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE} as
            select '12345678' as cnpj_basico, 1 as is_active
            union all
            select '99999999' as cnpj_basico, 0 as is_active
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.{tables.ESTABLISHMENTS_TABLE} as
            select '12345678000190' as cnpj
            """
        )

    def fail_build(*_args, **_kwargs):
        raise AssertionError("existing companies stage should not be rebuilt")

    monkeypatch.setattr(
        assets.transforms,
        "build_brazil_rfb_companies_and_establishments",
        fail_build,
    )

    result = assets.brazil_comp_rfb_companies_duckdb.node_def.compute_fn.decorated_fn(
        _FakeContext()
    )

    assert result.metadata == {
        "companies": 2,
        "establishments": 1,
        "active_companies": 1,
        "reused_existing_stage": True,
    }


def test_brazil_comp_rfb_snapshot_manifest_depends_on_raw_archives_s3() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    parents = {
        parent.path[-1]
        for parent in repo.asset_graph.get(
            dg.AssetKey("brazil_comp_rfb_snapshot_files_duckdb")
        ).parent_keys
    }

    assert parents == {"brazil_comp_rfb_raw_archives_s3"}


def test_brazil_comp_rfb_raw_assets_depend_on_snapshot_manifest_only() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    for asset_name in RAW_STAGE_ASSET_KEYS:
        parents = {
            parent.path[-1]
            for parent in repo.asset_graph.get(dg.AssetKey(asset_name)).parent_keys
        }

        assert parents == {"brazil_comp_rfb_snapshot_files_duckdb"}


def test_brazil_comp_rfb_companies_asset_depends_on_raw_files() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    parents = {
        parent.path[-1]
        for parent in repo.asset_graph.get(
            dg.AssetKey("brazil_comp_rfb_companies_duckdb")
        ).parent_keys
    }

    assert parents == RAW_STAGE_ASSET_KEYS


def test_brazil_comp_rfb_clickhouse_assets_depend_on_normalized_companies() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    for asset_name in (
        "brazil_comp_rfb_clickhouse_companies",
        "brazil_comp_rfb_clickhouse_establishments",
    ):
        parents = {
            parent.path[-1]
            for parent in repo.asset_graph.get(dg.AssetKey(asset_name)).parent_keys
        }
        assert parents == {"brazil_comp_rfb_companies_duckdb"}


def test_brazil_comp_rfb_contact_domain_assets_have_ordered_dependencies() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    expected_parents = {
        "brazil_comp_rfb_contact_info_duckdb": {"brazil_comp_rfb_companies_duckdb"},
        "brazil_comp_rfb_websites_duckdb": {"brazil_comp_rfb_contact_info_duckdb"},
        "brazil_comp_rfb_clickhouse_company_contacts": {
            "brazil_comp_rfb_contact_info_duckdb"
        },
        "brazil_comp_rfb_clickhouse_company_domains": {
            "brazil_comp_rfb_websites_duckdb"
        },
        "brazil_comp_rfb_clickhouse_websites": {"brazil_comp_rfb_websites_duckdb"},
    }

    for asset_name, expected in expected_parents.items():
        parents = {
            parent.path[-1]
            for parent in repo.asset_graph.get(dg.AssetKey(asset_name)).parent_keys
        }
        assert parents == expected


def test_brazil_comp_rfb_cleanup_depends_on_clickhouse_exports() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    parents = {
        parent.path[-1]
        for parent in repo.asset_graph.get(
            dg.AssetKey("brazil_comp_rfb_previous_partition_cleanup")
        ).parent_keys
    }

    assert parents == {
        "brazil_comp_rfb_clickhouse_companies",
        "brazil_comp_rfb_clickhouse_establishments",
        "brazil_comp_rfb_company_relations_clickhouse",
        "brazil_comp_rfb_clickhouse_company_contacts",
        "brazil_comp_rfb_clickhouse_company_domains",
        "brazil_comp_rfb_clickhouse_websites",
    }


def test_brazil_comp_rfb_resolve_job_covers_brazil_outputs_and_domain_graph() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    assert "brazil_comp_rfb_resolve_job" in set(repo.job_names)
    resolve_job = repo.get_job("brazil_comp_rfb_resolve_job")
    resolve_keys = {
        key.path[-1] for key in resolve_job.asset_layer.executable_asset_keys
    }

    assert type(resolve_job.partitions_def).__name__ == "MonthlyPartitionsDefinition"
    assert resolve_job.run_config is None
    assert {
        "brazil_comp_rfb_raw_archives_s3",
        "brazil_comp_rfb_snapshot_files_duckdb",
        "brazil_comp_rfb_empresas_duckdb",
        "brazil_comp_rfb_estabelecimentos_duckdb",
        "brazil_comp_rfb_simples_duckdb",
        "brazil_comp_rfb_reference_duckdb",
        "brazil_comp_rfb_companies_duckdb",
        "brazil_comp_rfb_contact_info_duckdb",
        "brazil_comp_rfb_websites_duckdb",
        "brazil_comp_rfb_clickhouse_companies",
        "brazil_comp_rfb_clickhouse_establishments",
        "brazil_comp_rfb_clickhouse_company_contacts",
        "brazil_comp_rfb_clickhouse_company_domains",
        "brazil_comp_rfb_clickhouse_websites",
        "brazil_comp_rfb_previous_partition_cleanup",
    }.issubset(resolve_keys)
    assert "domains_clickhouse" not in resolve_keys
    assert "estonia_ar_general_data_duckdb" not in resolve_keys
    assert "norway_resolved_clickhouse" not in resolve_keys
