from __future__ import annotations

from datetime import UTC
from datetime import datetime
from io import BytesIO

import dagster as dg
import polars as pl

from dagster_v3.defs.norway_brreg.assets.entity_normalized import (
    norway_brreg_entities_snapshot_normalized_parquets,
    norway_brreg_entity_updates_normalized_parquets,
    normalize_entity_records_to_no_companies,
    normalize_entity_records_to_no_industries,
    normalize_entity_records_to_no_websites,
)
from dagster_v3.defs.norway_brreg.entity_parquet import entity_records_parquet_bytes
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import (
    NORWAY_BRREG_ENTITY_BUCKET,
    entity_snapshot_object_key,
)
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
    ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
    ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_resolved import tables as no_tables


class FakeObjectStore:
    def __init__(self) -> None:
        self.created_buckets: list[str] = []
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body


class FakeEntityStorage:
    def __init__(self, raw_frame: pl.DataFrame) -> None:
        self.raw_frame = raw_frame
        self.snapshot_read_calls: list[str] = []
        self.update_read_calls: list[str] = []
        self.snapshot_writes: dict[str, pl.DataFrame] = {}
        self.update_writes: dict[tuple[str, str], pl.DataFrame] = {}

    def read_raw_snapshot_frame(self, run_id: str) -> pl.DataFrame:
        self.snapshot_read_calls.append(run_id)
        return self.raw_frame

    def read_raw_update_frame(self, partition_date: str) -> pl.DataFrame:
        self.update_read_calls.append(partition_date)
        return self.raw_frame

    def write_snapshot_table(self, run_id: str, table_name: str, frame: pl.DataFrame) -> str:
        self.snapshot_writes[table_name] = frame
        return f"snapshot/{run_id}/{table_name}.parquet"

    def write_update_table(
        self,
        partition_date: str,
        table_name: str,
        frame: pl.DataFrame,
    ) -> str:
        self.update_writes[(partition_date, table_name)] = frame
        return f"updates/{partition_date}/{table_name}.parquet"


def test_storage_resource_reads_raw_snapshot_and_writes_named_normalized_table() -> None:
    object_store = FakeObjectStore()
    run_id = "run-1"
    raw_frame = _raw_snapshot_frame()
    object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, entity_snapshot_object_key(run_id))] = (
        _parquet_bytes(raw_frame)
    )
    storage = NorwayBrregEntityParquetStorageResource(object_store=object_store)

    assert storage.read_raw_snapshot_frame(run_id).to_dicts() == raw_frame.to_dicts()

    key = storage.write_snapshot_table(
        run_id,
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
        pl.DataFrame([{"org_number": "1000"}]),
    )

    assert key == (
        "norway_brreg/entities/normalized/snapshot/run_id=run-1/no_companies.parquet"
    )
    assert object_store.created_buckets == [NORWAY_BRREG_ENTITY_BUCKET]
    assert pl.read_parquet(
        BytesIO(object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, key)])
    ).to_dicts() == [{"org_number": "1000"}]
    assert storage.read_normalized_snapshot_table(
        run_id,
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    ).to_dicts() == [{"org_number": "1000"}]


def test_update_storage_resource_writes_named_normalized_table_against_object_store() -> None:
    object_store = FakeObjectStore()
    storage = NorwayBrregEntityParquetStorageResource(object_store=object_store)

    key = storage.write_update_table(
        "2026-06-29",
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
        pl.DataFrame([{"org_number": "3000"}]),
    )

    assert key == (
        "norway_brreg/entities/normalized/updates/date=2026-06-29/removed_orgs.parquet"
    )
    assert pl.read_parquet(
        BytesIO(object_store.objects[(NORWAY_BRREG_ENTITY_BUCKET, key)])
    ).to_dicts() == [{"org_number": "3000"}]


def test_normalize_entity_records_to_no_tables_matches_clickhouse_shapes() -> None:
    raw_frame = _raw_snapshot_frame()
    resolved_at = datetime(2026, 6, 29, tzinfo=UTC)

    no_companies = normalize_entity_records_to_no_companies(
        raw_frame,
        source_run_id="run-1",
        resolved_at=resolved_at,
    )
    assert no_companies.columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_COMPANIES_TABLE]
    )
    assert no_companies.select(
        [
            "org_number",
            "country_iso2",
            "name",
            "name_normalized",
            "registration_date",
            "incorporation_date",
            "lifecycle_status",
            "is_active",
            "legal_form_code",
            "legal_form_description_original",
            "articles_purpose_original",
            "activity_text_original",
            "primary_website_url",
            "primary_website_host",
            "source_system",
            "source_run_id",
            "source_record_id",
        ]
    ).to_dicts() == [
        {
            "org_number": "1000",
            "country_iso2": "NO",
            "name": "Active One AS",
            "name_normalized": "active one as",
            "registration_date": datetime(2020, 1, 2).date(),
            "incorporation_date": datetime(2020, 1, 1).date(),
            "lifecycle_status": "active",
            "is_active": True,
            "legal_form_code": "AS",
            "legal_form_description_original": "Aksjeselskap",
            "articles_purpose_original": "To develop software.",
            "activity_text_original": "Technology services.",
            "primary_website_url": "https://www.activeone.no/about",
            "primary_website_host": "www.activeone.no",
            "source_system": "norway_brregenhet",
            "source_run_id": "run-1",
            "source_record_id": "1000",
        },
        {
            "org_number": "2000",
            "country_iso2": "NO",
            "name": "Inactive Two AS",
            "name_normalized": "inactive two as",
            "registration_date": datetime(2018, 3, 4).date(),
            "incorporation_date": None,
            "lifecycle_status": "bankrupt",
            "is_active": False,
            "legal_form_code": "AS",
            "legal_form_description_original": "Aksjeselskap",
            "articles_purpose_original": None,
            "activity_text_original": None,
            "primary_website_url": None,
            "primary_website_host": None,
            "source_system": "norway_brregenhet",
            "source_run_id": "run-1",
            "source_record_id": "2000",
        },
    ]

    no_websites = normalize_entity_records_to_no_websites(
        raw_frame,
        source_run_id="run-1",
        resolved_at=resolved_at,
    )
    assert no_websites.columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_WEBSITES_TABLE]
    )
    assert no_websites.select(
        [
            "org_number",
            "website_url",
            "website_normalized_url",
            "website_host",
            "root_domain",
            "website_path",
            "is_current",
            "is_primary",
        ]
    ).to_dicts() == [
        {
            "org_number": "1000",
            "website_url": "WWW.ActiveOne.NO/about",
            "website_normalized_url": "https://www.activeone.no/about",
            "website_host": "www.activeone.no",
            "root_domain": "activeone.no",
            "website_path": "/about",
            "is_current": True,
            "is_primary": True,
        }
    ]

    no_industries = normalize_entity_records_to_no_industries(
        raw_frame,
        source_run_id="run-1",
        resolved_at=resolved_at,
    )
    assert no_industries.columns == list(
        no_tables.RESOLVED_EXPORT_COLUMNS[no_tables.NO_INDUSTRIES_TABLE]
    )
    assert no_industries.select(
        [
            "org_number",
            "source_industry_code",
            "description_original",
            "description_en",
            "nace_normalized_code",
            "is_primary",
        ]
    ).to_dicts() == [
        {
            "org_number": "1000",
            "source_industry_code": "62.010",
            "description_original": "Programmeringstjenester",
            "description_en": None,
            "nace_normalized_code": "62010",
            "is_primary": True,
        },
        {
            "org_number": "1000",
            "source_industry_code": "70.100",
            "description_original": "Hovedkontortjenester",
            "description_en": None,
            "nace_normalized_code": "70100",
            "is_primary": False,
        },
    ]


def test_snapshot_multi_asset_reads_raw_once_and_materializes_all_table_parquets() -> None:
    storage = FakeEntityStorage(_raw_snapshot_frame())
    context = dg.build_asset_context()
    run_id = context.op_execution_context.run_id

    results = list(
        norway_brreg_entities_snapshot_normalized_parquets(
            context=context,
            norway_brreg_entity_storage=storage,
        )
    )

    assert storage.snapshot_read_calls == [run_id]
    assert {result.asset_key.path[-1] for result in results} == {
        "norway_brreg_entities_snapshot_no_companies_parquet",
        "norway_brreg_entities_snapshot_no_websites_parquet",
        "norway_brreg_entities_snapshot_no_industries_parquet",
        "norway_brreg_entities_snapshot_affected_orgs_parquet",
        "norway_brreg_entities_snapshot_removed_orgs_parquet",
    }
    assert set(storage.snapshot_writes) == {
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
    }
    assert {
        result.metadata["table_name"]: result.metadata["row_count"] for result in results
    } == {
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES: 2,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES: 1,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: 2,
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: 2,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS: 0,
    }


def test_update_multi_asset_reads_raw_once_and_materializes_replacements_and_removed_orgs() -> None:
    storage = FakeEntityStorage(_raw_update_frame())
    context = dg.build_asset_context(partition_key="2026-06-29")

    results = list(
        norway_brreg_entity_updates_normalized_parquets(
            context=context,
            norway_brreg_entity_storage=storage,
        )
    )

    assert storage.update_read_calls == ["2026-06-29"]
    assert {result.asset_key.path[-1] for result in results} == {
        "norway_brreg_entity_updates_no_companies_parquet",
        "norway_brreg_entity_updates_no_websites_parquet",
        "norway_brreg_entity_updates_no_industries_parquet",
        "norway_brreg_entity_updates_affected_orgs_parquet",
        "norway_brreg_entity_updates_removed_orgs_parquet",
    }
    assert storage.update_writes[("2026-06-29", ENTITY_NORMALIZED_TABLE_NO_COMPANIES)].select(
        ["org_number", "name"]
    ).to_dicts() == [{"org_number": "1000", "name": "Active One AS"}]
    assert storage.update_writes[("2026-06-29", ENTITY_NORMALIZED_TABLE_REMOVED_ORGS)].to_dicts() == [
        {
            "org_number": "3000",
            "change_type": "removed",
            "source_change_type": "Fjernet",
            "updated_at": "2026-06-29T09:00:00.000Z",
            "update_id": 11,
        }
    ]
    assert {
        result.metadata["table_name"]: result.metadata["row_count"] for result in results
    } == {
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES: 1,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES: 1,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: 2,
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: 2,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS: 1,
    }
    assert all(result.metadata["partition_date"] == "2026-06-29" for result in results)


def _raw_snapshot_frame() -> pl.DataFrame:
    return _read_parquet_bytes(
        entity_records_parquet_bytes(
            [
                {
                    "org_number": "1000",
                    "change_type": "snapshot",
                    "source_change_type": "snapshot",
                    "updated_at": None,
                    "update_id": None,
                    "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/1000",
                    "entity": _active_entity(),
                    "raw_update": None,
                },
                {
                    "org_number": "2000",
                    "change_type": "snapshot",
                    "source_change_type": "snapshot",
                    "updated_at": None,
                    "update_id": None,
                    "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/2000",
                    "entity": _inactive_entity_without_website(),
                    "raw_update": None,
                },
            ]
        )
    )


def _raw_update_frame() -> pl.DataFrame:
    return _read_parquet_bytes(
        entity_records_parquet_bytes(
            [
                {
                    "org_number": "1000",
                    "change_type": "changed",
                    "source_change_type": "Endring",
                    "updated_at": "2026-06-29T08:30:00.000Z",
                    "update_id": 10,
                    "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/1000",
                    "entity": _active_entity(),
                    "raw_update": {"organisasjonsnummer": "1000", "endringstype": "Endring"},
                },
                {
                    "org_number": "3000",
                    "change_type": "removed",
                    "source_change_type": "Fjernet",
                    "updated_at": "2026-06-29T09:00:00.000Z",
                    "update_id": 11,
                    "entity_url": "https://data.brreg.no/enhetsregisteret/api/enheter/3000",
                    "entity": None,
                    "raw_update": {"organisasjonsnummer": "3000", "endringstype": "Fjernet"},
                },
            ],
            allow_empty=True,
        )
    )


def _read_parquet_bytes(body: bytes) -> pl.DataFrame:
    return pl.read_parquet(BytesIO(body))


def _parquet_bytes(frame: pl.DataFrame) -> bytes:
    buffer = BytesIO()
    frame.write_parquet(buffer)
    return buffer.getvalue()


def _active_entity() -> dict[str, object]:
    return {
        "organisasjonsnummer": "1000",
        "navn": "Active One AS",
        "registreringsdatoEnhetsregisteret": "2020-01-02",
        "stiftelsesdato": "2020-01-01",
        "hjemmeside": "WWW.ActiveOne.NO/about",
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "naeringskode1": {"kode": "62.010", "beskrivelse": "Programmeringstjenester"},
        "naeringskode2": {"kode": "70.100", "beskrivelse": "Hovedkontortjenester"},
        "vedtektsfestetFormaal": ["To develop software."],
        "aktivitet": ["Technology services."],
        "_links": {"self": {"href": "https://data.brreg.no/enheter/1000"}},
    }


def _inactive_entity_without_website() -> dict[str, object]:
    return {
        "organisasjonsnummer": "2000",
        "navn": "Inactive Two AS",
        "registreringsdatoEnhetsregisteret": "2018-03-04",
        "organisasjonsform": {"kode": "AS", "beskrivelse": "Aksjeselskap"},
        "konkurs": True,
        "_links": {"self": {"href": "https://data.brreg.no/enheter/2000"}},
    }
