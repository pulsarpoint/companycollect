from __future__ import annotations

import json
import re
from collections.abc import Callable
from collections.abc import Iterator
from datetime import UTC
from datetime import date
from datetime import datetime
from typing import Any

import dagster as dg
import polars as pl

from dagster_v3.defs.norway_brreg.assets.entity_snapshot import GROUP_NAME
from dagster_v3.defs.norway_brreg.assets.entity_snapshot import NORWAY_BRREG_ENTITY_BUCKET
from dagster_v3.defs.norway_brreg.entity_storage import (
    ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
    ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
    ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
    ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
    ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
    NorwayBrregEntityParquetStorageResource,
)
from dagster_v3.defs.norway_brreg.assets.entity_updates import (
    NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
)
from dagster_v3.defs.norway_brreg.resources import build_entity_rows
from dagster_v3.domains import normalized_url
from dagster_v3.domains import root_domain
from dagster_v3.domains import website_host
from dagster_v3.domains import website_path

NO_COMPANIES_SCHEMA = {
    "org_number": pl.Utf8,
    "country_iso2": pl.Utf8,
    "name": pl.Utf8,
    "name_normalized": pl.Utf8,
    "registration_date": pl.Date,
    "incorporation_date": pl.Date,
    "lifecycle_status": pl.Utf8,
    "is_active": pl.Boolean,
    "legal_form_code": pl.Utf8,
    "legal_form_description_original": pl.Utf8,
    "articles_purpose_original": pl.Utf8,
    "activity_text_original": pl.Utf8,
    "primary_website_url": pl.Utf8,
    "primary_website_host": pl.Utf8,
    "source_system": pl.Utf8,
    "source_run_id": pl.Utf8,
    "source_record_id": pl.Utf8,
    "resolved_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
}

NO_WEBSITES_SCHEMA = {
    "org_number": pl.Utf8,
    "website_url": pl.Utf8,
    "website_normalized_url": pl.Utf8,
    "website_host": pl.Utf8,
    "root_domain": pl.Utf8,
    "website_path": pl.Utf8,
    "registered_on": pl.Date,
    "ended_on": pl.Date,
    "is_current": pl.Boolean,
    "is_primary": pl.Boolean,
    "source_system": pl.Utf8,
    "source_run_id": pl.Utf8,
    "source_record_id": pl.Utf8,
    "resolved_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
}

NO_INDUSTRIES_SCHEMA = {
    "org_number": pl.Utf8,
    "source_industry_code": pl.Utf8,
    "source_industry_code_set": pl.Utf8,
    "description_original": pl.Utf8,
    "description_language": pl.Utf8,
    "description_en": pl.Utf8,
    "description_translated_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
    "description_translation_provider": pl.Utf8,
    "description_translation_model": pl.Utf8,
    "nace_revision": pl.Utf8,
    "nace_code": pl.Utf8,
    "nace_normalized_code": pl.Utf8,
    "nace_mapping_method": pl.Utf8,
    "nace_mapping_status": pl.Utf8,
    "is_primary": pl.Boolean,
    "source_system": pl.Utf8,
    "source_run_id": pl.Utf8,
    "source_record_id": pl.Utf8,
    "resolved_at": pl.Datetime(time_unit="ms", time_zone="UTC"),
}

ORG_LIST_SCHEMA = {
    "org_number": pl.Utf8,
    "change_type": pl.Utf8,
    "source_change_type": pl.Utf8,
    "updated_at": pl.Utf8,
    "update_id": pl.Int64,
}

NormalizeFn = Callable[[pl.DataFrame, str, datetime], pl.DataFrame]

NORMALIZED_PARQUET_KINDS = {"python", "s3", "parquet", "brreg"}

SNAPSHOT_NORMALIZED_ASSETS = (
    (
        "norway_brreg_entities_snapshot_no_companies_parquet",
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
        "Normalizes a full Norway Brreg entity snapshot into no_companies parquet.",
    ),
    (
        "norway_brreg_entities_snapshot_no_websites_parquet",
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
        "Normalizes a full Norway Brreg entity snapshot into no_websites parquet.",
    ),
    (
        "norway_brreg_entities_snapshot_no_industries_parquet",
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
        "Normalizes a full Norway Brreg entity snapshot into no_industries parquet.",
    ),
    (
        "norway_brreg_entities_snapshot_affected_orgs_parquet",
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
        "Lists org numbers affected by a full Norway Brreg entity snapshot.",
    ),
    (
        "norway_brreg_entities_snapshot_removed_orgs_parquet",
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
        "Lists removed org numbers in a full Norway Brreg entity snapshot.",
    ),
)

UPDATE_NORMALIZED_ASSETS = (
    (
        "norway_brreg_entity_updates_no_companies_parquet",
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES,
        "Normalizes one day of Norway Brreg entity updates into no_companies parquet.",
    ),
    (
        "norway_brreg_entity_updates_no_websites_parquet",
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES,
        "Normalizes one day of Norway Brreg entity updates into no_websites parquet.",
    ),
    (
        "norway_brreg_entity_updates_no_industries_parquet",
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES,
        "Normalizes one day of Norway Brreg entity updates into no_industries parquet.",
    ),
    (
        "norway_brreg_entity_updates_affected_orgs_parquet",
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS,
        "Lists org numbers affected by one day of Norway Brreg entity updates.",
    ),
    (
        "norway_brreg_entity_updates_removed_orgs_parquet",
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS,
        "Lists removed org numbers in one day of Norway Brreg entity updates.",
    ),
)


def _normalized_asset_specs(
    assets: tuple[tuple[str, str, str], ...],
    *,
    dependency: dg.AssetKey,
) -> list[dg.AssetSpec]:
    return [
        dg.AssetSpec(
            asset_name,
            deps=[dependency],
            group_name=GROUP_NAME,
            kinds=NORMALIZED_PARQUET_KINDS,
            description=description,
        )
        for asset_name, _table_name, description in assets
    ]


@dg.multi_asset(
    specs=_normalized_asset_specs(
        SNAPSHOT_NORMALIZED_ASSETS,
        dependency=dg.AssetKey("norway_brreg_entities_snapshot_s3"),
    )
)
def norway_brreg_entities_snapshot_normalized_parquets(
    context,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
) -> Iterator[dg.MaterializeResult]:
    run_id = context.op_execution_context.run_id
    context.log.info("Normalizing Norway Brreg entity snapshot parquets for run_id=%s", run_id)
    raw_frame = norway_brreg_entity_storage.read_raw_snapshot_frame()
    resolved_at = datetime.now(UTC)
    context.log.info(
        "Norway Brreg entity snapshot loaded: run_id=%s rows=%d outputs=%d",
        run_id,
        raw_frame.height,
        len(SNAPSHOT_NORMALIZED_ASSETS),
    )
    for asset_name, table_name, _description in SNAPSHOT_NORMALIZED_ASSETS:
        frame = _normalize_table(table_name, raw_frame, source_run_id=run_id, resolved_at=resolved_at)
        s3_key = norway_brreg_entity_storage.write_snapshot_table(run_id, table_name, frame)
        context.log.info(
            "Norway Brreg snapshot table %s written: rows=%d key=%s",
            table_name,
            frame.height,
            s3_key,
        )
        yield _materialize_result(
            asset_key=asset_name,
            table_name=table_name,
            frame=frame,
            s3_key=s3_key,
        )


@dg.multi_asset(
    specs=_normalized_asset_specs(
        UPDATE_NORMALIZED_ASSETS,
        dependency=dg.AssetKey("norway_brreg_entity_updates_s3"),
    ),
    partitions_def=NORWAY_BRREG_ENTITY_UPDATE_PARTITIONS,
)
def norway_brreg_entity_updates_normalized_parquets(
    context,
    norway_brreg_entity_storage: NorwayBrregEntityParquetStorageResource,
) -> Iterator[dg.MaterializeResult]:
    partition_date = context.partition_key
    run_id = context.op_execution_context.run_id
    context.log.info(
        "Normalizing Norway Brreg entity update parquets for partition=%s run_id=%s",
        partition_date,
        run_id,
    )
    raw_frame = norway_brreg_entity_storage.read_raw_update_frame(partition_date)
    resolved_at = datetime.now(UTC)
    context.log.info(
        "Norway Brreg entity updates loaded: partition=%s rows=%d outputs=%d",
        partition_date,
        raw_frame.height,
        len(UPDATE_NORMALIZED_ASSETS),
    )
    for asset_name, table_name, _description in UPDATE_NORMALIZED_ASSETS:
        frame = _normalize_table(table_name, raw_frame, source_run_id=run_id, resolved_at=resolved_at)
        s3_key = norway_brreg_entity_storage.write_update_table(
            partition_date,
            table_name,
            frame,
        )
        context.log.info(
            "Norway Brreg update table %s written: partition=%s rows=%d key=%s",
            table_name,
            partition_date,
            frame.height,
            s3_key,
        )
        yield _materialize_result(
            asset_key=asset_name,
            table_name=table_name,
            frame=frame,
            s3_key=s3_key,
            extra_metadata={"partition_date": partition_date},
        )


def normalize_entity_records_to_no_companies(
    raw_frame: pl.DataFrame,
    source_run_id: str,
    resolved_at: datetime,
) -> pl.DataFrame:
    entity_rows = _entity_rows(raw_frame, source_run_id=source_run_id)
    return _frame(
        [_no_companies_row(row, resolved_at=resolved_at) for row in entity_rows],
        schema=NO_COMPANIES_SCHEMA,
    )


def normalize_entity_records_to_no_websites(
    raw_frame: pl.DataFrame,
    source_run_id: str,
    resolved_at: datetime,
) -> pl.DataFrame:
    entity_rows = _entity_rows(raw_frame, source_run_id=source_run_id)
    return _frame(
        [
            website_row
            for row in entity_rows
            if (website_row := _no_websites_row(row, resolved_at=resolved_at)) is not None
        ],
        schema=NO_WEBSITES_SCHEMA,
    )


def normalize_entity_records_to_no_industries(
    raw_frame: pl.DataFrame,
    source_run_id: str,
    resolved_at: datetime,
) -> pl.DataFrame:
    entity_rows = _entity_rows(raw_frame, source_run_id=source_run_id)
    return _frame(
        [
            industry_row
            for row in entity_rows
            for industry_row in _no_industries_rows(row, resolved_at=resolved_at)
        ],
        schema=NO_INDUSTRIES_SCHEMA,
    )


def normalize_entity_records_to_affected_orgs(
    raw_frame: pl.DataFrame,
    source_run_id: str,
    resolved_at: datetime,
) -> pl.DataFrame:
    return _frame(
        [_org_list_row(row) for row in raw_frame.to_dicts() if _string(row.get("org_number"))],
        schema=ORG_LIST_SCHEMA,
    )


def normalize_entity_records_to_removed_orgs(
    raw_frame: pl.DataFrame,
    source_run_id: str,
    resolved_at: datetime,
) -> pl.DataFrame:
    return _frame(
        [
            _org_list_row(row)
            for row in raw_frame.to_dicts()
            if _string(row.get("org_number")) and _row_is_removed(row)
        ],
        schema=ORG_LIST_SCHEMA,
    )


def _normalize_table(
    table_name: str,
    raw_frame: pl.DataFrame,
    *,
    source_run_id: str,
    resolved_at: datetime,
) -> pl.DataFrame:
    normalizers: dict[str, NormalizeFn] = {
        ENTITY_NORMALIZED_TABLE_NO_COMPANIES: normalize_entity_records_to_no_companies,
        ENTITY_NORMALIZED_TABLE_NO_WEBSITES: normalize_entity_records_to_no_websites,
        ENTITY_NORMALIZED_TABLE_NO_INDUSTRIES: normalize_entity_records_to_no_industries,
        ENTITY_NORMALIZED_TABLE_AFFECTED_ORGS: normalize_entity_records_to_affected_orgs,
        ENTITY_NORMALIZED_TABLE_REMOVED_ORGS: normalize_entity_records_to_removed_orgs,
    }
    return normalizers[table_name](raw_frame, source_run_id, resolved_at)


def _materialize_result(
    *,
    asset_key: str,
    table_name: str,
    frame: pl.DataFrame,
    s3_key: str,
    extra_metadata: dict[str, Any] | None = None,
) -> dg.MaterializeResult:
    metadata: dict[str, Any] = {
        "table_name": table_name,
        "row_count": frame.height,
        "s3_bucket": NORWAY_BRREG_ENTITY_BUCKET,
        "s3_key": s3_key,
    }
    if extra_metadata is not None:
        metadata.update(extra_metadata)
    return dg.MaterializeResult(asset_key=asset_key, metadata=metadata)


def _entity_rows(raw_frame: pl.DataFrame, *, source_run_id: str) -> list[dict[str, Any]]:
    return build_entity_rows(
        [_entity_from_raw_row(row) for row in raw_frame.to_dicts() if _row_has_entity(row)],
        run_id=source_run_id,
    )


def _entity_from_raw_row(row: dict[str, Any]) -> dict[str, Any]:
    return json.loads(_string(row.get("entity_json")))


def _row_has_entity(row: dict[str, Any]) -> bool:
    return bool(_string(row.get("entity_json")).strip()) and not _row_is_removed(row)


def _row_is_removed(row: dict[str, Any]) -> bool:
    return _string(row.get("change_type")).lower() == "removed" or _string(
        row.get("source_change_type")
    ) in {"Fjernet", "Slettet"}


def _org_list_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "org_number": _string(row.get("org_number")),
        "change_type": _string(row.get("change_type")),
        "source_change_type": _string(row.get("source_change_type")),
        "updated_at": _string(row.get("updated_at")),
        "update_id": row.get("update_id"),
    }


def _no_companies_row(row: dict[str, Any], *, resolved_at: datetime) -> dict[str, Any]:
    website = _string(row.get("website"))
    legal_name = _string(row.get("legal_name"))
    return {
        "org_number": _string(row.get("org_number")),
        "country_iso2": _string(row.get("country_iso2")),
        "name": legal_name,
        "name_normalized": legal_name.lower(),
        "registration_date": _date_or_none(row.get("registration_date")),
        "incorporation_date": _date_or_none(row.get("incorporation_date")),
        "lifecycle_status": _string(row.get("status")),
        "is_active": bool(row.get("is_active")),
        "legal_form_code": _none_if_empty(row.get("legal_form_code")),
        "legal_form_description_original": _none_if_empty(
            row.get("legal_form_description_original")
        ),
        "articles_purpose_original": _none_if_empty(row.get("articles_purpose_original")),
        "activity_text_original": _none_if_empty(row.get("activity_text_original")),
        "primary_website_url": _none_if_empty(normalized_url(website)),
        "primary_website_host": _none_if_empty(website_host(website)),
        "source_system": _string(row.get("source_slug")),
        "source_run_id": _string(row.get("source_run_id")),
        "source_record_id": _string(row.get("source_record_id")),
        "resolved_at": resolved_at,
    }


def _no_websites_row(
    row: dict[str, Any],
    *,
    resolved_at: datetime,
) -> dict[str, Any] | None:
    website = _string(row.get("website"))
    normalized = normalized_url(website)
    host = website_host(website)
    domain = root_domain(website)
    if not normalized or not domain:
        return None
    return {
        "org_number": _string(row.get("org_number")),
        "website_url": website,
        "website_normalized_url": normalized,
        "website_host": host,
        "root_domain": domain,
        "website_path": _none_if_empty(website_path(website)),
        "registered_on": None,
        "ended_on": None,
        "is_current": True,
        "is_primary": True,
        "source_system": _string(row.get("source_slug")),
        "source_run_id": _string(row.get("source_run_id")),
        "source_record_id": _string(row.get("source_record_id")),
        "resolved_at": resolved_at,
    }


def _no_industries_rows(
    row: dict[str, Any],
    *,
    resolved_at: datetime,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index in range(1, 4):
        source_industry_code = _string(row.get(f"nace{index}_code"))
        if not source_industry_code:
            continue
        rows.append(
            {
                "org_number": _string(row.get("org_number")),
                "source_industry_code": source_industry_code,
                "source_industry_code_set": "NACE_REV_2",
                "description_original": _none_if_empty(
                    row.get(f"nace{index}_description_original")
                ),
                "description_language": "no",
                "description_en": _none_if_empty(row.get(f"nace{index}_description_en")),
                "description_translated_at": None,
                "description_translation_provider": None,
                "description_translation_model": None,
                "nace_revision": "NACE_REV_2",
                "nace_code": source_industry_code,
                "nace_normalized_code": re.sub(r"[^0-9A-Za-z]", "", source_industry_code),
                "nace_mapping_method": "direct_code",
                "nace_mapping_status": "mapped",
                "is_primary": index == 1,
                "source_system": _string(row.get("source_slug")),
                "source_run_id": _string(row.get("source_run_id")),
                "source_record_id": _string(row.get("source_record_id")),
                "resolved_at": resolved_at,
            }
        )
    return rows


def _frame(rows: list[dict[str, Any]], *, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=schema).select(list(schema))


def _date_or_none(value: Any) -> date | None:
    text = _string(value)
    return date.fromisoformat(text) if text else None


def _none_if_empty(value: Any) -> str | None:
    text = _string(value)
    return text if text else None


def _string(value: Any) -> str:
    return "" if value is None else str(value)
