from __future__ import annotations

import tempfile
import zipfile
from collections.abc import Callable
from pathlib import Path

import duckdb

from dagster_v3.defs.estonia_ar import resources, tables

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
CONTACTS_TABLE = tables.GENERAL_DATA_RAW_TABLE
CONTACTS_SOURCE_SLUG = "estonia_ar_contacts"
DEFAULT_TIMEOUT_SECONDS = resources.DEFAULT_TIMEOUT_SECONDS

# Only ariregistri_kood + yldandmed.sidevahendid are parsed out of the ~4.5 GB JSON;
# every other (large, nested) field is ignored by read_json's column projection.
_READ_JSON_COLUMNS = (
    "{'ariregistri_kood': 'BIGINT', "
    "'yldandmed': 'STRUCT(sidevahendid STRUCT("
    "liik VARCHAR, liik_tekstina VARCHAR, sisu VARCHAR, lopp_kpv VARCHAR)[])'}"
)


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _extract_single_json(zip_path: Path, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".json")]
        if not members:
            raise ValueError(f"no JSON member found in {zip_path}")
        archive.extract(members[0], dest_dir)
        return dest_dir / members[0]


def _build_contacts_from_json(
    *, database_path: str | Path, json_path: Path, source_run_id: str
) -> dict[str, int]:
    contact_type_en = "\n            ".join(
        f"when '{code}' then '{en}'"
        for code, en in resources.EE_CONTACT_TYPE_EN_BY_CODE.items()
    )
    sql = f"""
        create or replace table {DLT_DATASET_NAME}.{CONTACTS_TABLE} as
        with exploded as (
            select
                ariregistri_kood::varchar as reg_code,
                unnest(yldandmed.sidevahendid) as sv
            from read_json(
                {_sql_literal(str(json_path))},
                format = 'array',
                records = true,
                columns = {_READ_JSON_COLUMNS}
            )
        )
        select
            'EE' as country_iso2,
            '{CONTACTS_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            reg_code as source_record_id,
            reg_code,
            sv.liik as contact_type,
            case sv.liik
            {contact_type_en}
            else '' end as contact_type_en,
            trim(sv.sisu) as contact_value,
            case when sv.lopp_kpv is null or sv.lopp_kpv = '' then 1 else 0 end as is_current,
            try_strptime(sv.lopp_kpv, '%d.%m.%Y')::date as end_date,
            {_sql_literal(tables.GENERAL_DATA_URL)} as source_url
        from exploded
        where coalesce(trim(sv.sisu), '') <> ''
    """
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        connection.execute(sql)
        qualified = f"{DLT_DATASET_NAME}.{CONTACTS_TABLE}"
        contacts = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
        websites = int(
            connection.execute(
                f"select count(*) from {qualified} where contact_type = 'WWW'"
            ).fetchone()[0]
        )
    return {"contacts": contacts, "websites": websites}


def build_estonia_ar_company_contacts(
    *,
    database_path: str | Path,
    source_run_id: str,
    download_url: str = tables.GENERAL_DATA_URL,
    session: resources.HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Download the yldandmed general-data JSON and extract normalized contacts.

    One row per contact (website/email/phone/…) from each company's `sidevahendid`
    array. Refuses to replace on empty input.
    """
    with tempfile.TemporaryDirectory(prefix="estonia_ar_contacts_") as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "yldandmed.json.zip"
        resources._download_to_path(
            url=download_url,
            dest=zip_path,
            timeout_seconds=timeout_seconds,
            user_agent=resources.DEFAULT_USER_AGENT,
            session=session,
        )
        json_path = _extract_single_json(zip_path, tmp)
        counts = _build_contacts_from_json(
            database_path=database_path, json_path=json_path, source_run_id=source_run_id
        )
    if counts["contacts"] == 0:
        raise ValueError(
            "Estonia AR yldandmed produced no contacts; refusing to replace the table"
        )
    if log is not None:
        log(
            "Built Estonia AR company contacts: contacts=%s websites=%s",
            counts["contacts"],
            counts["websites"],
        )
    return counts
