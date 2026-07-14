import json
import tempfile
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

import requests

from dagster_v3.defs.brazil_financial.cvm.parsing import (
    BRAZIL_CVM_DUCKDB_SCHEMA,
    CSV_ENCODING,
    CSV_FALLBACK_ENCODING,
    CSV_WINDOWS_1252_ENCODING,
)
from dagster_v3.defs.brazil_financial.cvm.source import BRAZIL_CVM_RAW_BUCKET

CVM_COMPANIES_SOURCE_URL = (
    "https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv"
)
CVM_COMPANIES_SOURCE_FILE_NAME = "cad_cia_aberta.csv"
CVM_COMPANIES_CSV_OBJECT_PREFIX = "brazil_cvm/cad/raw_csv"
CVM_COMPANIES_METADATA_OBJECT_KEY = (
    f"{CVM_COMPANIES_CSV_OBJECT_PREFIX}/latest/metadata.json"
)
CVM_COMPANIES_TABLE = "companies"
CVM_COMPANIES_SOURCE_SLUG = "brazil_cvm_companies"
CVM_COMPANIES_COLUMNS = (
    "country_iso2",
    "source_slug",
    "source_run_id",
    "source_record_id",
    "cnpj",
    "cnpj_basico",
    "cvm_code",
    "legal_name",
    "trade_name",
    "registration_date",
    "constitution_date",
    "cancellation_date",
    "cancellation_reason",
    "registration_status",
    "registration_status_start_date",
    "industry_sector",
    "market_type",
    "registration_category",
    "registration_category_start_date",
    "issuer_status",
    "issuer_status_start_date",
    "shareholding_control",
    "address_type",
    "street",
    "address_complement",
    "district",
    "municipality",
    "state",
    "country",
    "postal_code",
    "phone_area_code",
    "phone_number",
    "fax_area_code",
    "fax_number",
    "email",
    "responsible_type",
    "responsible_name",
    "responsible_start_date",
    "responsible_street",
    "responsible_address_complement",
    "responsible_district",
    "responsible_municipality",
    "responsible_state",
    "responsible_country",
    "responsible_postal_code",
    "responsible_phone_area_code",
    "responsible_phone_number",
    "responsible_fax_area_code",
    "responsible_fax_number",
    "responsible_email",
    "auditor_cnpj",
    "auditor_name",
    "source_url",
    "source_file_name",
    "source_row_number",
    "resolved_at",
)


@dataclass(frozen=True)
class BrazilCvmCompaniesCsvSyncResult:
    source_url: str
    csv_key: str
    metadata_key: str
    downloaded: bool
    reused_existing_csv: bool
    size_bytes: int
    sha256: str
    content_type: str
    source_last_modified: str
    synced_at: str

    def metadata(self) -> dict[str, object]:
        return {
            "source_url": self.source_url,
            "s3_bucket": BRAZIL_CVM_RAW_BUCKET,
            "csv_key": self.csv_key,
            "metadata_key": self.metadata_key,
            "downloaded": self.downloaded,
            "reused_existing_csv": self.reused_existing_csv,
            "size_bytes": self.size_bytes,
            "sha256": self.sha256,
            "content_type": self.content_type,
            "source_last_modified": self.source_last_modified,
            "synced_at": self.synced_at,
        }


def cvm_companies_csv_object_key(content_sha256: str) -> str:
    return (
        f"{CVM_COMPANIES_CSV_OBJECT_PREFIX}/sha256={content_sha256}/"
        f"{CVM_COMPANIES_SOURCE_FILE_NAME}"
    )


def sync_brazil_fin_cvm_companies_csv(
    *,
    object_store: Any,
    source_url: str = CVM_COMPANIES_SOURCE_URL,
    session: Any | None = None,
    timeout_seconds: int = 300,
    synced_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> BrazilCvmCompaniesCsvSyncResult:
    session = session or requests
    synced_at = synced_at or datetime.now(UTC)
    response = session.get(source_url, timeout=timeout_seconds)
    response.raise_for_status()
    body = response.content
    content_sha256 = sha256(body).hexdigest()
    csv_key = cvm_companies_csv_object_key(content_sha256)

    object_store.ensure_bucket(BRAZIL_CVM_RAW_BUCKET)
    reused_existing_csv = object_store.exists(csv_key, bucket=BRAZIL_CVM_RAW_BUCKET)
    if not reused_existing_csv:
        object_store.write_bytes(csv_key, body, bucket=BRAZIL_CVM_RAW_BUCKET)
        if log is not None:
            log("Stored Brazil CVM companies raw CSV: key=%s", csv_key)
    elif log is not None:
        log("Reusing Brazil CVM companies raw CSV: key=%s", csv_key)

    result = BrazilCvmCompaniesCsvSyncResult(
        source_url=source_url,
        csv_key=csv_key,
        metadata_key=CVM_COMPANIES_METADATA_OBJECT_KEY,
        downloaded=True,
        reused_existing_csv=reused_existing_csv,
        size_bytes=len(body),
        sha256=content_sha256,
        content_type=str(response.headers.get("Content-Type", "")),
        source_last_modified=str(response.headers.get("Last-Modified", "")),
        synced_at=synced_at.isoformat(),
    )
    metadata = {
        **asdict(result),
        "source_file_name": CVM_COMPANIES_SOURCE_FILE_NAME,
    }
    object_store.write_json(
        CVM_COMPANIES_METADATA_OBJECT_KEY,
        json.dumps(metadata, sort_keys=True),
        bucket=BRAZIL_CVM_RAW_BUCKET,
    )
    return result


def load_brazil_fin_cvm_companies_from_object_store(
    *,
    connection: Any,
    object_store: Any,
    source_run_id: str,
    metadata_key: str = CVM_COMPANIES_METADATA_OBJECT_KEY,
    resolved_at: datetime | None = None,
    log: Callable[..., object] | None = None,
) -> dict[str, int | str]:
    metadata = json.loads(
        object_store.read_bytes(metadata_key, bucket=BRAZIL_CVM_RAW_BUCKET).decode(
            "utf-8"
        )
    )
    csv_key = str(metadata["csv_key"])
    csv_body = object_store.read_bytes(csv_key, bucket=BRAZIL_CVM_RAW_BUCKET)

    with tempfile.TemporaryDirectory(prefix="brazil_fin_cvm_companies_raw_") as tmpdir:
        csv_path = Path(tmpdir) / str(
            metadata.get("source_file_name", CVM_COMPANIES_SOURCE_FILE_NAME)
        )
        csv_path.write_bytes(csv_body)
        if log is not None:
            log("Loading Brazil CVM companies CSV from object storage: key=%s", csv_key)
        counts = load_brazil_fin_cvm_companies_csv(
            connection=connection,
            csv_path=csv_path,
            source_url=str(metadata["source_url"]),
            source_run_id=source_run_id,
            resolved_at=resolved_at,
        )
    return {
        **counts,
        "raw_csv_key": csv_key,
        "raw_metadata_key": metadata_key,
        "raw_sha256": str(metadata.get("sha256", "")),
    }


def load_brazil_fin_cvm_companies_csv(
    *,
    connection: Any,
    csv_path: str | Path,
    source_url: str,
    source_run_id: str,
    resolved_at: datetime | None = None,
) -> dict[str, int]:
    resolved_at = resolved_at or datetime.now(UTC)
    csv_path = Path(csv_path)
    _ensure_companies_table(connection)
    _read_companies_csv_to_temp_table(connection=connection, csv_path=csv_path)
    connection.execute(f"delete from {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE}")
    insert_columns = ", ".join(CVM_COMPANIES_COLUMNS)
    connection.execute(
        f"""
        insert into {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE}
        ({insert_columns})
        select
            'BR',
            ?,
            ?,
            concat_ws('|', ?, regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), coalesce(CD_CVM, '')),
            regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'),
            substr(regexp_replace(CNPJ_CIA, '[^0-9]', '', 'g'), 1, 8),
            coalesce(CD_CVM, ''),
            coalesce(DENOM_SOCIAL, ''),
            coalesce(DENOM_COMERC, ''),
            try_cast(nullif(DT_REG, '') as date),
            try_cast(nullif(DT_CONST, '') as date),
            try_cast(nullif(DT_CANCEL, '') as date),
            coalesce(MOTIVO_CANCEL, ''),
            coalesce(SIT, ''),
            try_cast(nullif(DT_INI_SIT, '') as date),
            coalesce(SETOR_ATIV, ''),
            coalesce(TP_MERC, ''),
            coalesce(CATEG_REG, ''),
            try_cast(nullif(DT_INI_CATEG, '') as date),
            coalesce(SIT_EMISSOR, ''),
            try_cast(nullif(DT_INI_SIT_EMISSOR, '') as date),
            coalesce(CONTROLE_ACIONARIO, ''),
            coalesce(TP_ENDER, ''),
            coalesce(LOGRADOURO, ''),
            coalesce(COMPL, ''),
            coalesce(BAIRRO, ''),
            coalesce(MUN, ''),
            coalesce(UF, ''),
            coalesce(PAIS, ''),
            coalesce(CEP, ''),
            coalesce(DDD_TEL, ''),
            coalesce(TEL, ''),
            coalesce(DDD_FAX, ''),
            coalesce(FAX, ''),
            coalesce(EMAIL, ''),
            coalesce(TP_RESP, ''),
            coalesce(RESP, ''),
            try_cast(nullif(DT_INI_RESP, '') as date),
            coalesce(LOGRADOURO_RESP, ''),
            coalesce(COMPL_RESP, ''),
            coalesce(BAIRRO_RESP, ''),
            coalesce(MUN_RESP, ''),
            coalesce(UF_RESP, ''),
            coalesce(PAIS_RESP, ''),
            coalesce(CEP_RESP, ''),
            coalesce(DDD_TEL_RESP, ''),
            coalesce(TEL_RESP, ''),
            coalesce(DDD_FAX_RESP, ''),
            coalesce(FAX_RESP, ''),
            coalesce(EMAIL_RESP, ''),
            coalesce(regexp_replace(CNPJ_AUDITOR, '[^0-9]', '', 'g'), ''),
            coalesce(AUDITOR, ''),
            ?,
            ?,
            source_row_number,
            ?
        from _brazil_fin_cvm_companies_csv
        """,
        [
            CVM_COMPANIES_SOURCE_SLUG,
            source_run_id,
            CVM_COMPANIES_SOURCE_SLUG,
            source_url,
            csv_path.name,
            resolved_at,
        ],
    )
    return {
        "company_row_count": _count_companies(connection),
        "active_company_row_count": _count_active_companies(connection),
        "distinct_cnpj_count": _count_distinct_cnpj(connection),
    }


def _ensure_companies_table(connection: Any) -> None:
    connection.execute(f"create schema if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}")
    connection.execute(
        f"""
        create table if not exists {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE} (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            cnpj varchar,
            cnpj_basico varchar,
            cvm_code varchar,
            legal_name varchar,
            trade_name varchar,
            registration_date date,
            constitution_date date,
            cancellation_date date,
            cancellation_reason varchar,
            registration_status varchar,
            registration_status_start_date date,
            industry_sector varchar,
            market_type varchar,
            registration_category varchar,
            registration_category_start_date date,
            issuer_status varchar,
            issuer_status_start_date date,
            shareholding_control varchar,
            address_type varchar,
            street varchar,
            address_complement varchar,
            district varchar,
            municipality varchar,
            state varchar,
            country varchar,
            postal_code varchar,
            phone_area_code varchar,
            phone_number varchar,
            fax_area_code varchar,
            fax_number varchar,
            email varchar,
            responsible_type varchar,
            responsible_name varchar,
            responsible_start_date date,
            responsible_street varchar,
            responsible_address_complement varchar,
            responsible_district varchar,
            responsible_municipality varchar,
            responsible_state varchar,
            responsible_country varchar,
            responsible_postal_code varchar,
            responsible_phone_area_code varchar,
            responsible_phone_number varchar,
            responsible_fax_area_code varchar,
            responsible_fax_number varchar,
            responsible_email varchar,
            auditor_cnpj varchar,
            auditor_name varchar,
            source_url varchar,
            source_file_name varchar,
            source_row_number bigint,
            resolved_at timestamp
        )
        """
    )


def _read_companies_csv_to_temp_table(*, connection: Any, csv_path: Path) -> None:
    try:
        _read_companies_csv_to_temp_table_with_encoding(
            connection=connection,
            csv_path=csv_path,
            encoding=CSV_ENCODING,
        )
    except Exception as exc:
        if "File is not latin-1 encoded" not in str(exc):
            raise
        fallback_path = csv_path.with_suffix(f"{csv_path.suffix}.utf8")
        fallback_path.write_text(
            csv_path.read_bytes().decode(CSV_WINDOWS_1252_ENCODING),
            encoding=CSV_FALLBACK_ENCODING,
        )
        _read_companies_csv_to_temp_table_with_encoding(
            connection=connection,
            csv_path=fallback_path,
            encoding=CSV_FALLBACK_ENCODING,
        )


def _read_companies_csv_to_temp_table_with_encoding(
    *,
    connection: Any,
    csv_path: Path,
    encoding: str,
) -> None:
    connection.execute(
        f"""
        create or replace temporary table _brazil_fin_cvm_companies_csv as
        select
            row_number() over ()::bigint as source_row_number,
            *
        from read_csv(
            ?,
            delim=';',
            header=true,
            all_varchar=true,
            encoding='{encoding}',
            ignore_errors=false
        )
        """,
        [str(csv_path)],
    )


def _count_companies(connection: Any) -> int:
    return int(
        connection.execute(
            f"select count(*) from {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE}"
        ).fetchone()[0]
    )


def _count_active_companies(connection: Any) -> int:
    return int(
        connection.execute(
            f"""
            select count(*)
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE}
            where upper(registration_status) like 'ATIVO%'
               or upper(registration_status) = 'EM FUNCIONAMENTO NORMAL'
            """
        ).fetchone()[0]
    )


def _count_distinct_cnpj(connection: Any) -> int:
    return int(
        connection.execute(
            f"""
            select count(distinct cnpj)
            from {BRAZIL_CVM_DUCKDB_SCHEMA}.{CVM_COMPANIES_TABLE}
            where cnpj != ''
            """
        ).fetchone()[0]
    )
