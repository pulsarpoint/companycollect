import json
import logging
import tempfile
import time
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, Protocol

import duckdb
import requests
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.defs.france_sirene import tables

LOGGER = logging.getLogger(__name__)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
UNITE_LEGALE_RAW_TABLE = tables.UNITE_LEGALE_RAW_TABLE
COMPANIES_TABLE = tables.COMPANIES_TABLE
REGISTER_SOURCE_SLUG = "france_sirene_register"

DEFAULT_TIMEOUT_SECONDS = 600
DEFAULT_USER_AGENT = "corpscout-dagster-v3/0.1 (goran.raovic@gmail.com)"
DOWNLOAD_CHUNK_BYTES = 1 << 20
DOWNLOAD_MAX_ATTEMPTS = 5
DOWNLOAD_RETRY_BASE_SECONDS = 2.0

_DOWNLOAD_RETRYABLE_ERRORS = (
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)


class HttpSession(Protocol):
    def get(self, url: str, *, timeout: int, stream: bool = False) -> Any: ...


# INSEE catégorie juridique (4-digit) -> English legal form. Covers the common
# codes (the vast majority of units); unknown -> "" (extensible).
FR_LEGAL_FORM_EN_BY_CODE = {
    "1000": "Sole trader",
    "5202": "General partnership",
    "5306": "Limited partnership",
    "5308": "Partnership limited by shares",
    "5370": "Liberal practice limited partnership",
    "5385": "Liberal practice company",
    "5410": "Limited liability company (SARL)",
    "5415": "Limited liability company (SARL)",
    "5422": "Limited liability company (SARL)",
    "5426": "Limited liability company (SARL)",
    "5430": "Limited liability company (SARL)",
    "5431": "Limited liability company (SARL)",
    "5432": "Limited liability company (SARL)",
    "5442": "Limited liability company (SARL)",
    "5443": "Limited liability company (SARL)",
    "5451": "Limited liability company (SARL)",
    "5453": "Limited liability company (SARL)",
    "5454": "Limited liability company (SARL)",
    "5455": "Limited liability company (SARL)",
    "5458": "Limited liability company (SARL)",
    "5459": "Limited liability company (SARL)",
    "5460": "Limited liability company (SARL)",
    "5485": "Limited liability company (SARL)",
    "5498": "Limited liability company (SARL)",
    "5499": "Limited liability company (SARL)",
    "5505": "Public limited company (SA)",
    "5510": "Public limited company (SA)",
    "5515": "Public limited company (SA)",
    "5520": "Public limited company (SA)",
    "5525": "Public limited company (SA)",
    "5530": "Public limited company (SA)",
    "5560": "Public limited company (SA)",
    "5599": "Public limited company (SA)",
    "5710": "Simplified joint-stock company (SAS)",
    "5720": "Single-member simplified joint-stock company (SASU)",
    "5785": "Simplified joint-stock company (SAS)",
    "6220": "Economic interest grouping (GIE)",
    "6533": "Real estate civil company (SCI)",
    "6540": "Real estate civil company (SCI)",
    "6541": "Real estate civil company (SCI)",
    "6542": "Civil company",
    "6543": "Civil company",
    "6544": "Civil company",
    "9220": "Declared association (loi 1901)",
    "9221": "Association",
    "9222": "Association",
    "9223": "Association",
    "9224": "Association",
    "9230": "Association under local law",
    "9300": "Foundation",
}

FR_STATUS_EN_BY_CODE = {"A": "Active", "C": "Ceased"}


def _resolve_resource_url(
    suffix: str,
    *,
    session: HttpSession | None,
    api_url: str,
    timeout_seconds: int,
) -> str:
    """Resolve the current (monthly, datestamped) stock zip URL ending with `suffix`."""
    http_session = session or dlt_requests.Session()
    response = http_session.get(api_url, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    for resource in payload.get("resources", []):
        url = (resource.get("url") or "").strip()
        if url.endswith(suffix):
            return url
    raise LookupError(f"could not resolve resource URL (suffix {suffix}) from {api_url}")


def resolve_unite_legale_url(
    *,
    session: HttpSession | None = None,
    api_url: str = tables.DATAGOUV_API,
    timeout_seconds: int = 60,
) -> str:
    """Resolve the current StockUniteLegale (legal units) zip URL."""
    return _resolve_resource_url(
        tables.UNITE_LEGALE_URL_SUFFIX,
        session=session,
        api_url=api_url,
        timeout_seconds=timeout_seconds,
    )


def resolve_etablissement_url(
    *,
    session: HttpSession | None = None,
    api_url: str = tables.DATAGOUV_API,
    timeout_seconds: int = 60,
) -> str:
    """Resolve the current StockEtablissement (establishments) zip URL."""
    return _resolve_resource_url(
        tables.ETABLISSEMENT_URL_SUFFIX,
        session=session,
        api_url=api_url,
        timeout_seconds=timeout_seconds,
    )


def _stream_download(
    *, url: str, dest: Path, timeout_seconds: int, session: HttpSession | None
) -> None:
    http_session = session or dlt_requests.Session()
    response = http_session.get(url, timeout=timeout_seconds, stream=True)
    response.raise_for_status()
    iter_content = getattr(response, "iter_content", None)
    written = 0
    with dest.open("wb") as out:
        if callable(iter_content):
            for chunk in iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                if chunk:
                    out.write(chunk)
                    written += len(chunk)
        else:
            body = response.content
            out.write(body)
            written = len(body)
    headers = getattr(response, "headers", None)
    expected = headers.get("Content-Length") if hasattr(headers, "get") else None
    if expected is not None and str(expected).isdigit() and written < int(expected):
        raise requests.exceptions.ChunkedEncodingError(
            f"incomplete download: {written}/{expected} bytes from {url}"
        )


def _download_to_path(
    *,
    url: str,
    dest: Path,
    timeout_seconds: int,
    session: HttpSession | None,
    log: Callable[..., None] | None = None,
    max_attempts: int = DOWNLOAD_MAX_ATTEMPTS,
    retry_base_seconds: float = DOWNLOAD_RETRY_BASE_SECONDS,
) -> None:
    progress_log = log or LOGGER.info
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            _stream_download(
                url=url, dest=dest, timeout_seconds=timeout_seconds, session=session
            )
            return
        except _DOWNLOAD_RETRYABLE_ERRORS as exc:
            last_error = exc
            if attempt >= max_attempts:
                break
            wait_seconds = retry_base_seconds * attempt
            progress_log(
                "France SIRENE download failed (attempt %s/%s), retrying in %ss: url=%s error=%s",
                attempt,
                max_attempts,
                wait_seconds,
                url,
                exc,
            )
            time.sleep(wait_seconds)
    assert last_error is not None
    raise last_error


def _extract_single_csv(zip_path: Path, dest_dir: Path) -> Path:
    with zipfile.ZipFile(zip_path) as archive:
        members = [n for n in archive.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise ValueError(f"no CSV member found in {zip_path}")
        archive.extract(members[0], dest_dir)
        return dest_dir / members[0]


def load_france_sirene_unite_legale(
    *,
    database_path: str | Path,
    download_url: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> int:
    """Download the StockUniteLegale zip and load the CSV into a DuckDB raw table.

    Uses DuckDB's multithreaded read_csv (all_varchar) — never row-by-row Python —
    for the ~4M-row file. Downstream SQL does the relational casts.
    """
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="france_sirene_") as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "stock_unite_legale.zip"
        _download_to_path(
            url=download_url,
            dest=zip_path,
            timeout_seconds=timeout_seconds,
            session=session,
            log=log if callable(log) else None,
        )
        csv_path = _extract_single_csv(zip_path, tmp)
        with duckdb.connect(str(database_path)) as connection:
            connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
            connection.execute(
                f"create or replace table {DLT_DATASET_NAME}.{UNITE_LEGALE_RAW_TABLE} as "
                "select * from read_csv(?, header=true, all_varchar=true, "
                "quote='\"', escape='\"')",
                [str(csv_path)],
            )
            count = int(
                connection.execute(
                    f"select count(*) from {DLT_DATASET_NAME}.{UNITE_LEGALE_RAW_TABLE}"
                ).fetchone()[0]
            )
    if count == 0:
        raise ValueError(
            "France SIRENE StockUniteLegale produced no rows; refusing to replace the table"
        )
    if log is not None:
        log("Loaded France SIRENE unite_legale raw: rows=%s", count)
    return count


def build_etablissement_siege_from_csv(
    *, database_path: str | Path, csv_path: str | Path
) -> int:
    """Load one siège address per legal unit from a StockEtablissement CSV."""
    qualified = f"{DLT_DATASET_NAME}.{tables.ETABLISSEMENT_SIEGE_TABLE}"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema if not exists {DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create or replace table {qualified} as
            select
                siren,
                coalesce(nullif(trim(concat_ws(' ',
                    nullif(trim(numeroVoieEtablissement), ''),
                    nullif(trim(typeVoieEtablissement), ''),
                    nullif(trim(libelleVoieEtablissement), ''))), ''), '') as address,
                coalesce(trim(complementAdresseEtablissement), '') as address_supplement,
                coalesce(
                    nullif(trim(codePostalEtablissement), ''),
                    nullif(trim(codeCedexEtablissement), ''), '') as postal_code,
                coalesce(
                    nullif(trim(libelleCommuneEtablissement), ''),
                    nullif(trim(libelleCommuneEtrangerEtablissement), ''), '') as city,
                coalesce(trim(codeCommuneEtablissement), '') as city_code,
                coalesce(trim(libellePaysEtrangerEtablissement), '') as country_label
            from read_csv(?, header=true, all_varchar=true, quote='"', escape='"')
            where etablissementSiege = 'true' and siren is not null and trim(siren) <> ''
            """,
            [str(csv_path)],
        )
        return int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])


def load_france_sirene_etablissement_siege(
    *,
    database_path: str | Path,
    download_url: str,
    session: HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> int:
    """Download StockEtablissement and keep one siège (HQ) address per legal unit.

    The establishment file is large (~30M rows); read_csv filters to sièges
    (`etablissementSiege='true'`) and projects only the address fields, so one row
    per siren lands. Foreign-address fields fall back when the French ones are empty.
    """
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    qualified = f"{DLT_DATASET_NAME}.{tables.ETABLISSEMENT_SIEGE_TABLE}"
    with tempfile.TemporaryDirectory(prefix="france_sirene_etab_") as tmpdir:
        tmp = Path(tmpdir)
        zip_path = tmp / "stock_etablissement.zip"
        _download_to_path(
            url=download_url,
            dest=zip_path,
            timeout_seconds=timeout_seconds,
            session=session,
            log=log if callable(log) else None,
        )
        csv_path = _extract_single_csv(zip_path, tmp)
        count = build_etablissement_siege_from_csv(
            database_path=database_path, csv_path=csv_path
        )
    if count == 0:
        raise ValueError(
            "France SIRENE StockEtablissement produced no sièges; refusing to replace the table"
        )
    if log is not None:
        log("Loaded France SIRENE siège addresses: rows=%s", count)
    return count


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _case_map(column: str, mapping: dict[str, str]) -> str:
    whens = " ".join(
        f"when {_sql_literal(code)} then {_sql_literal(en)}"
        for code, en in mapping.items()
    )
    return f"case {column} {whens} else '' end"


def build_france_sirene_companies(
    *,
    database_path: str | Path,
    source_run_id: str,
    source_url: str,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Normalize the raw legal-units table into france_sirene.companies."""
    raw = f"{DLT_DATASET_NAME}.{UNITE_LEGALE_RAW_TABLE}"
    siege = f"{DLT_DATASET_NAME}.{tables.ETABLISSEMENT_SIEGE_TABLE}"
    qualified = f"{DLT_DATASET_NAME}.{COMPANIES_TABLE}"
    legal_form_en = _case_map("categorieJuridiqueUniteLegale", FR_LEGAL_FORM_EN_BY_CODE)
    status_en = _case_map("etatAdministratifUniteLegale", FR_STATUS_EN_BY_CODE)
    sql = f"""
        create or replace table {qualified} as
        select
            'FR' as country_iso2,
            '{REGISTER_SOURCE_SLUG}' as source_slug,
            {_sql_literal(source_run_id)} as source_run_id,
            ul.siren as source_record_id,
            '' as source_payload_hash,
            ul.siren as siren,
            coalesce(
                nullif(trim(denominationUniteLegale), ''),
                nullif(trim(concat_ws(' ', prenomUsuelUniteLegale, nomUniteLegale)), ''),
                ''
            ) as name,
            coalesce(trim(denominationUniteLegale), '') as denomination_original,
            coalesce(trim(sigleUniteLegale), '') as acronym,
            coalesce(categorieJuridiqueUniteLegale, '') as legal_form_code,
            {legal_form_en} as legal_form_en,
            coalesce(etatAdministratifUniteLegale, '') as status_code,
            {status_en} as status_en,
            coalesce(etatAdministratifUniteLegale = 'A', false) as is_active,
            try_strptime(dateCreationUniteLegale, '%Y-%m-%d')::date as creation_date,
            coalesce(categorieEntreprise, '') as enterprise_category,
            coalesce(economieSocialeSolidaireUniteLegale = 'O', false) as is_social_solidarity_economy,
            coalesce(
                nullif(trim(activitePrincipaleNAF25UniteLegale), ''),
                nullif(trim(activitePrincipaleUniteLegale), ''),
                ''
            ) as naf_code,
            case when nullif(trim(activitePrincipaleNAF25UniteLegale), '') is not null
                 then 'NAF2025'
                 else coalesce(nomenclatureActivitePrincipaleUniteLegale, '') end as naf_nomenclature,
            {_sql_literal(source_url)} as source_url,
            coalesce(es.address, '') as address,
            coalesce(es.address_supplement, '') as address_supplement,
            coalesce(es.postal_code, '') as postal_code,
            coalesce(es.city, '') as city,
            coalesce(es.city_code, '') as city_code,
            coalesce(es.country_label, '') as country_label,
            '' as raw_entity
        from {raw} ul
        left join {siege} es on es.siren = ul.siren
        where ul.siren is not null and trim(ul.siren) <> ''
    """
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(sql)
        rows = int(connection.execute(f"select count(*) from {qualified}").fetchone()[0])
        active = int(
            connection.execute(
                f"select count(*) from {qualified} where is_active"
            ).fetchone()[0]
        )
    if rows == 0:
        raise ValueError(
            "France SIRENE produced no companies; refusing to replace the table"
        )
    if log is not None:
        log("Built France SIRENE companies: rows=%s active=%s", rows, active)
    return {"companies": rows, "active": active}
