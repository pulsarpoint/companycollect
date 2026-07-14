from __future__ import annotations

import tempfile
from collections.abc import Callable
from pathlib import Path

from duckdb import DuckDBPyConnection

from dagster_v3.defs.estonia_ar import resources, tables
from dagster_v3.defs.estonia_ar.contacts import (
    _build_contacts_from_json,
    _extract_single_json,
)
from dagster_v3.defs.estonia_ar.industries import _build_industries_from_json

DEFAULT_TIMEOUT_SECONDS = resources.DEFAULT_TIMEOUT_SECONDS


def build_estonia_ar_general_data(
    *,
    duckdb_connection: DuckDBPyConnection,
    source_run_id: str,
    download_url: str = tables.GENERAL_DATA_URL,
    session: resources.HttpSession | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Download the ~4.5 GB yldandmed general-data JSON ONCE and build both the
    contacts and industries DuckDB tables from it.

    Contacts (`sidevahendid`) and industries (`teatatud_tegevusalad`) live in the
    same JSON, so a single download/unzip feeds both — no duplicate fetch. Refuses
    to replace either table on empty input.
    """
    with tempfile.TemporaryDirectory(prefix="estonia_ar_general_data_") as tmpdir:
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
        contacts_counts = _build_contacts_from_json(
            duckdb_connection=duckdb_connection,
            json_path=json_path,
            source_run_id=source_run_id,
        )
        industries_counts = _build_industries_from_json(
            duckdb_connection=duckdb_connection,
            json_path=json_path,
            source_run_id=source_run_id,
        )
    if contacts_counts["contacts"] == 0:
        raise ValueError(
            "Estonia AR yldandmed produced no contacts; refusing to replace the table"
        )
    if industries_counts["industries"] == 0:
        raise ValueError(
            "Estonia AR yldandmed produced no industries; refusing to replace the table"
        )
    counts = {
        "contacts": contacts_counts["contacts"],
        "website_domains": contacts_counts["websites"],
        "email_domains": contacts_counts["email_domains"],
        "industries": industries_counts["industries"],
        "industries_nace_mapped": industries_counts["nace_mapped"],
        "industry_companies": industries_counts["companies"],
    }
    if log is not None:
        log(
            "Built Estonia AR general data (single download): "
            "contacts=%s website_domains=%s email_domains=%s industries=%s companies=%s",
            counts["contacts"],
            counts["website_domains"],
            counts["email_domains"],
            counts["industries"],
            counts["industry_companies"],
        )
    return counts
