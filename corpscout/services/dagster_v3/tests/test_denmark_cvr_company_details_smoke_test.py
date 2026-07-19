from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.denmark_cvr.company_details import (
    DenmarkCvrCompanyDetailDownload,
    DenmarkCvrCompanyDetailResource,
)
from scripts.denmark_cvr_company_details_smoke_test import (
    sample_company_cvrs,
    validate_company_detail_sample,
)


def test_sample_company_cvrs_returns_a_stable_bounded_sample(tmp_path: Path) -> None:
    database_path = tmp_path / "denmark.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("CREATE SCHEMA denmark_cvr")
        connection.execute("CREATE TABLE denmark_cvr.companies (cvr VARCHAR)")
        connection.executemany(
            "INSERT INTO denmark_cvr.companies VALUES (?)",
            [(f"{company_number:08d}",) for company_number in range(1, 61)],
        )

    first_sample = sample_company_cvrs(
        database_path,
        sample_size=50,
        seed="test-seed",
    )
    second_sample = sample_company_cvrs(
        database_path,
        sample_size=50,
        seed="test-seed",
    )

    assert first_sample == second_sample
    assert len(first_sample) == 50
    assert len(set(first_sample)) == 50


def test_validate_company_detail_sample_reports_all_unmapped_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_cvr = "12345678"
    second_cvr = "87654321"
    downloads = (
        _download(first_cvr, {"stamdata": {"cvrnummer": first_cvr}}),
        _download(
            second_cvr,
            {
                "stamdata": {
                    "cvrnummer": second_cvr,
                    "firstUnknown": "one",
                    "nested": {"secondUnknown": "two"},
                },
            },
        ),
    )
    requested_cvrs: list[tuple[str, ...]] = []

    def iter_company_details(
        _details: DenmarkCvrCompanyDetailResource,
        cvrs: tuple[str, ...],
    ) -> tuple[DenmarkCvrCompanyDetailDownload, ...]:
        requested_cvrs.append(cvrs)
        return downloads

    monkeypatch.setattr(
        DenmarkCvrCompanyDetailResource,
        "iter_company_details",
        iter_company_details,
    )
    progress: list[str] = []

    summary = validate_company_detail_sample(
        DenmarkCvrCompanyDetailResource(),
        (first_cvr, second_cvr),
        report_progress=progress.append,
    )

    assert requested_cvrs == [(first_cvr, second_cvr)]
    assert summary.selected_company_count == 2
    assert summary.valid_company_count == 1
    assert len(summary.failures) == 1
    assert "stamdata.firstUnknown" in summary.failures[0]
    assert "stamdata.nested" in summary.failures[0]
    assert "stamdata.nested.secondUnknown" in summary.failures[0]
    assert progress[-1] == "[2/2] CVR 87654321: failed"


def _download(cvr: str, payload: dict[str, Any]) -> DenmarkCvrCompanyDetailDownload:
    return DenmarkCvrCompanyDetailDownload(
        cvr=cvr,
        source_url=f"https://datacvr.virk.dk/gateway/test?cvrnummer={cvr}",
        raw_body="{}",
        payload=payload,
        status=200,
        response_headers={"content-type": "application/json"},
    )
