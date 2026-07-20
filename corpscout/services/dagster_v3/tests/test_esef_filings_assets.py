"""Tests for the ESEF filings index crawl asset.

No network: EsefFilingsClient is monkeypatched on the assets module to a stub
returning canned EsefFilingRecord instances -- mirroring the class-monkeypatch
pattern used for ExchangeRateClient in
tests/test_norway_brreg_financial_statement_assets.py. Each test materializes
into a fresh tmp_path DuckDB file via dg.materialize's resource override.
"""

from collections.abc import Iterator
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.common.duckdb_resources import (
    duckdb_resource,
    read_only_duckdb_connection,
)
from dagster_v3.defs.esef_filings import assets
from dagster_v3.defs.esef_filings.client import EsefFilingRecord


def _record(
    *,
    fxo_id: str,
    country: str = "SE",
    lei: str = "549300CSLHPO6Y1AZN37",
    entity_name: str = "Example AB",
    json_url: str | None = "https://filings.xbrl.org/x/facts.json",
) -> EsefFilingRecord:
    return EsefFilingRecord(
        lei=lei,
        entity_name=entity_name,
        fxo_id=fxo_id,
        country=country,
        period_end="2022-12-31",
        date_added="2023-01-01 00:00:00",
        processed_at="2023-01-02 00:00:00",
        json_url=json_url,
        package_url="https://filings.xbrl.org/x/package.zip",
        report_url="https://filings.xbrl.org/x/report.html",
        viewer_url="https://filings.xbrl.org/x/viewer.html",
        package_sha256="deadbeef",
        error_count=0,
        warning_count=0,
        inconsistency_count=0,
    )


class _StubEsefFilingsClient:
    """Stand-in for EsefFilingsClient() -- no session, no network."""

    def __init__(self, records: list[EsefFilingRecord]) -> None:
        self._records = records

    def iter_filings(self) -> Iterator[EsefFilingRecord]:
        yield from self._records


def _patch_client(
    monkeypatch: pytest.MonkeyPatch, records: list[EsefFilingRecord]
) -> None:
    monkeypatch.setattr(
        assets, "EsefFilingsClient", lambda: _StubEsefFilingsClient(records)
    )


def _db_resource(tmp_path: Path) -> object:
    # duckdb_resource() caches by (path, connection_config), so calling this
    # again for the same tmp_path returns the same resource -- required for
    # the read-back connection below to share config with the write side
    # (DuckDB refuses a second connection to the same file with a different
    # configuration than an existing one).
    return duckdb_resource(tmp_path / "esef_filings_source.duckdb")


def _resources(tmp_path: Path) -> dict[str, object]:
    return {"esef_filings_duckdb": _db_resource(tmp_path)}


def _fetch_filings_index(tmp_path: Path) -> list[tuple]:
    with read_only_duckdb_connection(_db_resource(tmp_path)) as connection:
        return connection.execute(
            "select fxo_id, has_json_facts, source_url, source_run_id "
            "from esef_filings.filings_index order by fxo_id"
        ).fetchall()


def test_three_records_land_one_without_json_facts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = [
        _record(fxo_id="A-1"),
        _record(fxo_id="A-2", json_url=None),
        _record(fxo_id="A-3", country="FI"),
    ]
    _patch_client(monkeypatch, records)

    result = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )

    assert result.success
    metadata = result.asset_materializations_for_node("esef_filings_index_duckdb")[
        0
    ].metadata
    assert metadata["row_count"].value == 3
    assert metadata["with_json_facts_count"].value == 2
    assert metadata["without_json_facts_count"].value == 1
    assert metadata["distinct_country_count"].value == 2
    assert metadata["country_distribution_top10"].value == {"SE": 2, "FI": 1}

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2", "A-3"]
    assert [row[1] for row in rows] == [True, False, True]
    assert all(row[2] == assets.ESEF_INDEX_URL for row in rows)
    assert all(row[3] == result.run_id for row in rows)


def test_second_materialization_full_replaces_no_duplicates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert first.success

    _patch_client(monkeypatch, [_record(fxo_id="B-1")])
    second = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert second.success

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["B-1"]


def test_empty_crawl_raises_value_error_and_preserves_existing_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1"), _record(fxo_id="A-2")])
    first = dg.materialize(
        [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
    )
    assert first.success

    _patch_client(monkeypatch, [])
    with pytest.raises(ValueError, match="0 filings"):
        dg.materialize(
            [assets.esef_filings_index_duckdb], resources=_resources(tmp_path)
        )

    rows = _fetch_filings_index(tmp_path)
    assert [row[0] for row in rows] == ["A-1", "A-2"]


def test_filings_index_non_empty_check_passes_on_populated_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_client(monkeypatch, [_record(fxo_id="A-1")])

    result = dg.materialize(
        [assets.esef_filings_index_duckdb, assets.filings_index_non_empty],
        resources=_resources(tmp_path),
    )

    assert result.success
    checks = result.get_asset_check_evaluations()
    assert len(checks) == 1
    assert checks[0].check_name == "filings_index_non_empty"
    assert checks[0].passed is True
    assert checks[0].metadata["row_count"].value == 1


def test_esef_filings_source_duckdb_path_defaults_under_data_root() -> None:
    assert assets.esef_filings_source_duckdb_path() == Path(
        "data/esef_filings_source.duckdb"
    )
    assert assets.esef_filings_source_duckdb_path(root="custom") == Path(
        "custom/esef_filings_source.duckdb"
    )
