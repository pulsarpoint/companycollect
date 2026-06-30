from __future__ import annotations

from typing import Any

import dagster as dg
import polars as pl
import pytest

from dagster_v3.defs.norway_brreg import financial_fetches
from dagster_v3.defs.norway_brreg.assets.financial_fetches import (
    FINANCIAL_FETCH_CANDIDATE_SCHEMA,
    norway_brreg_financial_fetches_snapshot_parquet,
    norway_brreg_financial_fetches_updates_parquet,
)


class FakeEntityStorage:
    def __init__(
        self,
        *,
        snapshot_frame: pl.DataFrame | None = None,
        update_frame: pl.DataFrame | None = None,
    ) -> None:
        self.snapshot_frame = snapshot_frame
        self.update_frame = update_frame
        self.snapshot_reads: list[str] = []
        self.update_reads: list[tuple[str, str]] = []

    def read_normalized_snapshot_table(self, table_name: str) -> pl.DataFrame:
        assert self.snapshot_frame is not None
        self.snapshot_reads.append(table_name)
        return self.snapshot_frame

    def read_normalized_update_table(
        self,
        partition_date: str,
        table_name: str,
    ) -> pl.DataFrame:
        assert self.update_frame is not None
        self.update_reads.append((partition_date, table_name))
        return self.update_frame


class FakeFinancialStorage:
    def __init__(
        self,
        raw_frames: dict[tuple[str, str], pl.DataFrame] | None = None,
    ) -> None:
        self.raw_frames = raw_frames or {}
        self.raw_exists_calls: list[tuple[str, str]] = []
        self.raw_read_calls: list[tuple[str, str]] = []
        self.raw_write_calls: list[tuple[str, str, bool, pl.DataFrame]] = []
        self.snapshot_fetches_frame: pl.DataFrame | None = None
        self.update_fetches: dict[str, pl.DataFrame] = {}
        self.update_candidates: dict[str, pl.DataFrame] = {}

    def raw_fetch_exists(self, org_number: str, accounts_year: str) -> bool:
        self.raw_exists_calls.append((org_number, accounts_year))
        return (org_number, accounts_year) in self.raw_frames

    def read_raw_fetch(self, org_number: str, accounts_year: str) -> pl.DataFrame:
        self.raw_read_calls.append((org_number, accounts_year))
        return self.raw_frames[(org_number, accounts_year)]

    def write_raw_fetch(
        self,
        org_number: str,
        accounts_year: str,
        frame: pl.DataFrame,
        *,
        overwrite: bool,
    ) -> str:
        self.raw_write_calls.append((org_number, accounts_year, overwrite, frame))
        if overwrite or (org_number, accounts_year) not in self.raw_frames:
            self.raw_frames[(org_number, accounts_year)] = frame
        return f"raw/{org_number}/{accounts_year}.parquet"

    def write_snapshot_fetches(self, frame: pl.DataFrame) -> str:
        self.snapshot_fetches_frame = frame
        return "snapshot/fetches.parquet"

    def write_update_fetches(self, partition_date: str, frame: pl.DataFrame) -> str:
        self.update_fetches[partition_date] = frame
        return f"updates/{partition_date}/fetches.parquet"

    def write_update_financial_candidates(
        self,
        partition_date: str,
        frame: pl.DataFrame,
    ) -> str:
        self.update_candidates[partition_date] = frame
        return f"updates/{partition_date}/candidates.parquet"


def test_snapshot_asset_reuses_existing_raw_fetch_without_calling_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_row = _success_fetch_row("923609016", "2025")
    financial_storage = FakeFinancialStorage(
        {("923609016", "2025"): pl.DataFrame([raw_row])}
    )

    def fail_fetch(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("snapshot asset should reuse existing raw fetch parquet")

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fail_fetch)

    result = norway_brreg_financial_fetches_snapshot_parquet(
        context=dg.build_asset_context(),
        norway_brreg_entity_storage=FakeEntityStorage(
            snapshot_frame=_no_companies_frame(
                [
                    {
                        "org_number": "923609016",
                        "name": "EQUINOR ASA",
                        "is_active": True,
                        "primary_website_url": "https://www.equinor.com",
                        "last_submitted_accounts_year": "2025",
                    }
                ]
            )
        ),
        norway_brreg_financial_storage=financial_storage,
    )

    assert financial_storage.raw_exists_calls == [("923609016", "2025")]
    assert financial_storage.raw_read_calls == [("923609016", "2025")]
    assert financial_storage.raw_write_calls == []
    assert _fetch_statuses(financial_storage.snapshot_fetches_frame) == [
        ("923609016", "success")
    ]
    assert result.metadata["candidate_count"] == 1
    assert result.metadata["reused_count"] == 1
    assert result.metadata["fetched_count"] == 0
    assert result.metadata["row_count"] == 1
    assert result.metadata["status_counts"] == {"success": 1}
    assert result.metadata["s3_key"] == "snapshot/fetches.parquet"


def test_snapshot_asset_fetches_missing_raw_and_writes_raw_and_aggregate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_calls: list[list[dict[str, Any]]] = []

    def fake_fetch(**kwargs: Any) -> list[dict[str, Any]]:
        orgs = list(kwargs["orgs"])
        fetch_calls.append(orgs)
        return [_success_fetch_row(orgs[0]["org_number"], orgs[0]["last_submitted_accounts_year"])]

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fake_fetch)
    financial_storage = FakeFinancialStorage()

    result = norway_brreg_financial_fetches_snapshot_parquet(
        context=dg.build_asset_context(),
        norway_brreg_entity_storage=FakeEntityStorage(
            snapshot_frame=_no_companies_frame(
                [
                    {
                        "org_number": "811685852",
                        "name": "SUCCESS AS",
                        "is_active": True,
                        "primary_website_url": None,
                        "last_submitted_accounts_year": "2024",
                    }
                ]
            )
        ),
        norway_brreg_financial_storage=financial_storage,
    )

    assert [[org["org_number"] for org in call] for call in fetch_calls] == [["811685852"]]
    assert [(org, year, overwrite) for org, year, overwrite, _frame in financial_storage.raw_write_calls] == [
        ("811685852", "2024", False)
    ]
    assert _fetch_statuses(financial_storage.raw_frames[("811685852", "2024")]) == [
        ("811685852", "success")
    ]
    assert _fetch_statuses(financial_storage.snapshot_fetches_frame) == [
        ("811685852", "success")
    ]
    assert result.metadata["candidate_count"] == 1
    assert result.metadata["reused_count"] == 0
    assert result.metadata["fetched_count"] == 1
    assert result.metadata["row_count"] == 1


def test_update_asset_writes_candidates_overwrites_raw_and_writes_partition_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fetch_calls: list[list[dict[str, Any]]] = []

    def fake_fetch(**kwargs: Any) -> list[dict[str, Any]]:
        orgs = list(kwargs["orgs"])
        fetch_calls.append(orgs)
        return [_success_fetch_row(orgs[0]["org_number"], orgs[0]["last_submitted_accounts_year"])]

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fake_fetch)
    old_row = _success_fetch_row("923609016", "2024")
    financial_storage = FakeFinancialStorage(
        {("923609016", "2024"): pl.DataFrame([old_row])}
    )

    result = norway_brreg_financial_fetches_updates_parquet(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        norway_brreg_entity_storage=FakeEntityStorage(
            update_frame=_no_companies_frame(
                [
                    {
                        "org_number": "923609016",
                        "name": "EQUINOR ASA",
                        "is_active": True,
                        "primary_website_url": "https://www.equinor.com",
                        "last_submitted_accounts_year": "2024",
                    }
                ]
            )
        ),
        norway_brreg_financial_storage=financial_storage,
    )

    assert [[org["org_number"] for org in call] for call in fetch_calls] == [["923609016"]]
    assert financial_storage.update_candidates["2026-06-30"].to_dicts() == [
        {
            "org_number": "923609016",
            "legal_name": "EQUINOR ASA",
            "website": "https://www.equinor.com",
            "last_submitted_accounts_year": "2024",
        }
    ]
    assert [(org, year, overwrite) for org, year, overwrite, _frame in financial_storage.raw_write_calls] == [
        ("923609016", "2024", True)
    ]
    assert _fetch_statuses(financial_storage.update_fetches["2026-06-30"]) == [
        ("923609016", "success")
    ]
    assert result.metadata["partition_date"] == "2026-06-30"
    assert result.metadata["candidate_s3_key"] == "updates/2026-06-30/candidates.parquet"
    assert result.metadata["s3_key"] == "updates/2026-06-30/fetches.parquet"
    assert result.metadata["candidate_count"] == 1
    assert result.metadata["fetched_count"] == 1


def test_empty_update_candidate_partition_writes_empty_parquets_and_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_fetch(**_kwargs: Any) -> list[dict[str, Any]]:
        raise AssertionError("empty update partitions should not call the API")

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fail_fetch)
    financial_storage = FakeFinancialStorage()

    result = norway_brreg_financial_fetches_updates_parquet(
        context=dg.build_asset_context(partition_key="2026-06-30"),
        norway_brreg_entity_storage=FakeEntityStorage(update_frame=_empty_no_companies_frame()),
        norway_brreg_financial_storage=financial_storage,
    )

    candidate_frame = financial_storage.update_candidates["2026-06-30"]
    fetch_frame = financial_storage.update_fetches["2026-06-30"]
    assert candidate_frame.to_dicts() == []
    assert candidate_frame.schema == FINANCIAL_FETCH_CANDIDATE_SCHEMA
    assert fetch_frame.to_dicts() == []
    assert fetch_frame.columns == list(financial_fetches.BRREG_FINANCIAL_FETCHES_COLUMNS)
    assert result.metadata["candidate_count"] == 0
    assert result.metadata["row_count"] == 0
    assert result.metadata["status_counts"] == {}


def test_retryable_fetch_status_raises_after_writing_diagnostics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_fetch(**kwargs: Any) -> list[dict[str, Any]]:
        org = list(kwargs["orgs"])[0]
        return [
            _failure_fetch_row(
                org["org_number"],
                org["last_submitted_accounts_year"],
                fetch_status=financial_fetches.FINANCIAL_FETCH_STATUS_SERVER_ERROR,
            )
        ]

    monkeypatch.setattr(financial_fetches, "fetch_financial_rows_for_orgs", fake_fetch)
    financial_storage = FakeFinancialStorage()

    with pytest.raises(RuntimeError):
        norway_brreg_financial_fetches_snapshot_parquet(
            context=dg.build_asset_context(),
            norway_brreg_entity_storage=FakeEntityStorage(
                snapshot_frame=_no_companies_frame(
                    [
                        {
                            "org_number": "814115232",
                            "name": "SERVER AS",
                            "is_active": True,
                            "primary_website_url": None,
                            "last_submitted_accounts_year": "2024",
                        }
                    ]
                )
            ),
            norway_brreg_financial_storage=financial_storage,
        )

    assert _fetch_statuses(financial_storage.raw_frames[("814115232", "2024")]) == [
        ("814115232", "server_error")
    ]
    assert _fetch_statuses(financial_storage.snapshot_fetches_frame) == [
        ("814115232", "server_error")
    ]


def _no_companies_frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "org_number": pl.Utf8,
            "name": pl.Utf8,
            "is_active": pl.Boolean,
            "primary_website_url": pl.Utf8,
            "last_submitted_accounts_year": pl.Utf8,
        },
    )


def _empty_no_companies_frame() -> pl.DataFrame:
    return _no_companies_frame([])


def _success_fetch_row(org_number: str, accounts_year: str) -> dict[str, Any]:
    return financial_fetches.financial_fetch_success_row(
        org={
            "org_number": org_number,
            "legal_name": f"{org_number} AS",
            "website": "",
            "last_submitted_accounts_year": accounts_year,
        },
        source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}",
        payload=[{"id": int(org_number[-3:])}],
        source_run_id="run-1",
        source_line_number=1,
        status_code=200,
        fetched_at="2026-06-30T00:00:00.000Z",
        attempt_count=1,
    )


def _failure_fetch_row(
    org_number: str,
    accounts_year: str,
    *,
    fetch_status: str,
) -> dict[str, Any]:
    return financial_fetches.financial_fetch_failure_row(
        org={
            "org_number": org_number,
            "legal_name": f"{org_number} AS",
            "website": "",
            "last_submitted_accounts_year": accounts_year,
        },
        source_url=f"https://data.brreg.no/regnskapsregisteret/regnskap/{org_number}",
        source_run_id="run-1",
        source_line_number=1,
        status_code=500,
        fetch_status=fetch_status,
        error_type="HTTPStatusError",
        error_message="HTTP 500",
        fetched_at="2026-06-30T00:00:00.000Z",
        attempt_count=1,
        raw_response="server error",
    )


def _fetch_statuses(frame: pl.DataFrame | None) -> list[tuple[str, str]]:
    assert frame is not None
    return [
        (row["org_number"], row["fetch_status"])
        for row in frame.select(["org_number", "fetch_status"]).to_dicts()
    ]
