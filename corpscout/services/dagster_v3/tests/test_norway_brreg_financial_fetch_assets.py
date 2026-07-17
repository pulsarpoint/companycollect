from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

import dagster as dg
import polars as pl
import pytest

from dagster_v3.defs.norway_brreg_financial import financial_fetches
from dagster_v3.defs.norway_brreg_financial.assets import financial_fetches as assets
from dagster_v3.defs.norway_brreg_financial.assets.financial_fetches import (
    daily_financial_report_candidates,
    norway_brreg_financial_responses_updates_json,
    norway_brreg_financial_responses_updates_parquet,
    parse_daily_financial_report_candidates,
)
from dagster_v3.defs.norway_brreg_financial.financial_storage import (
    financial_update_response_index_object_key,
    financial_update_response_partition_prefix,
)


class FakeAnnouncementPage:
    def __init__(self, page_html: str) -> None:
        self.page_html = page_html
        self.goto_calls: list[tuple[str, str]] = []
        self.evaluate_calls: list[str] = []

    def goto(self, url: str, *, wait_until: str) -> None:
        self.goto_calls.append((url, wait_until))

    def evaluate(self, script: str) -> str:
        self.evaluate_calls.append(script)
        return self.page_html


class FakeAnnouncementBrowser:
    def __init__(self, page: FakeAnnouncementPage) -> None:
        self.page = page
        self.closed = False

    def new_page(self) -> FakeAnnouncementPage:
        return self.page

    def close(self) -> None:
        self.closed = True


class FakeFinancialStorage:
    def __init__(self) -> None:
        self.writes: list[tuple[str, pl.DataFrame]] = []

    def write_update_response_index(
        self,
        partition_date: str,
        frame: pl.DataFrame,
    ) -> str:
        self.writes.append((partition_date, frame))
        return financial_update_response_index_object_key(partition_date)


def test_update_asset_dependencies_form_json_then_parquet_graph() -> None:
    json_key = dg.AssetKey("norway_brreg_financial_responses_updates_json")
    parquet_key = dg.AssetKey("norway_brreg_financial_responses_updates_parquet")

    assert norway_brreg_financial_responses_updates_json.asset_deps[json_key] == set()
    assert (
        json_key
        in (norway_brreg_financial_responses_updates_parquet.asset_deps[parquet_key])
    )


def test_parse_daily_financial_report_candidates_deduplicates_companies() -> None:
    page_html = """
        <html><body>
          <a href="hent_en.jsp?kid=20260000000002&amp;sokeverdi=923609016&amp;spraak=nb">
            Godkjente årsregnskap
          </a>
          <a href="hent_en.jsp?kid=20260000000001&amp;sokeverdi=918572805&amp;spraak=nb">
            Godkjente årsregnskap
          </a>
          <a href="hent_en.jsp?kid=20260000000003&amp;sokeverdi=923609016&amp;spraak=nb">
            Godkjente årsregnskap
          </a>
          <a href="another-page.jsp?sokeverdi=999999999">Unrelated link</a>
        </body></html>
    """

    assert parse_daily_financial_report_candidates(page_html) == [
        {"org_number": "918572805"},
        {"org_number": "923609016"},
    ]


def test_daily_financial_report_candidates_searches_only_partition_date() -> None:
    page = FakeAnnouncementPage(
        '<a href="hent_en.jsp?kid=20260000000001&amp;sokeverdi=923609016">report</a>'
    )
    browser = FakeAnnouncementBrowser(page)

    candidates = daily_financial_report_candidates(
        "2026-07-16",
        launcher=lambda: browser,
    )

    [(url, wait_until)] = page.goto_calls
    assert url == (
        "https://w2.brreg.no/kunngjoring/kombisok.jsp?"
        "datoFra=16.07.2026&datoTil=16.07.2026&id_region=0"
        "&id_niva1=70&id_niva2=-+-+-&id_bransje1=0"
    )
    assert urlparse(url).path == "/kunngjoring/kombisok.jsp"
    assert wait_until == "networkidle"
    assert page.evaluate_calls == ["() => document.documentElement.outerHTML"]
    assert browser.closed
    assert candidates == [{"org_number": "923609016"}]


def test_update_json_uses_daily_announcements_and_calls_json_pipeline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def fake_daily_candidates(partition_date: str) -> list[dict[str, str]]:
        captured["searched_partition_date"] = partition_date
        return [{"org_number": "923609016"}]

    def fake_materialize(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "candidate_count": 1,
            "reused_count": 0,
            "downloaded_count": 1,
            "status_counts": {"success": 1},
            "partition_prefix": kwargs["partition_prefix"],
        }

    monkeypatch.setattr(
        assets, "daily_financial_report_candidates", fake_daily_candidates
    )
    monkeypatch.setattr(assets, "materialize_response_json_partition", fake_materialize)

    result = norway_brreg_financial_responses_updates_json(
        context=dg.build_asset_context(partition_key="2026-07-16"),
        norway_brreg_financial_storage=object(),
    )

    assert captured["searched_partition_date"] == "2026-07-16"
    assert captured["candidates"] == [{"org_number": "923609016"}]
    assert captured["partition_prefix"] == (
        "norway_brreg/financial/responses/updates/date=2026-07-16/"
    )
    assert result.metadata["downloaded_count"] == 1


def test_update_parquet_writes_verified_metadata_without_raw_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    storage = FakeFinancialStorage()
    frame = financial_fetches.financial_fetches_frame(
        [
            financial_fetches.response_record(
                org={"org_number": "923609016"},
                source_url="https://example.test/923609016",
                source_run_id="run-1",
                source_line_number=1,
                fetch_status="success",
                http_status=200,
                error_type="",
                error_message="",
                attempt_count=1,
                fetched_at="2026-07-17T00:00:00.000Z",
                source_object_key="responses/org=923609016/response.json",
                source_payload_hash="a" * 64,
            )
        ]
    )
    monkeypatch.setattr(
        assets,
        "verified_response_index_frame",
        lambda **kwargs: (
            frame,
            {
                "candidate_count": 1,
                "row_count": 1,
                "status_counts": {"success": 1},
                "success_manifest_key": f"{kwargs['partition_prefix']}_SUCCESS.json",
            },
        ),
    )

    result = norway_brreg_financial_responses_updates_parquet(
        context=dg.build_asset_context(partition_key="2026-07-16"),
        norway_brreg_financial_storage=storage,
    )

    [(partition_date, written_frame)] = storage.writes
    assert partition_date == "2026-07-16"
    assert "raw_response" not in written_frame.columns
    assert result.metadata["s3_key"] == (
        "norway_brreg/financial/response_index/updates/"
        "date=2026-07-16/responses.parquet"
    )
    assert financial_update_response_partition_prefix(partition_date).endswith(
        "date=2026-07-16/"
    )
