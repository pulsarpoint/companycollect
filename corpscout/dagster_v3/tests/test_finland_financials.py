from pathlib import Path

import pytest
import requests
import duckdb

from dagster_v3.defs.finland_financials import metrics, tables

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "clickhouse" / "migrations" / "000009_corpscout_fi_financial_metrics.up.sql"
).read_text()

# Synthetic Finnish plain dimensional XBRL: revenue (md103+x673, P&L duration) and
# total_assets (mi53+x360, balance-sheet instant).
STATEMENT = """<?xml version="1.0"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance"
            xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
            xmlns:fi_met="http://www.suomi.fi/xbrl/crr/dict/met"
            xmlns:fi_dim="http://www.suomi.fi/xbrl/crr/dict/dim"
            xmlns:fi_MC="http://www.suomi.fi/xbrl/crr/dict/dom/MC">
  <xbrli:context id="c_dur"><xbrli:entity>
    <xbrli:identifier scheme="http://ytj.fi">0202458-3</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:startDate>2023-10-01</xbrli:startDate>
    <xbrli:endDate>2024-09-30</xbrli:endDate></xbrli:period>
    <xbrli:scenario><xbrldi:explicitMember dimension="fi_dim:d">fi_MC:x673</xbrldi:explicitMember>
    </xbrli:scenario></xbrli:context>
  <xbrli:context id="c_inst"><xbrli:entity>
    <xbrli:identifier scheme="http://ytj.fi">0202458-3</xbrli:identifier></xbrli:entity>
    <xbrli:period><xbrli:instant>2024-09-30</xbrli:instant></xbrli:period>
    <xbrli:scenario><xbrldi:explicitMember dimension="fi_dim:d">fi_MC:x360</xbrldi:explicitMember>
    </xbrli:scenario></xbrli:context>
  <fi_met:md103 contextRef="c_dur" unitRef="ISO4217_EUR">43384.81</fi_met:md103>
  <fi_met:mi53 contextRef="c_inst" unitRef="ISO4217_EUR">52897.68</fi_met:mi53>
</xbrli:xbrl>
"""


def test_metric_map_loads():
    mapping = metrics.load_metric_map()
    assert mapping[("fi_met:md103", "fi_MC:x673")] == "revenue"
    assert mapping[("fi_met:mi53", "fi_MC:x360")] == "total_assets"


def test_extract_statement_metrics():
    from datetime import date
    from decimal import Decimal

    mapping = metrics.load_metric_map()
    result = metrics.extract_statement_metrics(STATEMENT.encode("utf-8"), mapping)
    assert result["period_end"] == date(2024, 9, 30)
    assert result["period_start"] == date(2023, 10, 1)
    assert result["currency"] == "EUR"
    assert result["metrics"]["revenue"] == Decimal("43384.81")
    assert result["metrics"]["total_assets"] == Decimal("52897.68")
    assert result["metrics"]["equity"] is None


def test_export_columns_match_migration():
    assert "CREATE TABLE IF NOT EXISTS corpscout.fi_financial_metrics" in MIGRATION
    for column in tables.FI_FINANCIAL_METRICS_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration 000009"
    for metric in tables.FI_METRIC_NAMES:
        assert f"{metric}_amount_original" in tables.FI_FINANCIAL_METRICS_COLUMNS
        assert f"{metric}_amount_usd" in tables.FI_FINANCIAL_METRICS_COLUMNS


def test_registration_partition_windows():
    from dagster_v3.defs.finland_financials import incremental

    assert incremental.month_window("2025-06-01") == ("2025-06-01", "2025-06-30")
    assert incremental.month_window("2026-05-01") == ("2026-05-01", "2026-05-31")
    assert incremental.day_window("2026-06-01") == ("2026-06-01", "2026-06-01")


def test_jobs_and_schedule_registered():
    import datetime as dt

    import dagster as dg
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    inc = {
        k.path[-1]
        for k in repo.get_job("finland_financials_incremental_job").asset_layer.executable_asset_keys
    }
    assert inc == {"finland_financials_incremental"}
    assert type(repo.get_job("finland_financials_incremental_job").partitions_def).__name__ == (
        "DailyPartitionsDefinition"
    )
    bf = {
        k.path[-1]
        for k in repo.get_job("finland_financials_backfill_job").asset_layer.executable_asset_keys
    }
    assert bf == {"finland_financials_backfill"}
    assert type(repo.get_job("finland_financials_backfill_job").partitions_def).__name__ == (
        "MonthlyPartitionsDefinition"
    )
    sch = repo.get_schedule_def("finland_financials_incremental_schedule")
    assert sch.cron_schedule == "0 6 * * *"

    asset_graph = repo.asset_graph
    backfill_partitions = asset_graph.get(dg.AssetKey("finland_financials_backfill")).partitions_def
    backfill_keys = backfill_partitions.get_partition_keys(
        current_time=dt.datetime(2026, 6, 28)
    )
    assert backfill_keys[0] == "2025-06-01"
    assert "2026-05-01" in backfill_keys
    assert "2026-06-01" not in backfill_keys

    incremental_partitions = asset_graph.get(
        dg.AssetKey("finland_financials_incremental")
    ).partitions_def
    incremental_keys = incremental_partitions.get_partition_keys(
        current_time=dt.datetime(2026, 6, 28)
    )
    assert incremental_keys[0] == "2026-06-01"


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        payload: dict | None = None,
        content: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content
        self.headers = headers or {}

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code < 400:
            return
        error = requests.HTTPError(f"{self.status_code} Client Error")
        error.response = self
        raise error


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, *, params: dict, timeout: int) -> FakeResponse:
        self.calls.append((url, params))
        if not self.responses:
            raise AssertionError("unexpected request")
        return self.responses.pop(0)


def test_discover_reports_pages_until_empty_without_truncation():
    session = FakeSession(
        [
            FakeResponse(
                payload={
                    "financials": [
                        {"businessId": "a", "financialDate": "2025-12-31"},
                        {"businessId": "b", "financialDate": "2025-12-31"},
                    ]
                }
            ),
            FakeResponse(payload={"financials": [{"businessId": "c", "financialDate": "2025-12-31"}]}),
            FakeResponse(payload={"financials": []}),
        ]
    )

    reports = metrics.discover_reports(
        registered_date_start="2026-06-01",
        registered_date_end="2026-06-30",
        session=session,
        sleep=lambda _: None,
    )

    assert [report["businessId"] for report in reports] == ["a", "b", "c"]
    assert [call[1]["page"] for call in session.calls] == [1, 2, 3]


def test_build_fetches_discovered_financial_date_even_when_period_is_older(
    tmp_path,
    monkeypatch,
):
    session = FakeSession(
        [
            FakeResponse(
                payload={
                    "financials": [
                        {"businessId": "0116158-6", "financialDate": "2025-12-31"}
                    ]
                }
            ),
            FakeResponse(payload={"financials": []}),
            FakeResponse(content=b"<xbrl/>"),
        ]
    )
    monkeypatch.setattr(metrics, "load_metric_map", lambda: {})
    monkeypatch.setattr(
        metrics,
        "extract_statement_metrics",
        lambda _body, _metric_map: {
            "period_start": None,
            "period_end": None,
            "currency": "EUR",
            "metrics": {name: None for name in tables.FI_METRIC_NAMES},
            "numeric": 1,
            "mapped": 1,
        },
    )

    with duckdb.connect(str(tmp_path / "fi.duckdb")) as connection:
        counts = metrics.build_finland_financials(
            connection=connection,
            source_run_id="run",
            registered_date_start="2026-06-01",
            registered_date_end="2026-06-30",
            session=session,
            sleep=lambda _: None,
        )

    assert counts["statements"] == 1
    assert session.calls[-1][1] == {
        "businessId": "0116158-6",
        "financialDate": "2025-12-31",
    }


def test_statement_fetch_429_retries_then_succeeds(tmp_path, monkeypatch):
    session = FakeSession(
        [
            FakeResponse(payload={"financials": [{"businessId": "a", "financialDate": "2025-12-31"}]}),
            FakeResponse(payload={"financials": []}),
            FakeResponse(status_code=429),
            FakeResponse(content=b"<xbrl/>"),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(metrics, "load_metric_map", lambda: {})
    monkeypatch.setattr(
        metrics,
        "extract_statement_metrics",
        lambda _body, _metric_map: {
            "period_start": None,
            "period_end": None,
            "currency": "EUR",
            "metrics": {name: None for name in tables.FI_METRIC_NAMES},
            "numeric": 1,
            "mapped": 1,
        },
    )

    with duckdb.connect(str(tmp_path / "fi.duckdb")) as connection:
        counts = metrics.build_finland_financials(
            connection=connection,
            source_run_id="run",
            registered_date_start="2026-06-01",
            registered_date_end="2026-06-01",
            session=session,
            sleep=sleeps.append,
        )

    assert counts["statements"] == 1
    assert sleeps == [30]


def test_statement_fetch_uses_retry_after_header(tmp_path, monkeypatch):
    session = FakeSession(
        [
            FakeResponse(payload={"financials": [{"businessId": "a", "financialDate": "2025-12-31"}]}),
            FakeResponse(payload={"financials": []}),
            FakeResponse(status_code=429, headers={"Retry-After": "7"}),
            FakeResponse(content=b"<xbrl/>"),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(metrics, "load_metric_map", lambda: {})
    monkeypatch.setattr(
        metrics,
        "extract_statement_metrics",
        lambda _body, _metric_map: {
            "period_start": None,
            "period_end": None,
            "currency": "EUR",
            "metrics": {name: None for name in tables.FI_METRIC_NAMES},
            "numeric": 1,
            "mapped": 1,
        },
    )

    with duckdb.connect(str(tmp_path / "fi.duckdb")) as connection:
        metrics.build_finland_financials(
            connection=connection,
            source_run_id="run",
            registered_date_start="2026-06-01",
            registered_date_end="2026-06-01",
            session=session,
            sleep=sleeps.append,
        )

    assert sleeps == [7]


def test_persistent_statement_fetch_429_fails_partition(tmp_path, monkeypatch):
    session = FakeSession(
        [
            FakeResponse(payload={"financials": [{"businessId": "a", "financialDate": "2025-12-31"}]}),
            FakeResponse(payload={"financials": []}),
            FakeResponse(status_code=429),
            FakeResponse(status_code=429),
            FakeResponse(status_code=429),
            FakeResponse(status_code=429),
            FakeResponse(status_code=429),
            FakeResponse(status_code=429),
        ]
    )
    sleeps: list[float] = []
    monkeypatch.setattr(metrics, "load_metric_map", lambda: {})

    with duckdb.connect(str(tmp_path / "fi.duckdb")) as connection:
        with pytest.raises(requests.HTTPError):
            metrics.build_finland_financials(
                connection=connection,
                source_run_id="run",
                registered_date_start="2026-06-01",
                registered_date_end="2026-06-01",
                session=session,
                sleep=sleeps.append,
            )

    assert sleeps == [30, 60, 120, 240, 480]
