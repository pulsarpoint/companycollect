from contextlib import contextmanager

import dagster as dg
import pytest

from dagster_v3.defs.provider_recon import assets
from dagster_v3.defs.provider_recon.resource import ProviderReconResource


class FakeRecon(ProviderReconResource):
    final: dict

    def start_collect(self, providers=()):
        return {"run_id": "20260928T031200Z-collect", "status": "running"}

    def wait_for_run(self, run_id, **_):
        return self.final


class FakeClient:
    def __init__(self, documents: int) -> None:
        self.documents = documents
        self.statements: list[str] = []

    def execute(self, query, params=None, **_):
        self.statements.append(query)
        if "system.tables" in query:
            return [(name,) for name in assets.tables.ALL_TABLES]
        if query.startswith("SELECT count()"):
            return [(self.documents,)]
        return []


class FakeClickhouse:
    def __init__(self, documents: int) -> None:
        self.client = FakeClient(documents)

    @contextmanager
    def get_connection(self):
        yield self.client


def succeeded(issues=()):
    return {"run_id": "20260928T031200Z-collect", "status": "succeeded", "changed": ["aws"], "unchanged_count": 36, "issues": list(issues)}


def test_documents_asset_reports_run_and_passes_check() -> None:
    recon = FakeRecon(final=succeeded())
    with dg.build_asset_context() as context:
        result = assets.provider_recon_documents(context, provider_recon=recon, clickhouse=FakeClickhouse(37))
    assert result.metadata["run_id"] == "20260928T031200Z-collect"
    assert result.metadata["documents"] == 37
    assert result.check_results[0].passed


def test_documents_asset_warns_on_feed_issues() -> None:
    issue = {"slug": "aws", "collector": "aws_ip_ranges", "status": "stale", "error": "status 503"}
    recon = FakeRecon(final=succeeded([issue]))
    with dg.build_asset_context() as context:
        result = assets.provider_recon_documents(context, provider_recon=recon, clickhouse=FakeClickhouse(37))
    check = result.check_results[0]
    assert not check.passed and check.severity == dg.AssetCheckSeverity.WARN
    assert "aws aws_ip_ranges stale: status 503" in check.metadata["issues"].value


def test_documents_asset_fails_when_run_fails() -> None:
    recon = FakeRecon(final={"run_id": "x", "status": "failed", "error": "put failed", "changed": [], "unchanged_count": 0, "issues": []})
    with dg.build_asset_context() as context, pytest.raises(dg.Failure, match="put failed"):
        assets.provider_recon_documents(context, provider_recon=recon, clickhouse=FakeClickhouse(37))


def test_clickhouse_asset_refuses_empty_mapping() -> None:
    with dg.build_asset_context() as context, pytest.raises(ValueError, match="no documents"):
        assets.provider_recon_clickhouse(context, clickhouse=FakeClickhouse(0))


def test_clickhouse_asset_runs_every_load_statement() -> None:
    ch = FakeClickhouse(37)
    with dg.build_asset_context() as context:
        result = assets.provider_recon_clickhouse(context, clickhouse=ch)
    inserts = [s for s in ch.client.statements if s.startswith("INSERT INTO")]
    assert len(inserts) == 3
    assert result.metadata["documents"] == 37


def test_schedule_is_created_stopped() -> None:
    assert assets.provider_recon_daily.default_status == dg.DefaultScheduleStatus.STOPPED
    assert assets.provider_recon_daily.cron_schedule == "12 3 * * *"
