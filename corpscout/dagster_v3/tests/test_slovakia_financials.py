import datetime as dt
from pathlib import Path
from types import SimpleNamespace

import duckdb

from dagster_v3.defs.slovakia_financials import incremental, metrics, tables

MIG_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"
METRICS_MIGRATION = (MIG_DIR / "000043_corpscout_sk_financial_metrics.up.sql").read_text()

# --- faithful POD fixture (4-col assets table + 2-col pasíva/P&L) -------------
POD_TEMPLATE = {
    "nazov": "Úč POD",
    "tabulky": [
        {"riadky": [{"cisloRiadku": 1}]},                                    # aktíva
        {"riadky": [{"cisloRiadku": 80}, {"cisloRiadku": 101}]},             # pasíva
        {"riadky": [{"cisloRiadku": 1}, {"cisloRiadku": 56}, {"cisloRiadku": 61}]},  # P&L
    ],
}
POD_OBSAH = {
    "tabulky": [
        {"data": ["100", "10", "880", "800"]},                 # 1×4: total_assets=index2=880
        {"data": ["500", "490", "300", "310"]},                # 2×2: equity=500, liabilities=300
        {"data": ["2000", "1900", "150", "140", "120", "110"]},  # 3×2: rev=2000,pretax=150,net=120
    ],
}


class FakeRuzClient:
    """Canned RÚZ responses for the build path."""

    def __init__(self):
        self.statements = {
            5001: {
                "id": 5001, "idUJ": 9001, "idUctovnychVykazov": [7001],
                "obdobieOd": "2023-01", "obdobieDo": "2023-12",
                "datumZostaveniaK": "2023-12-31", "typ": "Riadna",
                "datumPodania": "2024-03-15", "datumSchvalenia": "2024-04-01",
            },
            5002: {"id": 5002, "stav": "ZMAZANÉ"},
        }
        self.entities = {9001: {"id": 9001, "ico": "36355232", "dic": "2022178433"}}
        self.reports = {
            7001: {"id": 7001, "idSablony": 699, "pristupnostDat": "Verejné", "obsah": POD_OBSAH}
        }
        self.templates = {699: POD_TEMPLATE}

    def statement_ids(self, *, changed_since, after_id, max_records):
        ids = [i for i in sorted(self.statements) if i > after_id][:max_records]
        return ids, False

    def statement(self, sid): return self.statements[sid]
    def entity(self, eid): return self.entities[eid]
    def report(self, rid): return self.reports[rid]
    def template(self, tid): return self.templates[tid]


class FakeRates:
    def usd_rates(self, requests):
        from decimal import Decimal
        return {
            (r.currency, r.rate_date): SimpleNamespace(
                rate=Decimal("1.1"), rate_date=dt.date.fromisoformat(r.rate_date)
            )
            for r in requests
        }


def test_template_family():
    assert metrics.template_family("Úč POD v.2014") == "Úč POD"
    assert metrics.template_family("Úč MUJ") == "Úč MUJ"
    assert metrics.template_family("Úč NUJ") is None
    assert metrics.template_family(None) is None


def test_extract_report_metrics_decodes_both_column_widths():
    out, family = metrics.extract_report_metrics(POD_OBSAH, POD_TEMPLATE)
    assert family == "Úč POD"
    assert out == {
        "total_assets": 880.0, "equity": 500.0, "liabilities": 300.0,
        "revenue": 2000.0, "pretax_result": 150.0, "net_result": 120.0,
    }


def test_extract_report_metrics_unmapped_family():
    out, family = metrics.extract_report_metrics(POD_OBSAH, {"nazov": "Úč NUJ", "tabulky": []})
    assert out is None and family is None


def test_process_statement_skips_deleted():
    client = FakeRuzClient()
    assert metrics.process_statement(client, 5002, entity_cache={}, template_cache={}) is None


def test_process_statement_resolves_entity_and_metrics():
    client = FakeRuzClient()
    row = metrics.process_statement(client, 5001, entity_cache={}, template_cache={})
    assert row["ico"] == "36355232" and row["ruz_entity_id"] == "9001"
    assert row["template_name"] == "Úč POD" and row["template_mapped"] == 1
    assert row["fiscal_year"] == 2023 and row["period_end"] == "2023-12-31"
    assert row["metrics"]["net_result"] == 120.0 and row["mapped_metric_count"] == 6


def test_build_writes_metrics_table(tmp_path):
    db = tmp_path / "skfin.duckdb"
    counts = metrics.build_slovakia_financials(
        database_path=db, source_run_id="r1", after_id=5000, max_statements=10,
        client=FakeRuzClient(), request_delay_seconds=0,
    )
    assert counts["statements"] == 1 and counts["mapped_statements"] == 1
    assert counts["skipped"] == 1 and counts["last_id"] == 5002
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchall()]
        assert cols == list(tables.SK_FINANCIAL_METRICS_COLUMNS)
        row = con.execute(
            f"select ico, statement_id, fiscal_year, period_end_date, total_assets_amount_original, "
            f"total_assets_amount_usd, net_result_amount_original, currency_original "
            f"from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()
    assert row[0] == "36355232" and row[1] == "5001" and row[2] == 2023
    assert row[3] == dt.date(2023, 12, 31)
    assert float(row[4]) == 880.0 and row[5] is None  # usd not yet filled
    assert float(row[6]) == 120.0 and row[7] == "EUR"


def test_usd_conversion_fills_amounts(tmp_path):
    db = tmp_path / "skfin.duckdb"
    metrics.build_slovakia_financials(
        database_path=db, source_run_id="r1", after_id=5000, max_statements=10,
        client=FakeRuzClient(), request_delay_seconds=0,
    )
    counts = metrics.apply_slovakia_usd_conversion(database_path=db, exchange_rates=FakeRates())
    assert counts["rows_converted"] == 1
    with duckdb.connect(str(db), read_only=True) as con:
        usd, fx = con.execute(
            f"select total_assets_amount_usd, fx_rate_to_usd "
            f"from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()
    assert float(usd) == 968.0 and float(fx) == 1.1  # 880 * 1.1


def test_cursor_roundtrip(tmp_path):
    db = tmp_path / "skfin.duckdb"
    assert incremental.read_cursor(db) == 0
    incremental.write_cursor(db, 12345)
    assert incremental.read_cursor(db) == 12345
    incremental.write_cursor(db, 99999)
    assert incremental.read_cursor(db) == 99999


def test_metrics_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_METRICS_TABLE}" in METRICS_MIGRATION
    for column in tables.SK_FINANCIAL_METRICS_COLUMNS:
        assert f"    {column} " in METRICS_MIGRATION, f"missing {column} in 000043"
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in METRICS_MIGRATION
    assert "ORDER BY (ico, statement_id)" in METRICS_MIGRATION


def test_incremental_job_and_schedule():
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    sch = repo.get_schedule_def("slovakia_financials_incremental_schedule")
    assert sch.cron_schedule == "0 5 * * *"
    assert sch.job.name == "slovakia_financials_incremental_job"
    keys = {
        k.path[-1]
        for k in repo.get_job("slovakia_financials_incremental_job").asset_layer.executable_asset_keys
    }
    assert keys == {"slovakia_financials_incremental"}
