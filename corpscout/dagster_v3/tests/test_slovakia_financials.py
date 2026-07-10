import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import duckdb

from dagster_v3.defs.slovakia_financials import incremental, metrics, raw_store, tables

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
        self.template_calls = 0

    def statement_ids(self, *, changed_since, after_id, max_records):
        ids = [i for i in sorted(self.statements) if i > after_id][:max_records]
        return ids, False

    def statement(self, sid): return self.statements[sid]
    def entity(self, eid): return self.entities[eid]
    def report(self, rid): return self.reports[rid]

    def template(self, tid):
        self.template_calls += 1
        return self.templates[tid]


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


def _seed_raw_batch(store) -> str:
    raw_store.write_template(store, 699, POD_TEMPLATE)
    deleted = {"statement_id": 5002, "statement": {"id": 5002, "stav": "ZMAZANÉ"},
               "entity": None, "reports": []}
    return raw_store.write_statement_batch(
        store, after_id=5000, last_id=5002, bundles=[_pod_bundle(), deleted],
        manifest={"source_run_id": "r0", "statement_count": 2},
    )


def test_build_metrics_from_batches_writes_rows(tmp_path):
    store = FakeObjectStore()
    _seed_raw_batch(store)
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        counts = metrics.build_metrics_from_batches(
            connection=con, object_store=store, source_run_id="r1"
        )
    assert counts["batches_processed"] == 1 and counts["statements"] == 1
    assert counts["skipped"] == 1 and counts["table_statements"] == 1
    with duckdb.connect(str(db), read_only=True) as con:
        cols = [r[0] for r in con.execute(
            f"describe {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchall()]
        assert cols == [*tables.SK_FINANCIAL_METRICS_COLUMNS, "source_batch_key"]
        row = con.execute(
            f"select ico, statement_id, fiscal_year, period_end_date, "
            f"total_assets_amount_original, total_assets_amount_usd, "
            f"net_result_amount_original, currency_original, source_batch_key "
            f"from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()
    assert row[0] == "36355232" and row[1] == "5001" and row[2] == 2023
    assert row[3] == dt.date(2023, 12, 31)
    assert float(row[4]) == 880.0 and row[5] is None  # usd not yet filled
    assert float(row[6]) == 120.0 and row[7] == "EUR"
    assert row[8] == raw_store.statement_batch_key(5000, 5002)


def test_build_metrics_from_batches_is_incremental_and_idempotent(tmp_path):
    store = FakeObjectStore()
    _seed_raw_batch(store)
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        metrics.build_metrics_from_batches(connection=con, object_store=store, source_run_id="r1")
        # second run: batch already recorded as processed → no-op
        counts = metrics.build_metrics_from_batches(
            connection=con, object_store=store, source_run_id="r2"
        )
        assert counts["batches_processed"] == 0 and counts["table_statements"] == 1
        # a second batch appends without touching the first
        bundle = _pod_bundle()
        bundle["statement_id"] = 5003
        bundle["statement"]["id"] = 5003
        raw_store.write_statement_batch(
            store, after_id=5002, last_id=5003, bundles=[bundle],
            manifest={"source_run_id": "r3", "statement_count": 1},
        )
        counts = metrics.build_metrics_from_batches(
            connection=con, object_store=store, source_run_id="r3"
        )
        assert counts["batches_processed"] == 1 and counts["table_statements"] == 2


def test_usd_conversion_fills_amounts(tmp_path):
    store = FakeObjectStore()
    _seed_raw_batch(store)
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        metrics.build_metrics_from_batches(connection=con, object_store=store, source_run_id="r1")
        counts = metrics.apply_slovakia_usd_conversion(connection=con, exchange_rates=FakeRates())
    assert counts["rows_converted"] == 1
    with duckdb.connect(str(db), read_only=True) as con:
        usd, fx = con.execute(
            f"select total_assets_amount_usd, fx_rate_to_usd "
            f"from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}"
        ).fetchone()
    assert float(usd) == 968.0 and float(fx) == 1.1  # 880 * 1.1


def test_cursor_roundtrip(tmp_path):
    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        assert incremental.read_cursor(con) == 0
        incremental.write_cursor(con, 12345)
        assert incremental.read_cursor(con) == 12345
        incremental.write_cursor(con, 99999)
        assert incremental.read_cursor(con) == 99999


def test_clickhouse_export_refuses_empty_table(tmp_path):
    import pytest

    from dagster_v3.defs.slovakia_financials.clickhouse import (
        export_slovakia_financials_clickhouse_metrics,
    )

    db = tmp_path / "skfin.duckdb"
    with duckdb.connect(str(db)) as con:
        metrics._ensure_metrics_tables(con)  # empty table exists
        with pytest.raises(ValueError, match="0 rows"):
            export_slovakia_financials_clickhouse_metrics(
                duckdb_connection=con, clickhouse=None
            )


def test_source_batch_key_not_exported():
    assert "source_batch_key" not in tables.SK_FINANCIAL_METRICS_COLUMNS


def test_metrics_export_columns_match_migration():
    assert f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_METRICS_TABLE}" in METRICS_MIGRATION
    for column in tables.SK_FINANCIAL_METRICS_COLUMNS:
        assert f"    {column} " in METRICS_MIGRATION, f"missing {column} in 000043"
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in METRICS_MIGRATION
    assert "ORDER BY (ico, statement_id)" in METRICS_MIGRATION


class FakeObjectStore:
    """In-memory stand-in for ObjectStoreResource (mirrors the Sweden test fake)."""

    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def ensure_bucket(self, bucket: str | None = None) -> None:
        pass

    def write_bytes(self, key: str, body: bytes, bucket: str | None = None) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        assert bucket is not None
        return [k for (b, k) in self.objects if b == bucket and k.startswith(prefix)]


def test_batch_key_roundtrip():
    key = raw_store.statement_batch_key(0, 5002)
    assert key == (
        "slovakia_financials/raw_statements/batch-000000000000-000000005002/statements.ndjson"
    )
    assert raw_store.parse_batch_id_range(key) == (0, 5002)
    assert raw_store.parse_batch_id_range("slovakia_financials/raw_statements/junk") is None
    assert raw_store.manifest_key_for(key) == (
        "slovakia_financials/raw_statements/batch-000000000000-000000005002/manifest.json"
    )


def test_write_and_read_statement_batch():
    store = FakeObjectStore()
    bundles = [
        {"statement_id": 5001, "statement": {"id": 5001}, "entity": {"ico": "1"}, "reports": []},
        {"statement_id": 5002, "statement": {"stav": "ZMAZANÉ"}, "entity": None, "reports": []},
    ]
    key = raw_store.write_statement_batch(
        store, after_id=5000, last_id=5002, bundles=bundles,
        manifest={"source_run_id": "r1", "statement_count": 2},
    )
    assert raw_store.list_statement_batch_keys(store) == [key]
    assert raw_store.read_statement_batch(store, key) == bundles
    manifest = json.loads(
        store.read_bytes(raw_store.manifest_key_for(key), bucket=raw_store.RAW_BUCKET)
    )
    assert manifest["statement_count"] == 2


def test_template_store_roundtrip():
    store = FakeObjectStore()
    assert raw_store.template_exists(store, 699) is False
    raw_store.write_template(store, 699, {"nazov": "Úč POD"})
    assert raw_store.template_exists(store, 699) is True
    assert raw_store.read_template(store, 699) == {"nazov": "Úč POD"}


def test_sweep_statements_to_s3_stores_raw_batch_and_templates():
    from dagster_v3.defs.slovakia_financials import download

    store = FakeObjectStore()
    client = FakeRuzClient()
    counts = download.sweep_statements_to_s3(
        object_store=store, source_run_id="r1", after_id=5000,
        max_statements=10, client=client, request_delay_seconds=0,
    )
    assert counts["fetched_ids"] == 2 and counts["stored_statements"] == 2
    assert counts["fetch_failed"] == 0 and counts["last_id"] == 5002
    assert counts["batch_key"] == raw_store.statement_batch_key(5000, 5002)

    bundles = raw_store.read_statement_batch(store, counts["batch_key"])
    assert [b["statement_id"] for b in bundles] == [5001, 5002]
    assert bundles[0]["entity"]["ico"] == "36355232"
    assert bundles[0]["reports"][0]["obsah"] == POD_OBSAH
    assert bundles[1]["statement"]["stav"] == "ZMAZANÉ"  # deleted stored raw, decoded later

    assert raw_store.read_template(store, 699) == POD_TEMPLATE
    assert counts["templates_stored"] == 1
    manifest = json.loads(
        store.read_bytes(
            raw_store.manifest_key_for(counts["batch_key"]), bucket=raw_store.RAW_BUCKET
        )
    )
    assert manifest["source_run_id"] == "r1" and manifest["statement_count"] == 2


def test_sweep_statements_to_s3_skips_existing_templates():
    from dagster_v3.defs.slovakia_financials import download

    store = FakeObjectStore()
    raw_store.write_template(store, 699, POD_TEMPLATE)
    client = FakeRuzClient()
    counts = download.sweep_statements_to_s3(
        object_store=store, source_run_id="r1", after_id=5000,
        max_statements=10, client=client, request_delay_seconds=0,
    )
    assert client.template_calls == 0 and counts["templates_stored"] == 0


def test_sweep_statements_to_s3_empty_sweep_writes_nothing():
    from dagster_v3.defs.slovakia_financials import download

    store = FakeObjectStore()
    counts = download.sweep_statements_to_s3(
        object_store=store, source_run_id="r1", after_id=6000,
        max_statements=10, client=FakeRuzClient(), request_delay_seconds=0,
    )
    assert counts["batch_key"] is None and counts["last_id"] == 6000
    assert store.objects == {}


def _pod_bundle() -> dict:
    return {
        "statement_id": 5001,
        "statement": {
            "id": 5001, "idUJ": 9001, "idUctovnychVykazov": [7001],
            "obdobieOd": "2023-01", "obdobieDo": "2023-12",
            "datumZostaveniaK": "2023-12-31", "typ": "Riadna",
            "datumPodania": "2024-03-15", "datumSchvalenia": "2024-04-01",
        },
        "entity": {"id": 9001, "ico": "36355232"},
        "reports": [
            {"id": 7001, "idSablony": 699, "pristupnostDat": "Verejné", "obsah": POD_OBSAH}
        ],
    }


def test_decode_statement_bundle_resolves_entity_and_metrics():
    row = metrics.decode_statement_bundle(_pod_bundle(), lambda tid: {699: POD_TEMPLATE}[tid])
    assert row["ico"] == "36355232" and row["ruz_entity_id"] == "9001"
    assert row["template_name"] == "Úč POD" and row["template_mapped"] == 1
    assert row["fiscal_year"] == 2023 and row["period_end"] == "2023-12-31"
    assert row["metrics"]["net_result"] == 120.0 and row["mapped_metric_count"] == 6


def test_decode_statement_bundle_skips_deleted():
    bundle = {"statement_id": 5002, "statement": {"stav": "ZMAZANÉ"}, "entity": None, "reports": []}
    assert metrics.decode_statement_bundle(bundle, lambda tid: {}) is None


def test_decode_statement_bundle_ignores_non_public_reports():
    bundle = _pod_bundle()
    bundle["reports"][0]["pristupnostDat"] = "Neverejné"
    row = metrics.decode_statement_bundle(bundle, lambda tid: {699: POD_TEMPLATE}[tid])
    assert row["template_mapped"] == 0 and row["mapped_metric_count"] == 0
    assert row["metrics"]["net_result"] is None


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
