from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import dagster as dg
import pytest

from dagster_v3.defs.france_financial import tables
from dagster_v3.defs.france_financial.metrics import (
    apply_france_financial_usd_conversion,
)
from dagster_v3.defs.france_financial.records import (
    build_france_financial_metrics,
    load_parquet_path_into_raw_table,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATION = MIGRATIONS_DIR / "000209_corpscout_fr_financial_and_enrichments.up.sql"


def _sample_parquet(tmp_path: Path, *, empty: bool = False) -> Path:
    path = tmp_path / "ratios.parquet"
    connection = duckdb.connect()
    connection.execute(
        """
        create table ratios (
            siren varchar,
            date_cloture_exercice date,
            chiffre_d_affaires bigint,
            marge_brute bigint,
            ebe bigint,
            ebit bigint,
            resultat_net bigint,
            taux_d_endettement double,
            ratio_de_liquidite double,
            ratio_de_vetuste double,
            autonomie_financiere double,
            poids_bfr_exploitation_sur_ca double,
            couverture_des_interets double,
            caf_sur_ca double,
            capacite_de_remboursement double,
            marge_ebe double,
            resultat_courant_avant_impots_sur_ca double,
            poids_bfr_exploitation_sur_ca_jours double,
            rotation_des_stocks_jours double,
            credit_clients_jours double,
            credit_fournisseurs_jours double,
            type_bilan varchar,
            confidentiality varchar
        )
        """
    )
    if not empty:
        connection.execute(
            """
            insert into ratios values
            (
                '356000000', date '2024-12-31',
                34569000000, 34569000000, 1724000000, 2950000000, 1722000000,
                895.496, 0.0, 17.01, 3.984, 6.311, 0.0, 10.084, 78.542,
                4.988, 8.534, 22.719, 4.523, 0.0, 83.255, 'K', 'Public'
            ),
            (
                '356000000', date '2024-12-31',
                10260000000, 9793000000, -3771000000, -919000000, 1886000000,
                107.884, 194.978, NULL, 38.922, NULL, NULL, 19.025, 5.643,
                -36.754, NULL, NULL, NULL, 44.795, 74.476, 'C', 'Public'
            )
            """
        )
    connection.execute("copy ratios to ? (format parquet)", [str(path)])
    connection.close()
    return path


def test_load_and_build_financial_metrics(tmp_path: Path) -> None:
    parquet = _sample_parquet(tmp_path)
    connection = duckdb.connect()

    rows = load_parquet_path_into_raw_table(
        duckdb_connection=connection,
        parquet_path=parquet,
        source_url="https://example.test/ratios.parquet",
    )
    counts = build_france_financial_metrics(
        duckdb_connection=connection,
        source_run_id="test-run",
    )

    assert rows == 2
    assert counts == {"financial_metrics": 2, "duplicate_statement_keys": 0}
    columns = tuple(
        row[0]
        for row in connection.execute(
            """
            select column_name
            from information_schema.columns
            where table_schema = ? and table_name = ?
            order by ordinal_position
            """,
            [tables.DLT_DATASET_NAME, tables.METRICS_TABLE],
        ).fetchall()
    )
    assert columns == tables.FR_FINANCIAL_METRICS_COLUMNS

    row = connection.execute(
        f"""
        select siren, fiscal_year, balance_type_code, currency,
               revenue_amount_original, ebitda_amount_original,
               net_income_amount_original, debt_ratio_percent,
               liquidity_ratio_percent, asset_age_ratio_percent,
               customer_payment_days, confidentiality_status
        from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}
        where balance_type_code = 'K'
        """
    ).fetchone()
    assert row == (
        "356000000",
        2024,
        "K",
        "EUR",
        Decimal("34569000000.00"),
        Decimal("1724000000.00"),
        Decimal("1722000000.00"),
        Decimal("895.496000"),
        Decimal("0.000000"),
        Decimal("17.010000"),
        Decimal("0.000000"),
        "Public",
    )

    nullable = connection.execute(
        f"""
        select asset_age_ratio_percent, operating_working_capital_to_revenue_percent
        from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}
        where balance_type_code = 'C'
        """
    ).fetchone()
    assert nullable == (None, None)


def test_load_rejects_empty_parquet(tmp_path: Path) -> None:
    parquet = _sample_parquet(tmp_path, empty=True)
    with pytest.raises(ValueError, match="zero rows"):
        load_parquet_path_into_raw_table(
            duckdb_connection=duckdb.connect(),
            parquet_path=parquet,
            source_url="https://example.test/empty.parquet",
        )


@dataclass
class _FakeRate:
    rate: Decimal
    rate_date: date
    source: str


class _FakeExchangeRates:
    def __init__(self, rates: dict[tuple[str, str], _FakeRate]) -> None:
        self._rates = rates

    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], _FakeRate]:
        result: dict[tuple[str, str], _FakeRate] = {}
        missing: list[tuple[str, str]] = []
        for request in requests:
            key = (request.currency, str(request.rate_date))
            if key in self._rates:
                result[key] = self._rates[key]
            else:
                missing.append(key)
        if missing:
            raise LookupError(f"missing rates: {missing}")
        return result


def test_usd_conversion_fills_every_amount_pair(tmp_path: Path) -> None:
    connection = duckdb.connect()
    load_parquet_path_into_raw_table(
        duckdb_connection=connection,
        parquet_path=_sample_parquet(tmp_path),
        source_url="https://example.test/ratios.parquet",
    )
    build_france_financial_metrics(
        duckdb_connection=connection,
        source_run_id="test-run",
    )

    counts = apply_france_financial_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=_FakeExchangeRates(
            {
                ("EUR", "2024-12-31"): _FakeRate(
                    rate=Decimal("1.10"),
                    rate_date=date(2024, 12, 31),
                    source="ecb",
                )
            }
        ),
    )
    assert counts == {"rate_pairs": 1, "rates_found": 1, "rows_converted": 2}

    values = connection.execute(
        f"""
        select revenue_amount_usd, gross_margin_amount_usd,
               ebitda_amount_usd, ebit_amount_usd, net_income_amount_usd,
               fx_rate_to_usd, fx_source
        from {tables.DLT_DATASET_NAME}.{tables.METRICS_TABLE}
        where balance_type_code = 'K'
        """
    ).fetchone()
    assert values == (
        Decimal("38025900000.00"),
        Decimal("38025900000.00"),
        Decimal("1896400000.00"),
        Decimal("3245000000.00"),
        Decimal("1894200000.00"),
        Decimal("1.100000000000"),
        "ecb",
    )


def test_financial_columns_and_migration_contract() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert (
        f"CREATE TABLE IF NOT EXISTS {tables.QUALIFIED_FINANCIAL_METRICS_TABLE}" in sql
    )
    for column in tables.FR_FINANCIAL_METRICS_EXPORT_COLUMNS:
        assert f"    {column} " in sql, column
    for metric in tables.MONETARY_METRICS:
        assert f"{metric}_amount_original" in tables.FR_FINANCIAL_METRICS_COLUMNS
        assert f"{metric}_amount_usd" in tables.FR_FINANCIAL_METRICS_COLUMNS
    assert set(tables.FR_FINANCIAL_METRICS_COLUMNS) - set(
        tables.FR_FINANCIAL_METRICS_EXPORT_COLUMNS
    ) == set(tables.CLICKHOUSE_EXCLUDED_COLUMNS)


def test_financial_job_schedule_and_pool() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    schedule = repository.get_schedule_def("france_financial_schedule")
    assert schedule.cron_schedule == "10 7 12 * *"
    assert schedule.job.name == "france_financial_job"

    expected = {
        "france_financial_raw_duckdb",
        "france_financial_metrics_duckdb",
        "france_financial_metrics_usd_duckdb",
        "france_financial_metrics_clickhouse",
    }
    keys = {
        key.path[-1]
        for key in repository.get_job(
            "france_financial_job"
        ).asset_layer.executable_asset_keys
    }
    assert keys == expected

    graph = repository.asset_graph
    for asset_name in expected:
        assert graph.get(dg.AssetKey(asset_name)).pools == {"france_financial_duckdb"}
