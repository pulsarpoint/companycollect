"""BRL -> USD for PNCP contracts, run against a real DuckDB.

Asserting the SQL string would pass while the update joined nothing, so every
test here builds a candidates table, runs the conversion, and reads the rows
back.
"""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

import duckdb
import pytest

from dagster_v3.defs.brazil_pncp import tables
from dagster_v3.defs.brazil_pncp.usd_conversion import (
    CONTRACT_CURRENCY,
    VALUE_COLUMNS,
    apply_brazil_pncp_usd_conversion,
)

QUALIFIED = f"{tables.DUCKDB_SCHEMA}.{tables.CANDIDATES_TABLE}"


@dataclass(frozen=True)
class FakeRate:
    rate: Decimal
    rate_date: date
    source: str


class FakeExchangeRates:
    """Stands in for ExchangeRateClient, and records what it was asked for."""

    def __init__(self, rates: dict[str, str], *, missing: set[str] | None = None):
        self._rates = rates
        self._missing = missing or set()
        self.requested: list[tuple[str, str]] = []

    def usd_rates(self, requests):
        resolved = {}
        for request in requests:
            self.requested.append((request.currency, request.rate_date))
            if request.rate_date in self._missing:
                raise LookupError(f"no rate for {request.rate_date}")
            if (rate := self._rates.get(request.rate_date)) is not None:
                resolved[(request.currency, request.rate_date)] = FakeRate(
                    rate=Decimal(rate),
                    rate_date=date.fromisoformat(request.rate_date),
                    source="ecb",
                )
        return resolved


@pytest.fixture
def connection():
    con = duckdb.connect()
    con.execute(f"create schema {tables.DUCKDB_SCHEMA}")
    con.execute(
        f"""
        create table {QUALIFIED} (
            numero_controle_pncp varchar,
            valor_inicial decimal(38, 2),
            valor_parcela decimal(38, 2),
            valor_global decimal(38, 2),
            valor_acumulado decimal(38, 2),
            data_assinatura date,
            data_publicacao_pncp date
        )
        """
    )
    yield con
    con.close()


def _insert(connection, rows):
    """Rows are (id, valor_global, data_assinatura, data_publicacao) for the
    tests that only care about the primary figure; the others set all four."""
    connection.executemany(
        f"insert into {QUALIFIED} "
        f"(numero_controle_pncp, valor_global, data_assinatura, data_publicacao_pncp) "
        f"values (?, ?, ?, ?)",
        rows,
    )


def _read(connection):
    return {
        row[0]: row[1:]
        for row in connection.execute(
            f"select numero_controle_pncp, valor_global_usd, fx_rate_to_usd, "
            f"fx_rate_date, fx_source from {QUALIFIED} order by 1"
        ).fetchall()
    }


def test_converts_valor_global_at_the_signature_date_rate(connection) -> None:
    _insert(
        connection, [("c1", Decimal("1000.00"), date(2024, 6, 10), date(2024, 6, 20))]
    )
    rates = FakeExchangeRates({"2024-06-10": "0.185"})

    counts = apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection, exchange_rates=rates
    )

    usd, rate, rate_date, source = _read(connection)["c1"]
    assert usd == Decimal("185.00")
    assert rate == Decimal("0.185000000000")
    assert rate_date == date(2024, 6, 10)
    assert source == "ecb"
    assert counts["rows_converted"] == 1


def test_falls_back_to_the_publication_date_when_unsigned(connection) -> None:
    """PNCP leaves data_assinatura null on a minority of contracts, and a
    contract with no USD figure is invisible to every cross-country
    comparison."""
    _insert(connection, [("c1", Decimal("500.00"), None, date(2024, 6, 20))])
    rates = FakeExchangeRates({"2024-06-20": "0.20"})

    apply_brazil_pncp_usd_conversion(duckdb_connection=connection, exchange_rates=rates)

    usd, _, rate_date, _ = _read(connection)["c1"]
    assert usd == Decimal("100.00")
    # fx_rate_date shows which date was actually used, so a reader can tell.
    assert rate_date == date(2024, 6, 20)


def test_the_signature_date_wins_when_both_are_present(connection) -> None:
    _insert(
        connection, [("c1", Decimal("100.00"), date(2024, 1, 5), date(2024, 6, 20))]
    )
    rates = FakeExchangeRates({"2024-01-05": "0.10", "2024-06-20": "0.90"})

    apply_brazil_pncp_usd_conversion(duckdb_connection=connection, exchange_rates=rates)

    usd, _, rate_date, _ = _read(connection)["c1"]
    assert usd == Decimal("10.00")
    assert rate_date == date(2024, 1, 5)


def test_asks_for_brl_because_pncp_publishes_no_currency_field(connection) -> None:
    """There is no `moeda` anywhere in the API's 41 fields, so BRL is an
    assumption stated in one place rather than a value read from the payload."""
    _insert(connection, [("c1", Decimal("1.00"), date(2024, 6, 10), None)])
    rates = FakeExchangeRates({"2024-06-10": "0.185"})

    apply_brazil_pncp_usd_conversion(duckdb_connection=connection, exchange_rates=rates)

    assert {currency for currency, _ in rates.requested} == {CONTRACT_CURRENCY}


def test_one_missing_rate_does_not_discard_the_others(connection) -> None:
    """usd_rates raises LookupError if ANY requested pair is missing, so without
    a per-request fallback a single absent date would leave a whole batch of
    contracts unconverted."""
    _insert(
        connection,
        [
            ("c1", Decimal("100.00"), date(2024, 6, 10), None),
            ("c2", Decimal("100.00"), date(1998, 1, 2), None),
        ],
    )
    rates = FakeExchangeRates({"2024-06-10": "0.20"}, missing={"1998-01-02"})

    counts = apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection, exchange_rates=rates
    )

    rows = _read(connection)
    assert rows["c1"][0] == Decimal("20.00")
    assert rows["c2"][0] is None
    assert counts["rows_converted"] == 1
    assert counts["rate_dates"] == 2


def test_a_connection_error_is_not_swallowed_as_a_missing_rate(connection) -> None:
    """Only LookupError means "no such rate". Anything else is a real failure
    and must surface, or a broken ClickHouse would look like an empty rate
    table and blank every USD figure."""
    _insert(connection, [("c1", Decimal("100.00"), date(2024, 6, 10), None)])

    class Broken:
        def usd_rates(self, requests):
            raise ConnectionError("clickhouse is down")

    with pytest.raises(ConnectionError):
        apply_brazil_pncp_usd_conversion(
            duckdb_connection=connection, exchange_rates=Broken()
        )


def test_rerunning_clears_a_stale_conversion(connection) -> None:
    """A rate correction must not leave the old USD figure beside a contract
    whose rate has since changed."""
    _insert(connection, [("c1", Decimal("100.00"), date(2024, 6, 10), None)])
    apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=FakeExchangeRates({"2024-06-10": "0.20"}),
    )
    assert _read(connection)["c1"][0] == Decimal("20.00")

    apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=FakeExchangeRates({"2024-06-10": "0.25"}),
    )
    assert _read(connection)["c1"][0] == Decimal("25.00")

    # And a rate that disappears leaves NULL, not the previous number.
    apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection, exchange_rates=FakeExchangeRates({})
    )
    usd, rate, rate_date, source = _read(connection)["c1"]
    assert (usd, rate, rate_date, source) == (None, None, None, "")


def test_a_contract_with_no_usable_date_is_counted_not_hidden(connection) -> None:
    _insert(connection, [("c1", Decimal("100.00"), None, None)])

    counts = apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection, exchange_rates=FakeExchangeRates({})
    )

    assert counts["rows_missing_rate_date"] == 1
    assert counts["rows_with_value"] == 1
    assert counts["rows_converted"] == 0


def test_converts_every_value_the_register_publishes_not_just_the_shown_one(
    connection,
) -> None:
    """PNCP publishes four figures and the ingest keeps all four. Converting
    only valor_global would leave the other three usable in BRL alone, which is
    the same loss as not storing them, deferred one layer. Which figure a reader
    is shown is a presentation decision, not the pipeline's to make."""
    connection.execute(
        f"insert into {QUALIFIED} values "
        f"('c1', 100.00, 10.00, 120.00, 60.00, DATE '2024-06-10', NULL)"
    )

    apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=FakeExchangeRates({"2024-06-10": "0.20"}),
    )

    row = connection.execute(
        f"select {', '.join(usd for _, usd in VALUE_COLUMNS)} from {QUALIFIED}"
    ).fetchone()
    assert dict(zip((usd for _, usd in VALUE_COLUMNS), row)) == {
        "valor_inicial_usd": Decimal("20.00"),
        "valor_parcela_usd": Decimal("2.00"),
        "valor_global_usd": Decimal("24.00"),
        "valor_acumulado_usd": Decimal("12.00"),
    }


def test_a_figure_the_contract_omits_stays_null_rather_than_becoming_zero(
    connection,
) -> None:
    """valorAcumulado is absent on 51% of contracts. A zero USD figure would be
    indistinguishable from a contract genuinely worth nothing."""
    connection.execute(
        f"insert into {QUALIFIED} values "
        f"('c1', 100.00, NULL, 100.00, NULL, DATE '2024-06-10', NULL)"
    )

    counts = apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=FakeExchangeRates({"2024-06-10": "0.20"}),
    )

    row = connection.execute(
        f"select valor_parcela_usd, valor_acumulado_usd, valor_inicial_usd from {QUALIFIED}"
    ).fetchone()
    assert row == (None, None, Decimal("20.00"))
    # And the per-figure counts make a field that stops converting visible.
    assert counts["converted_valor_inicial_usd"] == 1
    assert counts["converted_valor_acumulado_usd"] == 0
    assert counts["present_valor_acumulado"] == 0


def test_a_contract_with_only_a_secondary_figure_still_gets_a_rate(
    connection,
) -> None:
    """Keying the date lookup off valor_global alone would skip a contract that
    publishes valorInicial and nothing else."""
    connection.execute(
        f"insert into {QUALIFIED} values "
        f"('c1', 500.00, NULL, NULL, NULL, DATE '2024-06-10', NULL)"
    )

    counts = apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=FakeExchangeRates({"2024-06-10": "0.20"}),
    )

    assert counts["rate_dates"] == 1
    assert connection.execute(f"select valor_inicial_usd from {QUALIFIED}").fetchone()[
        0
    ] == Decimal("100.00")


def test_refuses_to_run_before_the_candidates_are_built() -> None:
    con = duckdb.connect()
    con.execute(f"create schema {tables.DUCKDB_SCHEMA}")

    with pytest.raises(RuntimeError, match="brazil_pncp_contracts_duckdb"):
        apply_brazil_pncp_usd_conversion(
            duckdb_connection=con, exchange_rates=FakeExchangeRates({})
        )
    con.close()


def test_partitioned_conversion_updates_only_the_requested_month(connection) -> None:
    connection.execute(
        f"alter table {QUALIFIED} add column "
        f"{tables.CANDIDATE_PARTITION_COLUMN} varchar"
    )
    _insert(
        connection,
        [
            ("june", Decimal("100.00"), date(2025, 6, 10), date(2025, 6, 20)),
            ("july", Decimal("100.00"), date(2025, 7, 10), date(2025, 7, 20)),
        ],
    )
    connection.execute(
        f"update {QUALIFIED} set {tables.CANDIDATE_PARTITION_COLUMN} = "
        "case numero_controle_pncp when 'june' then '202506' else '202507' end"
    )

    counts = apply_brazil_pncp_usd_conversion(
        duckdb_connection=connection,
        exchange_rates=FakeExchangeRates({"2025-06-10": "0.20"}),
        partition="202506",
    )

    rows = _read(connection)
    assert rows["june"][0] == Decimal("20.00")
    assert rows["july"][0] is None
    assert counts["rows_converted"] == 1


def test_partitioned_conversion_refuses_a_missing_month(connection) -> None:
    connection.execute(
        f"alter table {QUALIFIED} add column "
        f"{tables.CANDIDATE_PARTITION_COLUMN} varchar"
    )
    _insert(
        connection,
        [("july", Decimal("100.00"), date(2025, 7, 10), date(2025, 7, 20))],
    )
    connection.execute(
        f"update {QUALIFIED} set {tables.CANDIDATE_PARTITION_COLUMN} = '202507'"
    )

    with pytest.raises(ValueError, match="202506"):
        apply_brazil_pncp_usd_conversion(
            duckdb_connection=connection,
            exchange_rates=FakeExchangeRates({}),
            partition="202506",
        )
