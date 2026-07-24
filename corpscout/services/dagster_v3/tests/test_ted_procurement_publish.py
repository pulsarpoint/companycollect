from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest

from dagster_v3.defs.ted_procurement import tables
from dagster_v3.defs.ted_procurement.parser import parse_award_notice_xml
from dagster_v3.defs.ted_procurement.publish import (
    apply_ted_usd_conversion,
    build_publish_tables,
    normalize_national_id,
)

FIXTURES = Path(__file__).parent / "fixtures" / "ted_procurement"


def test_normalize_national_id() -> None:
    assert normalize_national_id("FIN", "FI28563905") == "2856390-5"
    assert normalize_national_id("FIN", "2856390-5") == "2856390-5"
    assert normalize_national_id("SWE", "556533-8133") == "5565338133"
    assert normalize_national_id("SE", "5565338133") == "5565338133"
    assert normalize_national_id("SWE", "165565338133") == "5565338133"
    assert normalize_national_id("SWE", "195565338133") == "195565338133"
    assert normalize_national_id("", "whatever") == "whatever"
    assert normalize_national_id("FIN", "") == ""


def test_ted_countries_include_sweden() -> None:
    assert {
        (country.place_code, country.country_iso2) for country in tables.COUNTRIES
    } == {
        ("FIN", "FI"),
        ("SWE", "SE"),
    }


def _write_partition(tmp_path: Path, key: str, fixture_names: list[str]) -> Path:
    """Build a partition DuckDB the same shape ted_monthly_duckdb produces."""
    target = tmp_path / f"partition_key={key}" / "data.duckdb"
    target.parent.mkdir(parents=True)
    con = duckdb.connect(str(target))
    con.execute(
        "create table listing (publication_number varchar, publication_date varchar, "
        "notice_type varchar, buyer_name varchar, notice_title varchar, "
        "total_value varchar, total_value_currency varchar, country_iso2 varchar, "
        "place_country varchar)"
    )
    con.execute(
        "create table notice_docs (publication_number varchar, buyer_org_ref varchar)"
    )
    con.execute(
        "create table organizations (publication_number varchar, org_ref varchar, "
        "name varchar, national_id_raw varchar, national_id varchar, country varchar)"
    )
    con.execute(
        "create table winner_links (publication_number varchar, lot_id varchar, "
        "tender_id varchar, winner_ordinal integer, org_ref varchar, "
        "awarded_amount varchar, awarded_currency varchar)"
    )
    for name in fixture_names:
        number = name.removesuffix(".xml")
        parsed = parse_award_notice_xml((FIXTURES / name).read_bytes())
        country = "SE" if number == "494783-2026" else "FI"
        place = "SWE" if country == "SE" else "FIN"
        con.execute(
            "insert into listing values (?, '2026-03-04+01:00', 'can-standard', "
            "'Buyer', 'Title', '1000000', 'EUR', ?, ?)",
            [number, country, place],
        )
        con.execute(
            "insert into notice_docs values (?, ?)", [number, parsed.buyer_org_ref]
        )
        for org in parsed.organizations:
            con.execute(
                "insert into organizations values (?, ?, ?, ?, ?, ?)",
                [
                    number,
                    org.org_ref,
                    org.name,
                    org.national_id_raw,
                    normalize_national_id(org.country, org.national_id_raw),
                    org.country,
                ],
            )
        for w in parsed.winners:
            con.execute(
                "insert into winner_links values (?, ?, ?, ?, ?, ?, ?)",
                [
                    number,
                    w.lot_id,
                    w.tender_id,
                    w.winner_ordinal,
                    w.org_ref,
                    w.awarded_amount,
                    w.awarded_currency,
                ],
            )
    con.close()
    return target


@dataclass
class _FakeRate:
    rate: Decimal
    rate_date: date
    source: str


class _FakeExchangeRates:
    def usd_rates(self, requests: list[Any]) -> dict[tuple[str, str], _FakeRate]:
        return {
            (r.currency, str(r.rate_date)): _FakeRate(
                rate=Decimal("1.10"), rate_date=date(2026, 3, 4), source="ecb"
            )
            for r in requests
        }


@pytest.fixture
def publish_connection(tmp_path: Path) -> tuple[duckdb.DuckDBPyConnection, list]:
    p1 = _write_partition(
        tmp_path,
        "2026-03-01",
        ["492374-2026.xml", "494092-2026.xml", "494783-2026.xml"],
    )
    # Same notice re-listed in a later partition — dedup must keep the newer.
    p2 = _write_partition(tmp_path, "2026-04-01", ["492374-2026.xml"])
    con = duckdb.connect(":memory:")
    return con, [("2026-03-01", p1), ("2026-04-01", p2)]


def test_build_publish_tables_and_usd(publish_connection) -> None:
    con, partitions = publish_connection
    counts = build_publish_tables(
        duckdb_connection=con, partitions=partitions, source_run_id="run"
    )
    assert counts["notices"] == 3  # 492374 deduped across partitions
    assert counts["winners"] > 0

    notices = f"{tables.DLT_DATASET_NAME}.{tables.NOTICES_TABLE}"
    winners = f"{tables.DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}"

    deduped = con.execute(
        f"select partition_key, buyer_national_id from {notices} "
        f"where publication_number = '492374-2026'"
    ).fetchone()
    assert deduped[0] == "2026-04-01"  # newer partition won
    assert deduped[1] != ""  # buyer resolved through notice_docs -> organizations

    fin_winner = con.execute(
        f"select winner_name, winner_national_id, awarded_amount_original "
        f"from {winners} where publication_number = '492374-2026'"
    ).fetchone()
    assert fin_winner[1] == "3278699-2"
    assert fin_winner[2] == Decimal("735000.00")

    # Any VAT-form FI id in the winner surface must have been normalized.
    unnormalized = con.execute(
        f"select count(*) from {winners} where winner_national_id like 'FI%'"
    ).fetchone()[0]
    assert unnormalized == 0
    swedish = con.execute(
        f"select count(*) from {winners} where publication_number = '494783-2026'"
    ).fetchone()[0]
    assert swedish == 3  # framework multi-supplier award

    fx = apply_ted_usd_conversion(
        duckdb_connection=con, exchange_rates=_FakeExchangeRates()
    )
    assert fx["winner_amounts_converted"] > 0
    converted = con.execute(
        f"select awarded_amount_usd from {winners} "
        f"where publication_number = '492374-2026'"
    ).fetchone()[0]
    assert converted == Decimal("808500.00")

    # Column contracts match the migration order.
    for duckdb_table, contract in (
        (tables.NOTICES_TABLE, tables.TED_NOTICES_COLUMNS),
        (tables.NOTICE_WINNERS_TABLE, tables.TED_NOTICE_WINNERS_COLUMNS),
    ):
        columns = tuple(
            row[0]
            for row in con.execute(
                "select column_name from information_schema.columns "
                "where table_schema = ? and table_name = ? order by ordinal_position",
                [tables.DLT_DATASET_NAME, duckdb_table],
            ).fetchall()
        )
        assert columns == contract


def test_build_refuses_empty_partitions() -> None:
    con = duckdb.connect(":memory:")
    with pytest.raises(ValueError, match="No parsed TED partitions"):
        build_publish_tables(duckdb_connection=con, partitions=[], source_run_id="run")


def test_same_notice_survives_in_two_country_scopes(tmp_path: Path) -> None:
    partition = _write_partition(tmp_path, "2026-03-01", ["494783-2026.xml"])
    partition_connection = duckdb.connect(str(partition))
    partition_connection.execute(
        """
        insert into listing
        select publication_number, publication_date, notice_type, buyer_name,
               notice_title, total_value, total_value_currency, 'FI', 'FIN'
        from listing
        """
    )
    partition_connection.close()

    connection = duckdb.connect(":memory:")
    counts = build_publish_tables(
        duckdb_connection=connection,
        partitions=[("2026-03-01", partition)],
        source_run_id="run",
    )

    assert counts["notices"] == 2
    assert counts["winners"] == 6
    scopes = connection.execute(
        f"""
        select country_iso2, count(*)
        from {tables.DLT_DATASET_NAME}.{tables.NOTICE_WINNERS_TABLE}
        group by country_iso2
        order by country_iso2
        """
    ).fetchall()
    assert scopes == [("FI", 3), ("SE", 3)]
