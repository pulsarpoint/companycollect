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
    # Norwegian organisasjonsnummer. TED publishes the bare 9 digits -- verified
    # against live notices 350545-2025 and 351526-2025, whose ids 937884117,
    # 926725939 and 982903742 match no_companies.org_number directly -- so the
    # rule mainly has to leave them alone while tolerating VAT and grouped forms.
    assert normalize_national_id("NOR", "937884117") == "937884117"
    assert normalize_national_id("NO", "926725939") == "926725939"
    assert normalize_national_id("NOR", "NO937884117MVA") == "937884117"
    assert normalize_national_id("NOR", "937 884 117") == "937884117"
    # Not an organisasjonsnummer (10 digits) -- must pass through untouched
    # rather than be coerced into a wrong-but-plausible match.
    assert normalize_national_id("NOR", "0110140071") == "0110140071"
    assert normalize_national_id("", "whatever") == "whatever"
    assert normalize_national_id("FIN", "") == ""


@pytest.mark.parametrize(
    ("country", "raw", "expected"),
    [
        # Live TED 47-2026: the winning French organization publishes a
        # whitespace-grouped SIRET. The company spine is keyed by SIREN.
        ("FRA", "530 796 176 00028", "530796176"),
        ("FR", "53079617600028", "530796176"),
        ("FRA", "530796176", "530796176"),
        ("FR", "FR40530796176", "530796176"),
        # Live TED 110-2026 and 147-2026 carry malformed legacy identifiers;
        # neither may be coerced into a plausible SIREN.
        ("FRA", "1710451-1-2-1", "1710451-1-2-1"),
        ("FRA", "4137512310049", "4137512310049"),
        # Live TED 59-2026 publishes an IČO directly. Whitespace is harmless,
        # while the VAT number from 1419-2026 is a different identifier and
        # cannot safely be mapped to sk_companies.ico.
        ("SVK", "36379913", "36379913"),
        ("SK", "36 379 913", "36379913"),
        ("SVK", "SK2022354598", "SK2022354598"),
        # Live TED 507-2026 publishes the Latvian winner's 11-digit company
        # registration code directly. Latvia's VAT form is the same code with
        # an LV prefix, so both forms can safely join lv_companies.regcode.
        ("LVA", "40103250317", "40103250317"),
        ("LV", "40 103 250 317", "40103250317"),
        ("LVA", "LV40103250317", "40103250317"),
        ("LV", "LV4010325031", "LV4010325031"),
        # Live TED 7015-2026 publishes the Danish winner's CVR in VAT form.
        # CVR itself is eight digits; consortium and name-shaped identifiers
        # must stay raw because neither identifies exactly one Danish company.
        ("DNK", "DK26527791", "26527791"),
        ("DK", "25 05 00 53", "25050053"),
        ("DNK", "26527791", "26527791"),
        ("DK", "79095311/ 26369126", "79095311/ 26369126"),
        ("DNK", "Protector Forsikring Danmark", "Protector Forsikring Danmark"),
    ],
)
def test_normalize_live_country_identifiers(
    country: str, raw: str, expected: str
) -> None:
    assert normalize_national_id(country, raw) == expected


def test_ted_countries_include_all_supported_european_markets() -> None:
    assert {
        (country.place_code, country.country_iso2) for country in tables.COUNTRIES
    } == {
        ("FIN", "FI"),
        ("SWE", "SE"),
        ("NOR", "NO"),
        ("FRA", "FR"),
        ("SVK", "SK"),
        ("LVA", "LV"),
        ("DNK", "DK"),
    }


def _write_partition(tmp_path: Path, key: str, fixture_names: list[str]) -> Path:
    """Build a partition DuckDB the same shape ted_monthly_duckdb produces."""
    target = tmp_path / f"partition_key={key}" / "data.duckdb"
    target.parent.mkdir(parents=True)
    con = duckdb.connect(str(target))
    # The same DDL the asset writes. Spelled out here once, it drifted the
    # moment a table was added, so both sides now read it from one place.
    for table, columns in tables.PARTITION_TABLE_DDL.items():
        con.execute(f"create table {table} ({columns})")
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
        values = parsed.notice_values
        _insert(
            con,
            "notice_docs",
            [
                number,
                parsed.buyer_org_ref,
                values.estimated_value_amount,
                values.estimated_value_currency,
                values.framework_maximum_amount,
                values.framework_maximum_currency,
                values.framework_total_maximum_amount,
                values.framework_total_maximum_currency,
                values.framework_total_approximate_amount,
                values.framework_total_approximate_currency,
            ],
        )
        for org in parsed.organizations:
            _insert(
                con,
                "organizations",
                [
                    number,
                    org.org_ref,
                    org.name,
                    org.national_id_raw,
                    normalize_national_id(org.country, org.national_id_raw),
                    org.country,
                ],
            )
        for lot in parsed.lots:
            _insert(
                con,
                "lots",
                [
                    number,
                    lot.lot_id,
                    lot.lot_title,
                    lot.estimated_value_amount,
                    lot.estimated_value_currency,
                    lot.framework_maximum_amount,
                    lot.framework_maximum_currency,
                    lot.framework_value_maximum_amount,
                    lot.framework_value_maximum_currency,
                    lot.framework_value_reestimated_amount,
                    lot.framework_value_reestimated_currency,
                    lot.lower_tender_amount,
                    lot.lower_tender_currency,
                    lot.higher_tender_amount,
                    lot.higher_tender_currency,
                ],
            )
        for w in parsed.winners:
            _insert(
                con,
                "winner_links",
                [
                    number,
                    w.lot_id,
                    w.tender_id,
                    w.winner_ordinal,
                    w.org_ref,
                    w.awarded_amount,
                    w.awarded_currency,
                    w.subcontracting_amount,
                    w.subcontracting_currency,
                ],
            )
    con.close()
    return target


def _insert(con, table: str, row: list) -> None:
    placeholders = ", ".join("?" * tables.partition_column_count(table))
    con.execute(f"insert into {table} values ({placeholders})", row)


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
    return con, [("FI", "2026-03-01", p1), ("FI", "2026-04-01", p2)]


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
    assert fx["notice_winners.awarded_converted"] > 0
    converted = con.execute(
        f"select awarded_amount_usd from {winners} "
        f"where publication_number = '492374-2026'"
    ).fetchone()[0]
    assert converted == Decimal("808500.00")

    # Column contracts match the migration order.
    for duckdb_table, contract in (
        (tables.NOTICES_TABLE, tables.TED_NOTICES_COLUMNS),
        (tables.NOTICE_LOTS_TABLE, tables.TED_NOTICE_LOTS_COLUMNS),
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
        partitions=[("FI", "2026-03-01", partition)],
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


# --- month x country partitioning -------------------------------------------


def test_partitions_are_keyed_by_country_and_month() -> None:
    """Country is a partition dimension so one country can be backfilled alone.

    Previously the assets were month-only and looped every entry in COUNTRIES,
    so a country added later could not be filled without re-fetching the ones
    already loaded -- which is exactly why ted_notice_winners held 45,891
    Finnish winners and zero Swedish ones after Sweden was added to COUNTRIES.
    """
    from dagster_v3.defs.ted_procurement.assets import TED_PARTITIONS

    assert set(TED_PARTITIONS.partition_dimension_names) == {"country", "month"}
    countries = next(
        dimension.partitions_def
        for dimension in TED_PARTITIONS.partitions_defs
        if dimension.name == "country"
    )
    assert set(countries.get_partition_keys()) == {
        country.country_iso2 for country in tables.COUNTRIES
    }


def test_s3_prefix_and_duckdb_path_separate_countries() -> None:
    """Both storage layouts must key on country, or two countries collide."""
    from dagster_v3.defs.ted_procurement.publish import partition_duckdb_path

    se_prefix = tables.s3_partition_prefix(country_iso2="SE", month="2024-01-01")
    fi_prefix = tables.s3_partition_prefix(country_iso2="FI", month="2024-01-01")
    assert se_prefix != fi_prefix
    assert "country=SE" in se_prefix and "partition=2024-01-01" in se_prefix

    se_path = partition_duckdb_path(country_iso2="SE", month="2024-01-01")
    fi_path = partition_duckdb_path(country_iso2="FI", month="2024-01-01")
    assert se_path != fi_path
    assert "country=SE" in str(se_path)


def test_list_parsed_partitions_reports_country_and_month(
    tmp_path, monkeypatch
) -> None:
    from dagster_v3.defs.ted_procurement import publish as publish_module

    monkeypatch.setattr(publish_module, "PARTITION_DUCKDB_ROOT", tmp_path)
    for country, month in (("SE", "2024-01-01"), ("FI", "2024-02-01")):
        target = publish_module.partition_duckdb_path(country_iso2=country, month=month)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"")

    found = {
        (country, month)
        for country, month, _ in publish_module.list_parsed_partitions()
    }
    assert found == {("SE", "2024-01-01"), ("FI", "2024-02-01")}
