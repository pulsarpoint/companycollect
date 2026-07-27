import pathlib

from dagster_v3.defs.company_signals.rules import COUNTRY_PROCUREMENT_RULES
from dagster_v3.defs.company_signals.sources import (
    HILMA_SOURCE_SLUG,
    TED_SOURCE_SLUG,
    UHM_SOURCE_SLUG,
)


def test_sweden_reads_its_national_register_alongside_ted() -> None:
    rule = COUNTRY_PROCUREMENT_RULES["SE"]

    assert rule.companies_table == "se_companies"
    assert rule.company_id_column == "company_id"
    assert rule.identifier_length == 10
    assert rule.source_slugs == tuple(sorted((UHM_SOURCE_SLUG, TED_SOURCE_SLUG)))
    assert "se_uhm_procurement_awards" in rule.required_clickhouse_tables
    assert rule.upstream_asset_keys == (
        "sweden_uhm_procurement_awards_clickhouse",
        "ted_publish_clickhouse",
    )


def test_norway_reads_doffin_alongside_ted() -> None:
    """Doffin closes the below-threshold gap the caveat used to describe. What
    it does not close is deduplication, and the caveat has to keep saying so."""
    rule = COUNTRY_PROCUREMENT_RULES["NO"]

    assert rule.companies_table == "no_companies"
    # The register keys on org_number, not company_id -- the two differ.
    assert rule.company_id_column == "org_number"
    assert rule.identifier_length == 9
    assert rule.source_slugs == ("norway_doffin_procurement", TED_SOURCE_SLUG)
    # No UHM dependency: declaring one would make Norway wait on Swedish data.
    assert rule.upstream_asset_keys == (
        "norway_doffin_notices_clickhouse",
        "ted_publish_clickhouse",
    )
    assert "Doffin" in rule.coverage_caveat
    # The gap that is now stale must not still be claimed.
    assert "is not ingested" not in rule.coverage_caveat
    assert "may be" in rule.coverage_caveat


def test_finland_reads_hilma_and_ted() -> None:
    """Hilma is a winners/notices pair, unlike UHM's flat awards table.

    It needs no accommodation from the caller: sources own their own joins, so
    both shapes sit behind one interface. Finland declaring Hilma is what
    proves that -- before the projection interface existed it could not.
    """
    rule = COUNTRY_PROCUREMENT_RULES["FI"]

    assert rule.source_slugs == tuple(sorted((HILMA_SOURCE_SLUG, TED_SOURCE_SLUG)))
    assert "fi_hilma_notice_winners" in rule.required_clickhouse_tables
    assert "fi_hilma_notices" in rule.required_clickhouse_tables
    assert "finland_hilma_clickhouse" in rule.upstream_asset_keys

    assert rule.companies_table == "fi_companies"
    assert rule.company_id_column == "business_id"
    # Y-tunnus is 1234567-8 -- nine characters including the dash.
    assert rule.identifier_length == 9



def test_each_country_gets_its_own_asset_name() -> None:
    names = {rule.asset_name for rule in COUNTRY_PROCUREMENT_RULES.values()}
    assert names == {
        "se_government_contract_signals_clickhouse",
        "fi_government_contract_signals_clickhouse",
        "no_government_contract_signals_clickhouse",
        "br_government_contract_signals_clickhouse",
    }


def test_required_tables_follow_the_rule() -> None:
    se = COUNTRY_PROCUREMENT_RULES["SE"].required_clickhouse_tables
    no = COUNTRY_PROCUREMENT_RULES["NO"].required_clickhouse_tables

    assert "se_uhm_procurement_awards" in se
    # Norway must not require a table it never reads.
    assert "se_uhm_procurement_awards" not in no
    assert "fi_hilma_notices" not in no
    assert "no_companies" in no
    for tables_needed in (se, no):
        assert "ted_notice_winners" in tables_needed
        assert "ted_notices" in tables_needed


# --- the country views the rules point at -----------------------------------


def _view_migration() -> str:
    root = pathlib.Path(__file__).resolve().parents[3]
    return (
        root
        / "clickhouse"
        / "migrations"
        / "000182_corpscout_contract_value_grain.up.sql"
    ).read_text()


def test_every_country_rule_has_a_view_in_the_migration() -> None:
    """A rule that names a view nothing creates would fail only at run time."""
    # Views are spread across migrations as countries were added, so the
    # contract holds against the ledger rather than against one file.
    root = pathlib.Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
    ledger = "\n".join(f.read_text() for f in sorted(root.glob("*.up.sql")))

    for rule in COUNTRY_PROCUREMENT_RULES.values():
        assert f"CREATE VIEW corpscout.{rule.contracts_view} AS" in ledger


def test_sweden_view_reads_both_its_sources() -> None:
    sql = _view_migration().split("CREATE VIEW corpscout.se_government_contracts")[1]
    sql = sql.split("CREATE VIEW")[0]

    assert "corpscout.se_uhm_procurement_awards" in sql
    assert "corpscout.ted_notice_winners" in sql
    assert "corpscout.se_companies" in sql


def test_norway_view_is_ted_only_and_uses_its_own_register() -> None:
    sql = _view_migration().split("CREATE VIEW corpscout.no_government_contracts")[1]
    sql = sql.split("CREATE VIEW")[0]

    assert "corpscout.no_companies" in sql
    assert "c.org_number = w.winner_national_id" in sql
    # Norway must not read a source it does not have.
    assert "se_uhm_procurement_awards" not in sql
    assert "fi_hilma" not in sql


def test_finland_view_merges_hilma_and_ted() -> None:
    sql = _view_migration().split("CREATE VIEW corpscout.fi_government_contracts")[1]
    sql = sql.split("CREATE VIEW")[0]

    assert "corpscout.fi_hilma_notice_winners" in sql
    assert "corpscout.ted_notice_winners" in sql
    assert sql.count("UNION ALL") == 1


def test_cross_country_view_needs_no_edit_to_add_a_country() -> None:
    """The point of the naming convention.

    merge() resolves the pattern at query time, so a new country is one
    CREATE VIEW and nothing downstream changes. A UNION ALL would need editing,
    and a migration, for every country added.
    """
    sql = _view_migration()

    assert "merge(corpscout, '^[a-z]{2}_government_contracts$')" in sql
    for rule in COUNTRY_PROCUREMENT_RULES.values():
        assert rule.contracts_view.endswith("_government_contracts")
        assert len(rule.contracts_view.split("_")[0]) == 2


def test_only_winner_attributable_value_is_summed() -> None:
    """Hilma's value is notice-level and repeats across a notice's winners.

    Summing it per company multiplies one procurement by its winner count, so
    the two must stay in separate columns and only one may be summed.
    """
    sql = _view_migration()
    summary = sql.split("CREATE VIEW corpscout.company_government_contract_summary")[1]

    assert "sum(value_amount_usd)" in summary
    assert "notice_value_amount_usd" not in summary

    hilma = sql.split("CREATE VIEW corpscout.fi_government_contracts")[1]
    hilma = hilma.split("UNION ALL")[0]
    assert "AS notice_value_amount_original" in hilma
    # Nothing from Hilma is attributable to a single winner.
    assert "CAST(NULL AS Nullable(Decimal(38, 2))) AS value_amount_original" in hilma
