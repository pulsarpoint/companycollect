import pathlib

from dagster_v3.defs.company_signals.rules import COUNTRY_PROCUREMENT_RULES
from dagster_v3.defs.company_signals.sources import (
    DECP_SOURCE_SLUG,
    HILMA_SOURCE_SLUG,
    IUB_SOURCE_SLUG,
    RHR_SOURCE_SLUG,
    TED_SOURCE_SLUG,
    UVO_SOURCE_SLUG,
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


def test_france_slovakia_and_latvia_read_national_sources_and_ted() -> None:
    france = COUNTRY_PROCUREMENT_RULES["FR"]
    slovakia = COUNTRY_PROCUREMENT_RULES["SK"]
    latvia = COUNTRY_PROCUREMENT_RULES["LV"]

    assert france.companies_table == "fr_companies"
    assert france.company_id_column == "siren"
    assert france.identifier_length == 9
    assert france.ted_winner_countries == ("FR", "FRA")
    assert france.source_slugs == tuple(sorted((DECP_SOURCE_SLUG, TED_SOURCE_SLUG)))
    assert france.upstream_asset_keys == (
        "france_decp_contract_holders_clickhouse",
        "ted_publish_clickhouse",
    )

    assert slovakia.companies_table == "sk_companies"
    assert slovakia.company_id_column == "ico"
    assert slovakia.identifier_length == 8
    assert slovakia.ted_winner_countries == ("SK", "SVK")
    assert slovakia.source_slugs == tuple(sorted((TED_SOURCE_SLUG, UVO_SOURCE_SLUG)))
    assert slovakia.upstream_asset_keys == (
        "slovakia_uvo_procurement_notices_clickhouse",
        "ted_publish_clickhouse",
    )

    assert latvia.companies_table == "lv_companies"
    assert latvia.company_id_column == "regcode"
    assert latvia.identifier_length == 11
    assert latvia.ted_winner_countries == ("LV", "LVA")
    assert latvia.source_slugs == tuple(sorted((IUB_SOURCE_SLUG, TED_SOURCE_SLUG)))
    assert latvia.upstream_asset_keys == (
        "latvia_iub_procurement_clickhouse",
        "ted_publish_clickhouse",
    )


def test_estonia_reads_rhr_and_ted_with_its_registry_code() -> None:
    estonia = COUNTRY_PROCUREMENT_RULES["EE"]

    assert estonia.companies_table == "ee_companies"
    assert estonia.company_id_column == "reg_code"
    assert estonia.identifier_length == 8
    assert estonia.ted_winner_countries == ("EE", "EST")
    assert estonia.source_slugs == tuple(sorted((RHR_SOURCE_SLUG, TED_SOURCE_SLUG)))
    assert estonia.upstream_asset_keys == (
        "estonia_rhr_procurement_clickhouse",
        "ted_publish_clickhouse",
    )


def test_each_country_gets_its_own_asset_name() -> None:
    names = {rule.asset_name for rule in COUNTRY_PROCUREMENT_RULES.values()}
    assert names == {
        "se_government_contract_signals_clickhouse",
        "fi_government_contract_signals_clickhouse",
        "no_government_contract_signals_clickhouse",
        "br_government_contract_signals_clickhouse",
        "fr_government_contract_signals_clickhouse",
        "sk_government_contract_signals_clickhouse",
        "lv_government_contract_signals_clickhouse",
        "ee_government_contract_signals_clickhouse",
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

    for country, company_table in (
        ("FR", "fr_companies"),
        ("SK", "sk_companies"),
        ("LV", "lv_companies"),
        ("EE", "ee_companies"),
    ):
        required = COUNTRY_PROCUREMENT_RULES[country].required_clickhouse_tables
        assert company_table in required
        assert "ted_notice_winners" in required
        assert "ted_notices" in required
    assert "fr_decp_contract_holders" in (
        COUNTRY_PROCUREMENT_RULES["FR"].required_clickhouse_tables
    )
    assert "sk_uvo_procurement_notices" in (
        COUNTRY_PROCUREMENT_RULES["SK"].required_clickhouse_tables
    )
    assert "lv_iub_notice_winners_current" in (
        COUNTRY_PROCUREMENT_RULES["LV"].required_clickhouse_tables
    )
    assert "ee_rhr_procurement_winners_current" in (
        COUNTRY_PROCUREMENT_RULES["EE"].required_clickhouse_tables
    )


# --- the country views the rules point at -----------------------------------


def _view_migration() -> str:
    root = pathlib.Path(__file__).resolve().parents[3]
    return (
        root
        / "clickhouse"
        / "migrations"
        / "000182_corpscout_contract_value_grain.up.sql"
    ).read_text()


def _fr_sk_view_migration() -> str:
    root = pathlib.Path(__file__).resolve().parents[3]
    return (
        root
        / "clickhouse"
        / "migrations"
        / "000201_corpscout_fr_sk_national_procurement.up.sql"
    ).read_text()


def _lv_view_migration() -> str:
    root = pathlib.Path(__file__).resolve().parents[3]
    return (
        root
        / "clickhouse"
        / "migrations"
        / "000202_corpscout_lv_national_procurement.up.sql"
    ).read_text()


def _ee_view_migration() -> str:
    root = pathlib.Path(__file__).resolve().parents[3]
    return (
        root
        / "clickhouse"
        / "migrations"
        / "000206_corpscout_ee_national_procurement.up.sql"
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


def test_france_and_slovakia_views_join_their_domestic_company_registers() -> None:
    sql = _fr_sk_view_migration()
    france = sql.split("CREATE VIEW corpscout.fr_government_contracts AS")[1]
    france = france.split("CREATE VIEW")[0]
    slovakia = sql.split("CREATE VIEW corpscout.sk_government_contracts AS")[1]
    slovakia = slovakia.split("CREATE VIEW")[0]

    assert "corpscout.fr_companies AS c" in france
    assert "c.siren = w.winner_national_id" in france
    assert "w.country_iso2 = 'FR'" in france
    assert "upper(w.winner_country) IN ('FR', 'FRA')" in france
    assert "corpscout.fr_decp_contract_holders" in france

    assert "corpscout.sk_companies AS c" in slovakia
    assert "c.ico = w.winner_national_id" in slovakia
    assert "w.country_iso2 = 'SK'" in slovakia
    assert "upper(w.winner_country) IN ('SK', 'SVK')" in slovakia
    assert "corpscout.sk_uvo_procurement_notices" in slovakia


def test_cross_country_summary_includes_france_and_slovakia() -> None:
    sql = _fr_sk_view_migration()
    summary = sql.split("CREATE VIEW corpscout.company_government_contract_summary AS")[
        1
    ]

    assert "fr_government_contract_summary" in summary
    assert "sk_government_contract_summary" in summary


def test_latvia_view_joins_its_domestic_company_register() -> None:
    sql = _lv_view_migration()
    latvia = sql.split("CREATE VIEW corpscout.lv_government_contracts AS")[1]
    latvia = latvia.split("CREATE VIEW")[0]

    assert "corpscout.lv_companies AS c" in latvia
    assert "c.regcode = w.winner_national_id" in latvia
    assert "w.country_iso2 = 'LV'" in latvia
    assert "upper(w.winner_country) IN ('LV', 'LVA')" in latvia
    assert "corpscout.lv_iub_notice_winners_current" in latvia


def test_cross_country_summary_includes_latvia() -> None:
    sql = _lv_view_migration()
    summary = sql.split("CREATE VIEW corpscout.company_government_contract_summary AS")[
        1
    ]

    assert "lv_government_contract_summary" in summary


def test_estonia_view_uses_rhr_and_exact_ted_overlap_keys() -> None:
    sql = _ee_view_migration()
    estonia = sql.split("CREATE VIEW corpscout.ee_government_contracts AS")[1]
    estonia = estonia.split("CREATE VIEW")[0]

    assert "corpscout.ee_rhr_procurement_winners_current" in estonia
    assert "corpscout.ee_companies AS c" in estonia
    assert "c.reg_code = w.winner_national_id" in estonia
    assert "w.country_iso2 = 'EE'" in estonia
    assert "upper(w.winner_country) IN ('EE', 'EST')" in estonia
    assert "SELECT ted_publication_number" in estonia
    assert "w.awarded_value_attributable = 1" in estonia

    summary = sql.split(
        "CREATE VIEW corpscout.company_government_contract_summary AS"
    )[1]
    assert "ee_government_contract_summary" in summary


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
