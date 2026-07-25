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


def test_norway_is_ted_only_and_says_so() -> None:
    """Norway has no ingested national register, and that must not be silent."""
    rule = COUNTRY_PROCUREMENT_RULES["NO"]

    assert rule.companies_table == "no_companies"
    # The register keys on org_number, not company_id -- the two differ.
    assert rule.company_id_column == "org_number"
    assert rule.identifier_length == 9
    assert rule.source_slugs == (TED_SOURCE_SLUG,)
    # No UHM dependency: declaring one would make Norway wait on Swedish data.
    assert rule.upstream_asset_keys == ("ted_publish_clickhouse",)
    assert "Doffin" in rule.coverage_caveat


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


# --- evidence SQL is built from the rule ------------------------------------

from dagster_v3.defs.company_signals.procurement import (  # noqa: E402
    procurement_evidence_insert_sql,
)


def test_sweden_sql_reads_both_sources() -> None:
    sql = procurement_evidence_insert_sql("stage", COUNTRY_PROCUREMENT_RULES["SE"])

    assert "sweden_uhm_procurement_base AS" in sql
    assert "corpscout.se_uhm_procurement_awards" in sql
    assert "SELECT * FROM sweden_uhm_procurement_base" in sql
    assert "corpscout.se_companies" in sql
    assert "length(w.winner_national_id) = 10" in sql
    assert "IN ('SE', 'SWE')" in sql


def test_norway_sql_is_ted_only_and_uses_its_own_register() -> None:
    """No national register means no UHM CTE at all, not an empty one."""
    sql = procurement_evidence_insert_sql("stage", COUNTRY_PROCUREMENT_RULES["NO"])

    assert "sweden_uhm_procurement_base" not in sql
    assert "se_uhm_procurement_awards" not in sql
    assert "fi_hilma" not in sql
    assert "se_companies" not in sql
    # Norway's register keys on org_number and its ids are 9 digits.
    assert "corpscout.no_companies" in sql
    assert "c.org_number = w.winner_national_id" in sql
    assert "length(w.winner_national_id) = 9" in sql
    assert "IN ('NO', 'NOR')" in sql
    assert "'NO' AS country_code" in sql


def test_finland_sql_unions_hilma_and_ted() -> None:
    """The builder is shape-agnostic: two pair-shaped sources, one union."""
    sql = procurement_evidence_insert_sql("stage", COUNTRY_PROCUREMENT_RULES["FI"])

    assert "finland_hilma_procurement_base AS" in sql
    assert "ted_procurement_base AS" in sql
    assert "corpscout.fi_hilma_notice_winners" in sql
    assert "corpscout.fi_hilma_notices" in sql
    assert sql.count("SELECT * FROM ") >= 2
    # Finland keys on business_id in both sources.
    assert "c.business_id = w.winner_business_id" in sql


def test_cross_source_dedup_is_slug_agnostic() -> None:
    """Dedup must catch any two sources agreeing, not just UHM and TED."""
    for code in ("SE", "FI"):
        sql = procurement_evidence_insert_sql("stage", COUNTRY_PROCUREMENT_RULES[code])
        assert "uniqExact(source_slug) AS source_count" in sql
        assert "HAVING source_count > 1" in sql
        assert "WHERE row_count = source_count" in sql
        assert "uhm_rows" not in sql
