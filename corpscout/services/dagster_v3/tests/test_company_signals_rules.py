from dagster_v3.defs.company_signals.rules import (
    COUNTRY_PROCUREMENT_RULES,
    TED_SOURCE,
    UHM_SOURCE,
)


def test_sweden_reads_its_national_register_alongside_ted() -> None:
    rule = COUNTRY_PROCUREMENT_RULES["SE"]

    assert rule.companies_table == "se_companies"
    assert rule.company_id_column == "company_id"
    assert rule.identifier_length == 10
    assert rule.national_source is not None
    assert rule.national_source.awards_table == "se_uhm_procurement_awards"
    assert rule.source_slugs == tuple(sorted((UHM_SOURCE, TED_SOURCE)))
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
    assert rule.national_source is None
    assert rule.source_slugs == (TED_SOURCE,)
    # No UHM dependency: declaring one would make Norway wait on Swedish data.
    assert rule.upstream_asset_keys == ("ted_publish_clickhouse",)
    assert "Doffin" in rule.coverage_caveat


def test_each_country_gets_its_own_asset_name() -> None:
    names = {rule.asset_name for rule in COUNTRY_PROCUREMENT_RULES.values()}
    assert names == {
        "se_government_contract_signals_clickhouse",
        "no_government_contract_signals_clickhouse",
    }


def test_required_tables_follow_the_rule() -> None:
    se = COUNTRY_PROCUREMENT_RULES["SE"].required_clickhouse_tables
    no = COUNTRY_PROCUREMENT_RULES["NO"].required_clickhouse_tables

    assert "se_uhm_procurement_awards" in se
    # Norway must not require a table it never reads.
    assert "se_uhm_procurement_awards" not in no
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

    assert "uhm_base AS" in sql
    assert "corpscout.se_uhm_procurement_awards" in sql
    assert "SELECT * FROM uhm_base" in sql
    assert "corpscout.se_companies" in sql
    assert "length(w.winner_national_id) = 10" in sql
    assert "IN ('SE', 'SWE')" in sql


def test_norway_sql_is_ted_only_and_uses_its_own_register() -> None:
    """No national register means no UHM CTE at all, not an empty one."""
    sql = procurement_evidence_insert_sql("stage", COUNTRY_PROCUREMENT_RULES["NO"])

    assert "uhm_base" not in sql
    assert "se_uhm_procurement_awards" not in sql
    assert "se_companies" not in sql
    # Norway's register keys on org_number and its ids are 9 digits.
    assert "corpscout.no_companies" in sql
    assert "c.org_number = w.winner_national_id" in sql
    assert "length(w.winner_national_id) = 9" in sql
    assert "IN ('NO', 'NOR')" in sql
    assert "'NO' AS country_code" in sql
