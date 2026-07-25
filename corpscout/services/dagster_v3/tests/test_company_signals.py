import dagster as dg

from dagster_v3.defs.company_signals import tables
from dagster_v3.defs.company_signals.rules import COUNTRY_PROCUREMENT_RULES
from dagster_v3.defs.company_signals.procurement import (
    procurement_evidence_insert_sql,
)


def test_procurement_table_contracts() -> None:
    assert tables.GOVERNMENT_CONTRACT_EVIDENCE_COLUMNS == (
        "country_code",
        "company_id",
        "evidence_id",
        "source_slugs",
        "source_references",
        "publication_date",
        "buyer_name",
        "title",
        "agreement_type",
        "source_updated_at",
        "resolved_at",
    )
    assert tables.GOVERNMENT_CONTRACT_SUMMARY_COLUMNS == (
        "country_code",
        "company_id",
        "public_award_count",
        "public_award_last_date",
        "source_slugs",
        "source_updated_at",
        "resolved_at",
    )
    assert tables.SIGNAL_COVERAGE_COLUMNS == (
        "country_code",
        "signal_name",
        "coverage_status",
        "coverage_from",
        "coverage_to",
        "source_slugs",
        "source_updated_at",
        "resolved_at",
        "caveat",
    )


def test_procurement_sql_uses_exact_sweden_identity_and_country_scoped_ted() -> None:
    sql = procurement_evidence_insert_sql(
        "`corpscout`.`evidence_stage`", COUNTRY_PROCUREMENT_RULES["SE"]
    )

    assert "FROM corpscout.se_uhm_procurement_awards" in sql
    assert "FROM corpscout.ted_notice_winners" in sql
    assert "JOIN corpscout.ted_notices" in sql
    assert "JOIN corpscout.se_companies" in sql
    assert "u.company_match_status = 'exact'" in sql
    assert "u.company_id != ''" in sql
    assert "w.country_iso2 = 'SE'" in sql
    assert "n.country_iso2 = w.country_iso2" in sql
    assert "length(w.winner_national_id) = 10" in sql
    assert "upper(w.winner_country) IN ('SE', 'SWE')" in sql
    assert "unambiguous_cross_source_keys" in sql


def test_procurement_asset_depends_on_both_sources() -> None:
    from dagster_v3.defs.company_signals.procurement import (
        COUNTRY_CONTRACT_ASSETS,
    )

    # Sweden is now its own asset; Norway is a sibling with TED only.
    company_government_contract_summary_clickhouse = next(
        asset
        for asset in COUNTRY_CONTRACT_ASSETS
        if asset.key.to_user_string() == "se_government_contract_signals_clickhouse"
    )

    spec = company_government_contract_summary_clickhouse.specs_by_key[
        company_government_contract_summary_clickhouse.key
    ]
    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey("sweden_uhm_procurement_awards_clickhouse"),
        dg.AssetKey("ted_publish_clickhouse"),
    }
