import dagster as dg

from dagster_v3.defs.company_signals import tables
from dagster_v3.defs.company_signals.rules import COUNTRY_PROCUREMENT_RULES


def test_only_coverage_is_materialized() -> None:
    """Contracts are views, so there is no evidence or summary table to write.

    Coverage is the exception because its useful content is prose about what a
    country's sources miss, which no query over the rows can produce.
    """
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
    assert not hasattr(tables, "GOVERNMENT_CONTRACT_EVIDENCE_COLUMNS")
    assert not hasattr(tables, "GOVERNMENT_CONTRACT_SUMMARY_COLUMNS")


def test_each_country_asset_depends_only_on_the_sources_it_reads() -> None:
    from dagster_v3.defs.company_signals.procurement import (
        COUNTRY_CONTRACT_ASSETS,
    )

    deps = {
        asset.key.to_user_string(): {
            dep.asset_key for dep in asset.specs_by_key[asset.key].deps
        }
        for asset in COUNTRY_CONTRACT_ASSETS
    }

    assert deps["se_government_contract_signals_clickhouse"] == {
        dg.AssetKey("sweden_uhm_procurement_awards_clickhouse"),
        dg.AssetKey("ted_publish_clickhouse"),
    }
    assert deps["fi_government_contract_signals_clickhouse"] == {
        dg.AssetKey("finland_hilma_clickhouse"),
        dg.AssetKey("ted_publish_clickhouse"),
    }
    # Norway declaring a Swedish dependency would stall it on unrelated data.
    assert deps["no_government_contract_signals_clickhouse"] == {
        dg.AssetKey("norway_doffin_notices_clickhouse"),
        dg.AssetKey("ted_publish_clickhouse"),
    }
    # Brazil has no TED at all, being outside the EU.
    assert deps["br_government_contract_signals_clickhouse"] == {
        dg.AssetKey("brazil_pncp_contracts_clickhouse"),
    }


def test_coverage_caveats_state_what_the_country_is_missing() -> None:
    """A caveat that has gone stale is worse than none: it asserts something
    untrue about coverage. Finland reads Hilma now, so it must not still say
    the register is unjoined.
    """
    finland = COUNTRY_PROCUREMENT_RULES["FI"].coverage_caveat
    assert "not yet joined" not in finland
    assert "Hilma" in finland
    # Hilma does publish a realized lot value, so a caveat saying otherwise is
    # not merely vague -- it asserts something untrue.
    assert "no amount per winner" not in finland
    assert "realized value per lot" in finland

    # What each country's reader most needs to know about its values.
    assert "no contract value" in COUNTRY_PROCUREMENT_RULES["SE"].coverage_caveat
    assert "Doffin" in COUNTRY_PROCUREMENT_RULES["NO"].coverage_caveat
    assert "no TED" in COUNTRY_PROCUREMENT_RULES["BR"].coverage_caveat


def test_sweden_admits_its_counts_are_notices_not_contracts() -> None:
    """Sweden's two registers publish no reference to each other, so a contract
    advertised in both is counted twice -- checked, not assumed: UHM carries no
    TED number, Swedish TED notices carry no Upphandlings-ID, and a
    buyer/date/title hash matched zero of 242,699 rows.

    A page whose purpose is explaining where data comes from must not carry a
    caveat that misleads, so this has to be said before any provenance UI ships.
    """
    sweden = COUNTRY_PROCUREMENT_RULES["SE"].coverage_caveat

    assert "counted twice" in sweden
    assert "notices, not of distinct contracts" in sweden

    # Finland is the contrast, and must not be given Sweden's warning: its
    # published ted_number does collapse the duplicates.
    finland = COUNTRY_PROCUREMENT_RULES["FI"].coverage_caveat
    assert "counted twice" not in finland
    assert "counted once" in finland


def test_every_ted_country_admits_it_drops_foreign_winners() -> None:
    """Each country view joins only its own company register, so a contract won
    by a foreign company lands in no view at all -- not the notice country's,
    and not the winner's, whose register may not even be loaded. Unlike a NULL
    nothing marks the gap, which is why it has to be said in words.

    Measured 2026-07-27: 3,354 SE + 2,266 FI + 1,059 NO winner rows.
    """
    for country in ("SE", "FI", "NO"):
        caveat = COUNTRY_PROCUREMENT_RULES[country].coverage_caveat
        assert "absent" in caveat, country
        assert "companies are" in caveat, country
