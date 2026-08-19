"""Data-quality checks for every ClickHouse leaf asset, driven by one registry.

Two checks per leaf:
- **Freshness** (scheduled leaves only): a last-update freshness check with a
  cadence-aware bound, evaluated by ``clickhouse_freshness_sensor`` without
  needing a materialization — this is the "did the schedule silently stop"
  detector. WARN severity (the run didn't fail; the pipeline stopped moving).
- **Row count**: runs with each materialization and fails when any exported
  ``corpscout`` table is empty. Complements the exporter's empty-input guard
  by also covering the ClickHouse-native INSERT-SELECT paths
  (``contact_extraction.replace_table_from_select``).

New sources: add one ``ClickhouseLeaf`` per export/publish asset. ``max_age``
is None for leaves whose job has no schedule (freshness would only be noise).
"""

from dataclasses import dataclass
from datetime import timedelta

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE

FRESHNESS_CHECK_NAME = "freshness_check"
ROW_COUNT_CHECK_NAME = "clickhouse_tables_non_empty"

# Cadence-aware staleness bounds: one missed firing plus slack.
DAILY = timedelta(days=2)
WEEKDAYS = timedelta(days=4)  # covers the weekend gap
WEEKLY = timedelta(days=9)
MONTHLY = timedelta(days=35)


@dataclass(frozen=True)
class ClickhouseLeaf:
    asset_key: str
    tables: tuple[str, ...]
    max_age: timedelta | None  # None = unscheduled leaf → row-count check only


CLICKHOUSE_LEAVES: tuple[ClickhouseLeaf, ...] = (
    # country_people — daily 08:00 UTC; identifiers may legitimately be empty
    ClickhouseLeaf(
        "country_person_observations_clickhouse",
        ("country_person_observation",),
        DAILY,
    ),
    ClickhouseLeaf(
        "country_person_matches_clickhouse", ("country_person_match",), DAILY
    ),
    ClickhouseLeaf("country_people_clickhouse", ("country_person",), DAILY),
    # company_people_all — daily 07:45 UTC
    ClickhouseLeaf("company_people_all_clickhouse", ("company_people_all",), DAILY),
    # czech_ares — monthly 17th
    ClickhouseLeaf("czech_ares_clickhouse_companies", ("cz_companies",), MONTHLY),
    ClickhouseLeaf(
        "czech_ares_clickhouse_company_contacts",
        ("cz_company_contacts", "cz_company_domains"),
        MONTHLY,
    ),
    ClickhouseLeaf("czech_ares_clickhouse_industries", ("cz_industries",), MONTHLY),
    # esef_filings — unscheduled in Task 5; jobs/schedule land in Task 7
    ClickhouseLeaf("esef_filings_clickhouse", ("esef_filings",), None),
    ClickhouseLeaf("esef_facts_clickhouse", ("esef_facts",), None),
    ClickhouseLeaf(
        "esef_entity_registry_map_clickhouse", ("esef_entity_registry_map",), None
    ),
    ClickhouseLeaf(
        "esef_financial_metrics_clickhouse", ("esef_financial_metrics",), None
    ),
    # estonia_ar — register daily, general data monthly 8th
    ClickhouseLeaf("estonia_ar_clickhouse_companies", ("ee_companies",), DAILY),
    ClickhouseLeaf(
        "estonia_ar_clickhouse_company_contacts", ("ee_company_contacts",), MONTHLY
    ),
    ClickhouseLeaf(
        "estonia_ar_clickhouse_company_domains", ("ee_company_domains",), MONTHLY
    ),
    ClickhouseLeaf("estonia_ar_clickhouse_industries", ("ee_industries",), MONTHLY),
    # estonia_financial — monthly 5th (asset keys carry the estonia_ar_ prefix)
    ClickhouseLeaf(
        "estonia_ar_clickhouse_financial_statements",
        ("ee_financial_statements",),
        MONTHLY,
    ),
    ClickhouseLeaf(
        "estonia_ar_clickhouse_financial_metrics", ("ee_financial_metrics",), MONTHLY
    ),
    # exchange_rates_v2 — weekdays 18:30
    ClickhouseLeaf("exchange_rates_v2_clickhouse", ("exchange_rates",), WEEKDAYS),
    # finland_ytj — daily 04:45
    ClickhouseLeaf(
        "finland_ytj_resolved_clickhouse",
        (
            "fi_companies",
            "fi_company_addresses",
            "fi_names",
            "fi_websites",
            "fi_industries",
        ),
        DAILY,
    ),
    ClickhouseLeaf(
        "finland_ytj_clickhouse_canonical_contacts",
        ("fi_company_contacts", "fi_company_domains"),
        DAILY,
    ),
    # finland_xbrl — incremental download, parse, and publish daily 06:00
    ClickhouseLeaf(
        "data_daily_duckdb_ch", ("fi_xbrl_financial_statement_listings",), DAILY
    ),
    ClickhouseLeaf(
        "data_snapshot_duckdb_ch", ("fi_xbrl_financial_statement_listings",), None
    ),
    ClickhouseLeaf("fi_financial_statements_ch", ("fi_financial_statements",), DAILY),
    ClickhouseLeaf("fi_xbrl_contexts_ch", ("fi_xbrl_contexts",), DAILY),
    ClickhouseLeaf("fi_xbrl_units_ch", ("fi_xbrl_units",), DAILY),
    ClickhouseLeaf("fi_xbrl_facts_ch", ("fi_xbrl_facts_raw",), DAILY),
    ClickhouseLeaf("fi_xbrl_taxonomy_codes_ch", ("fi_xbrl_taxonomy_codes",), DAILY),
    ClickhouseLeaf("fi_financial_metrics_ch", ("fi_financial_metrics",), DAILY),
    # france_sirene — monthly 6th
    ClickhouseLeaf("france_sirene_clickhouse_companies", ("fr_companies",), MONTHLY),
    ClickhouseLeaf("france_sirene_clickhouse_industries", ("fr_industries",), MONTHLY),
    # France BCE/INPI financial ratios — monthly 12th
    ClickhouseLeaf(
        "france_financial_metrics_clickhouse", ("fr_financial_metrics",), MONTHLY
    ),
    # France Annuaire enrichments — daily 04:25
    ClickhouseLeaf(
        "france_annuaire_enrichments_clickhouse",
        ("fr_company_enrichments",),
        DAILY,
    ),
    # gleif — daily 20:30
    ClickhouseLeaf(
        "gleif_reference_clickhouse",
        (
            "gleif_lei_records",
            "gleif_lei_names",
            "gleif_lei_addresses",
            "gleif_lei_identifiers",
            "gleif_lei_relationships",
            "gleif_lei_relationship_periods",
            "gleif_lei_reporting_exceptions",
            "gleif_lei_issuers",
            "gleif_code_list_entries",
        ),
        DAILY,
    ),
    # latvia_financial — weekly Mon
    ClickhouseLeaf(
        "latvia_financial_statements_clickhouse", ("lv_financial_statements",), WEEKLY
    ),
    ClickhouseLeaf(
        "latvia_financial_metrics_clickhouse", ("lv_financial_metrics",), WEEKLY
    ),
    # latvia_ur — daily 04:30
    ClickhouseLeaf("latvia_ur_clickhouse_companies", ("lv_companies",), DAILY),
    ClickhouseLeaf(
        "latvia_ur_clickhouse_company_addresses",
        ("lv_company_addresses",),
        DAILY,
    ),
    ClickhouseLeaf(
        "latvia_ur_clickhouse_company_contacts",
        ("lv_company_contacts", "lv_company_domains"),
        DAILY,
    ),
    # nace — weekly Mon
    ClickhouseLeaf("nace_categories_clickhouse", ("nace_categories",), WEEKLY),
    # UN Comtrade — monthly 10th (schedule default-STOPPED until first live run)
    ClickhouseLeaf(
        "un_comtrade_annual_totals_clickhouse",
        ("un_comtrade_annual_availability", "un_comtrade_annual_totals"),
        MONTHLY,
    ),
    # open_page_rank — weekly Sun (schedule default-STOPPED; WARN is the signal)
    ClickhouseLeaf(
        "open_page_rank_domains_clickhouse", ("open_page_rank_domains",), WEEKLY
    ),
    # slovakia_financials — daily 05:00
    ClickhouseLeaf(
        "slovakia_financials_metrics_clickhouse", ("sk_financial_metrics",), DAILY
    ),
    # slovakia_rpo — weekly Mon
    ClickhouseLeaf("slovakia_rpo_clickhouse_companies", ("sk_companies",), WEEKLY),
    ClickhouseLeaf("slovakia_rpo_clickhouse_industries", ("sk_industries",), WEEKLY),
    # sweden_company — weekly Mon
    ClickhouseLeaf("sweden_company_companies_clickhouse", ("se_companies",), WEEKLY),
    ClickhouseLeaf(
        "sweden_company_addresses_clickhouse", ("se_company_addresses",), WEEKLY
    ),
    ClickhouseLeaf("sweden_company_industries_clickhouse", ("se_industries",), WEEKLY),
    ClickhouseLeaf(
        "sweden_company_address_geocodes_clickhouse",
        ("se_company_address_geocodes",),
        WEEKLY,
    ),
    ClickhouseLeaf(
        "sweden_company_address_geocode_results_clickhouse",
        ("se_company_address_geocode_results",),
        WEEKLY,
    ),
    ClickhouseLeaf(
        "sweden_shared_addresses_clickhouse",
        ("se_addresses_current", "se_company_address_links_current"),
        WEEKLY,
    ),
    # sweden_financial — reports/facts are scoped incremental exports. The
    # source-owned observation and other derived publishers are unscheduled
    # full rebuilds, so they receive row-count checks but no freshness check.
    ClickhouseLeaf(
        "sweden_financial_backfill_reports_clickhouse",
        ("se_financial_reports",),
        None,
    ),
    ClickhouseLeaf(
        "sweden_financial_current_reports_clickhouse",
        ("se_financial_reports",),
        None,
    ),
    ClickhouseLeaf(
        "sweden_financial_backfill_facts_clickhouse", ("se_financial_facts",), None
    ),
    ClickhouseLeaf(
        "sweden_financial_current_facts_clickhouse", ("se_financial_facts",), None
    ),
    ClickhouseLeaf(
        "sweden_financial_metrics_clickhouse", ("se_financial_metrics",), None
    ),
    ClickhouseLeaf(
        "se_bolagsverket_financial_observations_clickhouse",
        ("se_bolagsverket_financial_observations",),
        None,
    ),
    ClickhouseLeaf(
        "se_financial_report_signatories_clickhouse",
        ("se_financial_report_signatories",),
        None,
    ),
    ClickhouseLeaf("se_company_audits_clickhouse", ("se_company_audits",), None),
    # uk_companies_house — register monthly 7th, financials monthly 8th, daily 09:00
    ClickhouseLeaf(
        "uk_companies_house_clickhouse_companies", ("gb_companies",), MONTHLY
    ),
    ClickhouseLeaf(
        "uk_companies_house_clickhouse_industries", ("gb_industries",), MONTHLY
    ),
    ClickhouseLeaf(
        "uk_companies_house_clickhouse_financial_metrics",
        ("gb_financial_metrics",),
        MONTHLY,
    ),
    ClickhouseLeaf(
        "uk_companies_house_accounts_incremental", ("gb_financial_metrics",), DAILY
    ),
    ClickhouseLeaf(
        "uk_companies_house_pdf_financial_metrics", ("gb_financial_metrics",), None
    ),
    ClickhouseLeaf(
        "uk_companies_house_api_financial_metrics", ("gb_financial_metrics",), None
    ),
    # wikidata — weekly Mon
    ClickhouseLeaf(
        "wikidata_snapshot_complete",
        (
            "wikidata_companies",
            "wikidata_exchanges",
            "wikidata_company_listings",
            "wikidata_company_identifiers",
            "wikidata_company_websites",
            "wikidata_company_relationships",
            "wikidata_seed_extraction_runs",
        ),
        WEEKLY,
    ),
    ClickhouseLeaf(
        "wikidata_clickhouse_canonical_contacts",
        ("wikidata_company_contacts", "wikidata_company_domains"),
        WEEKLY,
    ),
)


def fetch_row_counts(client, tables) -> dict[str, int]:
    counts: dict[str, int] = {}
    for table in tables:
        rows = client.execute(f"SELECT count() FROM `{RESOLVED_DATABASE}`.`{table}`")
        counts[table] = int(rows[0][0])
    return counts


def _build_row_count_check(leaf: ClickhouseLeaf) -> dg.AssetChecksDefinition:
    @dg.asset_check(
        asset=dg.AssetKey(leaf.asset_key),
        name=ROW_COUNT_CHECK_NAME,
        description=(
            "Fails when any exported ClickHouse table is empty: "
            + ", ".join(leaf.tables)
        ),
    )
    def _check(
        context: dg.AssetCheckExecutionContext,
        clickhouse: ClickhouseResource,
    ) -> dg.AssetCheckResult:
        with clickhouse.get_connection() as client:
            counts = fetch_row_counts(client, leaf.tables)
        empty_tables = sorted(table for table, count in counts.items() if count == 0)
        return dg.AssetCheckResult(
            passed=not empty_tables,
            severity=dg.AssetCheckSeverity.ERROR,
            metadata={f"rows_{table}": count for table, count in counts.items()},
        )

    return _check


_row_count_checks = [_build_row_count_check(leaf) for leaf in CLICKHOUSE_LEAVES]

_freshness_checks: list[dg.AssetChecksDefinition] = []
for _max_age in (DAILY, WEEKDAYS, WEEKLY, MONTHLY):
    _asset_keys = [
        dg.AssetKey(leaf.asset_key)
        for leaf in CLICKHOUSE_LEAVES
        if leaf.max_age == _max_age
    ]
    if _asset_keys:
        _freshness_checks.extend(
            dg.build_last_update_freshness_checks(
                assets=_asset_keys,
                lower_bound_delta=_max_age,
                severity=dg.AssetCheckSeverity.WARN,
            )
        )

clickhouse_freshness_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=_freshness_checks,
    name="clickhouse_freshness_sensor",
    default_status=dg.DefaultSensorStatus.RUNNING,
)

defs = dg.Definitions(
    asset_checks=[*_row_count_checks, *_freshness_checks],
    sensors=[clickhouse_freshness_sensor],
)
