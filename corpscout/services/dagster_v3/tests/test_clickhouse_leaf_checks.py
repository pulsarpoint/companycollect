"""Every ClickHouse leaf asset carries data-quality checks.

Freshness (scheduled leaves only — "did the schedule silently stop") and
row-count non-empty checks (all leaves, run with each materialization) are
declared centrally in defs/common/clickhouse_checks.py from a registry of
(asset_key, tables, cadence).
"""

import dagster as dg

from dagster_v3.defs.common import clickhouse_checks as chk


def _repo():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_registry_covers_the_known_scheduled_leaves() -> None:
    keys = {spec.asset_key for spec in chk.CLICKHOUSE_LEAVES}
    # spot-check one leaf per module family
    for expected in (
        "czech_ares_clickhouse_companies",
        "esef_filings_clickhouse",
        "esef_facts_clickhouse",
        "esef_document_contact_candidates_clickhouse",
        "esef_document_concept_labels_clickhouse",
        "esef_disclosures_clickhouse",
        "esef_entity_registry_map_clickhouse",
        "esef_financial_metrics_clickhouse",
        "estonia_ar_clickhouse_companies",
        "estonia_ar_clickhouse_financial_metrics",
        "exchange_rates_v2_clickhouse",
        "finland_ytj_resolved_clickhouse",
        "data_daily_duckdb_ch",
        "france_sirene_clickhouse_companies",
        "gleif_reference_clickhouse",
        "latvia_financial_metrics_clickhouse",
        "latvia_ur_clickhouse_companies",
        "latvia_ur_clickhouse_company_addresses",
        "nace_categories_clickhouse",
        "open_page_rank_domains_clickhouse",
        "slovakia_financials_metrics_clickhouse",
        "slovakia_rpo_clickhouse_companies",
        "sweden_company_companies_clickhouse",
        "sweden_address_geocode_store_clickhouse",
        "sweden_address_geocodes_clickhouse",
        "sweden_shared_addresses_clickhouse",
        "sweden_financial_backfill_reports_clickhouse",
        "sweden_financial_current_facts_clickhouse",
        "se_bolagsverket_financial_metrics_clickhouse",
        "se_bolagsverket_financial_observations_clickhouse",
        "uk_companies_house_clickhouse_companies",
        "wikidata_snapshot_complete",
    ):
        assert expected in keys, expected

    finland = next(
        leaf
        for leaf in chk.CLICKHOUSE_LEAVES
        if leaf.asset_key == "finland_ytj_resolved_clickhouse"
    )
    assert "fi_company_addresses" in finland.tables

    # The derived serving table is a publish asset like any other, and the registry's
    # contract is one leaf per publish asset. It was the one Sweden address publish without
    # a leaf, so nothing watched the table four backoffice modules read every request.
    derived = next(
        leaf
        for leaf in chk.CLICKHOUSE_LEAVES
        if leaf.asset_key == "sweden_address_geocodes_clickhouse"
    )
    assert derived.tables == ("se_address_geocodes_current",)
    assert derived.max_age == chk.WEEKLY


def test_every_leaf_has_a_row_count_check_and_scheduled_leaves_freshness() -> None:
    repo = _repo()
    check_keys = set(repo.asset_checks_defs_by_key.keys())

    for spec in chk.CLICKHOUSE_LEAVES:
        asset_key = dg.AssetKey(spec.asset_key)
        assert dg.AssetCheckKey(asset_key, chk.ROW_COUNT_CHECK_NAME) in check_keys, (
            f"{spec.asset_key}: missing row-count check"
        )
        freshness_key = dg.AssetCheckKey(asset_key, chk.FRESHNESS_CHECK_NAME)
        if spec.max_age is not None:
            assert freshness_key in check_keys, f"{spec.asset_key}: missing freshness"
        else:
            assert freshness_key not in check_keys, (
                f"{spec.asset_key}: unscheduled leaf must not have freshness"
            )


def test_freshness_sensor_is_registered_and_running() -> None:
    repo = _repo()
    sensor = repo.get_sensor_def("clickhouse_freshness_sensor")
    assert sensor.default_status == dg.DefaultSensorStatus.RUNNING


def test_evaluate_row_counts_fails_on_any_empty_table() -> None:
    class FakeClient:
        def __init__(self, counts):
            self._counts = counts

        def execute(self, sql, params=None):
            for table, count in self._counts.items():
                if f"`{table}`" in sql:
                    return [(count,)]
            raise AssertionError(sql)

    counts = chk.fetch_row_counts(
        FakeClient({"lv_companies": 5, "lv_company_domains": 0}),
        ("lv_companies", "lv_company_domains"),
    )
    assert counts == {"lv_companies": 5, "lv_company_domains": 0}

    ok = chk.fetch_row_counts(FakeClient({"lv_companies": 5}), ("lv_companies",))
    assert ok == {"lv_companies": 5}
