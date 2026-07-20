"""Heavy bulk jobs carry the workload tag that run_queue throttles.

dagster.yaml caps runs tagged corpscout/workload=heavy-bulk so a synchronized
storm of multi-GB snapshot jobs can't occupy the whole run queue at once.
"""

HEAVY_BULK_JOBS = (
    "wikidata_company_seed_weekly_job",
    "gleif_reference_bootstrap_job",
    "gleif_reference_delta_job",
    "uk_companies_house_register_job",
    "uk_companies_house_financials_job",
    "france_sirene_register_job",
    "sweden_company_refresh_job",
    "sweden_financial_backfill_job",
    "sweden_financial_current_year_job",
    "czech_ares_register_job",
    "estonia_ar_general_data_job",
    "companies_all_job",
    "esef_filings_refresh_job",
    "esef_filings_backfill_job",
)


def test_heavy_bulk_jobs_carry_the_throttled_workload_tag() -> None:
    from dagster_v3.definitions import defs as load_defs
    from dagster_v3.defs.common.tags import HEAVY_BULK_RUN_TAGS

    repo = load_defs().get_repository_def()
    for job_name in HEAVY_BULK_JOBS:
        job = repo.get_job(job_name)
        for key, value in HEAVY_BULK_RUN_TAGS.items():
            assert job.tags.get(key) == value, job_name
