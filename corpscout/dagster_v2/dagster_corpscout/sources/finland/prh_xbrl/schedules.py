import dagster as dg

from dagster_corpscout.sources.finland.prh_xbrl.jobs import pull_window_job

# Materializes the just-closed registration month; eager automation cascades
# statement_tables and financial_metrics. STOPPED until production-ready.
pull_window_schedule = dg.build_schedule_from_partitioned_job(
    pull_window_job,
    name="finland_prh_xbrl_pull_window_schedule",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
