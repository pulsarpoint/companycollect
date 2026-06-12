import dagster as dg

from dagster_corpscout.sources.finland.prh_ytj.jobs import pull_job

# Cron enters the graph at the raw layer only; the eager automation conditions
# on normalized/code_lists/mapping/serving cascade the rest.
pull_schedule = dg.ScheduleDefinition(
    name="finland_prhytj_pull_schedule",
    job=pull_job,
    cron_schedule="0 3 * * 1",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
