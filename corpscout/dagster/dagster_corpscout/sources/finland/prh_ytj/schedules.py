import dagster as dg

from dagster_corpscout.sources.finland.prh_ytj.jobs import pipeline_job

pull_schedule = dg.ScheduleDefinition(
    name="finland_prhytj_pull_schedule",
    job=pipeline_job,
    cron_schedule="0 3 * * 1",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
