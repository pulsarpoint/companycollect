import dagster as dg

from dagster_corpscout.sources.finland_prhytj.assets import raw_snapshot

pull_job = dg.define_asset_job(
    name="finland_prhytj_pull",
    selection=[raw_snapshot],
)

pull_schedule = dg.ScheduleDefinition(
    name="finland_prhytj_pull_schedule",
    job=pull_job,
    cron_schedule="0 3 * * 1",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
