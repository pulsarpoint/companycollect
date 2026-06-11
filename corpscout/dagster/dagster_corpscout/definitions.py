import dagster as dg

from dagster_corpscout.resources.rustfs import RustFSResource
from dagster_corpscout.sources.finland_prhytj.assets import raw_snapshot
from dagster_corpscout.sources.finland_prhytj.schedules import pull_job, pull_schedule

defs = dg.Definitions(
    assets=[raw_snapshot],
    jobs=[pull_job],
    schedules=[pull_schedule],
    resources={
        "rustfs": RustFSResource(
            endpoint_url=dg.EnvVar("CORPSCOUT_S3_ENDPOINT"),
            access_key=dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY"),
            secret_key=dg.EnvVar("CORPSCOUT_S3_SECRET_KEY"),
        )
    },
)
