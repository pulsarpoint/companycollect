import dagster as dg

from dagster_corpscout.registry import all_asset_checks, all_assets, all_jobs, all_schedules
from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.resources.rustfs import RustFSResource

# Explicit (instead of Dagster's implicit default) so its existence and default
# status are part of the tested platform contract. It evaluates every asset
# with an automation_condition; enable it in the UI to activate eager cascades.
automation_sensor = dg.AutomationConditionSensorDefinition(
    name="automation_condition_sensor",
    target=dg.AssetSelection.all(),
    default_status=dg.DefaultSensorStatus.STOPPED,
)

defs = dg.Definitions(
    assets=all_assets,
    asset_checks=all_asset_checks,
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=[automation_sensor],
    resources={
        "rustfs": RustFSResource(
            endpoint_url=dg.EnvVar("CORPSCOUT_S3_ENDPOINT"),
            access_key=dg.EnvVar("CORPSCOUT_S3_ACCESS_KEY"),
            secret_key=dg.EnvVar("CORPSCOUT_S3_SECRET_KEY"),
        ),
        "clickhouse": ClickHouseResource(
            host=dg.EnvVar("CLICKHOUSE_HOST"),
            port=dg.EnvVar("CLICKHOUSE_PORT"),
            username=dg.EnvVar("CLICKHOUSE_USER"),
            password=dg.EnvVar("CLICKHOUSE_PASSWORD"),
            database=dg.EnvVar("CLICKHOUSE_DATABASE"),
            secure=dg.EnvVar("CLICKHOUSE_SECURE"),
        ),
    },
)
