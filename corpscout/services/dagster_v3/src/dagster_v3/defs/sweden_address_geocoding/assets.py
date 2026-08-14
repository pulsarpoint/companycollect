from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.sweden_address_geocoding.source import (
    LantmaterietAddressResource,
)

GROUP_NAME = "sweden_address_geocoding"


@dg.asset(
    group_name=GROUP_NAME,
    kinds={"python", "s3", "zip", "geopackage", "lantmateriet", "stac"},
    description=(
        "Discovers all Swedish municipality address archives through the "
        "Lantmäteriet STAC catalog and stores immutable authenticated ZIP "
        "snapshots plus a run manifest in RustFS/S3."
    ),
)
def sweden_lantmateriet_address_archives_s3(
    context: dg.AssetExecutionContext,
    sweden_lantmateriet_addresses: LantmaterietAddressResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    return sweden_lantmateriet_addresses.download_snapshot(
        object_store=object_store,
        run_id=context.run_id,
        retrieved_at=datetime.now(UTC),
        log_info=context.log.info,
    )


sweden_lantmateriet_addresses_job = dg.define_asset_job(
    name="sweden_lantmateriet_addresses_job",
    selection=dg.AssetSelection.assets("sweden_lantmateriet_address_archives_s3"),
)

sweden_lantmateriet_addresses_weekly = dg.ScheduleDefinition(
    name="sweden_lantmateriet_addresses_weekly",
    job=sweden_lantmateriet_addresses_job,
    cron_schedule="40 7 * * 1",
    execution_timezone="Europe/Stockholm",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description=(
        "Weekly Lantmäteriet municipality address archive refresh. Enable only "
        "after the Geotorget product order has received legal approval."
    ),
)


defs = dg.Definitions(
    assets=[sweden_lantmateriet_address_archives_s3],
    jobs=[sweden_lantmateriet_addresses_job],
    schedules=[sweden_lantmateriet_addresses_weekly],
    resources={
        "sweden_lantmateriet_addresses": LantmaterietAddressResource(),
    },
)
