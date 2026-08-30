import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.webtech.assets import (
    commoncrawl_webtech_scan_job,
    commoncrawl_webtech_scan_results,
)


defs = dg.Definitions(
    assets=[commoncrawl_webtech_scan_results],
    jobs=[commoncrawl_webtech_scan_job],
    resources={
        "webtech_object_store": ObjectStoreResource(bucket="webtech"),
    },
)
