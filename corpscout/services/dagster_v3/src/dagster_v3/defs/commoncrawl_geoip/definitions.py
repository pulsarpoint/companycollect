import dagster as dg

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_geoip.freshness import (
    geolite2_databases_fresh,
    geolite2_freshness_job,
)
from dagster_v3.defs.commoncrawl_geoip.install import (
    BUCKET,
    geolite2_databases,
    geolite2_install_job,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


defs = dg.Definitions(
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
        "geolite2_object_store": ObjectStoreResource(bucket=BUCKET),
    },
    assets=[geolite2_databases],
    asset_checks=[geolite2_databases_fresh],
    jobs=[geolite2_freshness_job, geolite2_install_job],
)
