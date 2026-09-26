import dagster as dg

from dagster_v3.defs.commoncrawl_geoip.freshness import (
    geolite2_databases_fresh,
    geolite2_freshness_job,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


defs = dg.Definitions(
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
    },
    asset_checks=[geolite2_databases_fresh],
    jobs=[geolite2_freshness_job],
)
