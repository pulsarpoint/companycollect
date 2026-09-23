import dagster as dg

from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


defs = dg.Definitions(
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
    },
)
