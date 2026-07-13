import dagster as dg

from dagster_v3.defs.commoncrawl_geoip.assets import (
    commoncrawl_ip_geoip,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


defs = dg.Definitions(
    assets=[
        commoncrawl_ip_geoip,
    ],
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
    },
)
