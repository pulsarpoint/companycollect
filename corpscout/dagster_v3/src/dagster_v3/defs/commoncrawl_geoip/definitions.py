import dagster as dg

from dagster_v3.defs.commoncrawl_geoip.assets import (
    COMMONCRAWL_IP_ADDRESSES_ASSET,
    commoncrawl_ip_geoip,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource


defs = dg.Definitions(
    assets=[
        COMMONCRAWL_IP_ADDRESSES_ASSET,
        commoncrawl_ip_geoip,
    ],
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
    },
)
