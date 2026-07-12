import dagster as dg

from dagster_v3.defs.commoncrawl_geoip.assets import (
    commoncrawl_ip_geoip,
)
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_ip import COMMONCRAWL_IP_ADDRESSES_ASSET


defs = dg.Definitions(
    assets=[
        COMMONCRAWL_IP_ADDRESSES_ASSET,
        commoncrawl_ip_geoip,
    ],
    resources={
        "maxmind_geoip": MaxMindDatabaseResource(),
    },
)
