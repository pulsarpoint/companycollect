import dagster as dg

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    COMMONCRAWL_DOMAINS_ASSET,
    CTLOGS_HOSTNAMES_ASSET,
    DNS_RECORD_OBSERVATIONS_ASSET,
    commoncrawl_domain_hostnames,
)


defs = dg.Definitions(
    assets=[
        CTLOGS_HOSTNAMES_ASSET,
        COMMONCRAWL_DOMAINS_ASSET,
        DNS_RECORD_OBSERVATIONS_ASSET,
        commoncrawl_domain_hostnames,
    ]
)
