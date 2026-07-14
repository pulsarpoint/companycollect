import dagster as dg

from dagster_v3.defs.commoncrawl_hostnames.assets import (
    commoncrawl_domain_hostnames,
)
from dagster_v3.defs.commoncrawl_hostnames.observation import (
    commoncrawl_hostname_sources_observation,
    commoncrawl_hostname_sources_observation_job,
)


defs = dg.Definitions(
    assets=[
        commoncrawl_hostname_sources_observation,
        commoncrawl_domain_hostnames,
    ],
    jobs=[commoncrawl_hostname_sources_observation_job],
)
