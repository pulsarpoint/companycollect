import dagster as dg

from dagster_v3.defs.commoncrawl_rdap.assets import commoncrawl_ip_rdap_networks


defs = dg.Definitions(assets=[commoncrawl_ip_rdap_networks])
