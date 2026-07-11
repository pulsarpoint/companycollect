import dagster as dg
from dagster_clickhouse import ClickhouseResource


CT_HOSTNAME_SHARD_COUNT = 16
COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS = dg.StaticPartitionsDefinition(
    [f"shard_{shard_index:02d}" for shard_index in range(CT_HOSTNAME_SHARD_COUNT)]
)
COMMONCRAWL_HOSTNAME_POOL = "commoncrawl_hostname_sync"
COMMONCRAWL_HOSTNAME_GROUP = "commoncrawl_dns"

CTLOGS_HOSTNAMES_ASSET = dg.AssetSpec(
    key="ctlogs_hostnames",
    description="Certificate Transparency hostnames stored in ctlogs.hostnames.",
    group_name=COMMONCRAWL_HOSTNAME_GROUP,
    kinds={"clickhouse", "ct"},
)
COMMONCRAWL_DOMAINS_ASSET = dg.AssetSpec(
    key="commoncrawl_domains",
    description="Root domains selected from Common Crawl for DNS scanning.",
    group_name=COMMONCRAWL_HOSTNAME_GROUP,
    kinds={"clickhouse", "commoncrawl", "dns"},
)

CT_HOSTNAME_UPSERT_SQL = """
INSERT INTO corpscout.commoncrawl_domain_hostnames
(
    root_domain,
    label,
    discovery_source,
    first_seen,
    last_seen,
    last_resolved
)
SELECT
    registered_domain AS root_domain,
    substring(
        fqdn_normalized,
        1,
        length(fqdn_normalized) - length(domain_normalized) - 1
    ) AS label,
    'ct' AS discovery_source,
    first_seen,
    last_seen,
    toDateTime64(0, 3, 'UTC') AS last_resolved
FROM
(
    SELECT
        registered_domain,
        lower(trimRight(registered_domain, '.')) AS domain_normalized,
        lower(trimRight(fqdn, '.')) AS fqdn_normalized,
        max(is_wildcard) AS is_wildcard,
        min(first_seen) AS first_seen,
        max(last_seen) AS last_seen
    FROM ctlogs.hostnames
    WHERE cityHash64(registered_domain) %% %(shard_count)s = %(shard_index)s
      AND registered_domain IN
      (
          SELECT DISTINCT root_domain
          FROM corpscout.commoncrawl_domains
          WHERE root_domain != ''
      )
    GROUP BY
        registered_domain,
        fqdn
)
WHERE is_wildcard = 0
  AND domain_normalized != ''
  AND fqdn_normalized != domain_normalized
  AND endsWith(fqdn_normalized, concat('.', domain_normalized))
  AND position(fqdn_normalized, '*') = 0
"""


@dg.asset(
    name="commoncrawl_domain_hostnames",
    deps=[CTLOGS_HOSTNAMES_ASSET.key, COMMONCRAWL_DOMAINS_ASSET.key],
    description=(
        "Adds non-wildcard CT subdomains for root domains present in the Common Crawl "
        "DNS domain set. Existing worker discoveries are preserved."
    ),
    group_name=COMMONCRAWL_HOSTNAME_GROUP,
    kinds={"clickhouse", "ct", "dns"},
    partitions_def=COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=COMMONCRAWL_HOSTNAME_POOL,
)
def commoncrawl_domain_hostnames(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Merge one CT hostname shard into the Common Crawl hostname inventory."""
    shard_index = ct_hostname_shard_index(context.partition_key)
    parameters = {
        "shard_count": CT_HOSTNAME_SHARD_COUNT,
        "shard_index": shard_index,
    }

    context.log.info(
        "Starting Common Crawl hostname sync: partition=%s shard_index=%s",
        context.partition_key,
        shard_index,
    )
    with clickhouse.get_connection() as client:
        client.execute(CT_HOSTNAME_UPSERT_SQL, parameters)
        progress = client.last_query.progress
        metadata = {
            "source_table": "ctlogs.hostnames",
            "membership_table": "corpscout.commoncrawl_domains",
            "destination_table": "corpscout.commoncrawl_domain_hostnames",
            "shard_index": shard_index,
            "shard_count": CT_HOSTNAME_SHARD_COUNT,
            "written_rows": progress.written_rows,
            "written_bytes": progress.written_bytes,
            "elapsed_seconds": client.last_query.elapsed,
        }

    context.log.info(
        "Completed Common Crawl hostname sync: partition=%s metadata=%s",
        context.partition_key,
        metadata,
    )
    return dg.MaterializeResult(metadata=metadata)


def ct_hostname_shard_index(partition_key: str) -> int:
    """Return the validated CT physical shard index from a Dagster partition key."""
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "shard" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid CT hostname shard partition: {partition_key!r}")

    shard_index = int(suffix)
    if not 0 <= shard_index < CT_HOSTNAME_SHARD_COUNT:
        raise ValueError(f"CT hostname shard index out of range: {shard_index}")
    return shard_index
