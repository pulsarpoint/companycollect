from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource


HOSTNAME_SHARD_COUNT = 16
COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS = dg.StaticPartitionsDefinition(
    [f"shard_{shard_index:02d}" for shard_index in range(HOSTNAME_SHARD_COUNT)]
)
COMMONCRAWL_HOSTNAME_POOL = "commoncrawl_hostname_sync"
COMMONCRAWL_HOSTNAME_GROUP = "commoncrawl_dns"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

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
DNS_RECORD_OBSERVATIONS_ASSET = dg.AssetSpec(
    key="commoncrawl_domain_dns_record_observations",
    description="Retry-safe DNS record observations written by cc-dns-scan and cc-dns-axfr.",
    group_name=COMMONCRAWL_HOSTNAME_GROUP,
    kinds={"clickhouse", "dns", "axfr"},
)

CT_HOSTNAME_UPSERT_SQL = """
INSERT INTO corpscout.commoncrawl_domain_hostnames
(
    root_domain,
    label,
    discovery_source,
    first_seen,
    last_seen,
    last_resolved,
    last_not_after
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
    toDateTime64(0, 3, 'UTC') AS last_resolved,
    last_not_after
FROM
(
    SELECT
        registered_domain,
        lower(trimRight(registered_domain, '.')) AS domain_normalized,
        lower(trimRight(fqdn, '.')) AS fqdn_normalized,
        max(is_wildcard) AS is_wildcard,
        min(first_seen) AS first_seen,
        max(last_seen) AS last_seen,
        max(last_not_after) AS last_not_after,
        max(last_ingested_at) AS last_ingested_at
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
  AND last_ingested_at <= %(ct_ingested_through)s
  AND
  (
      %(is_bootstrap)s = 1
      OR last_ingested_at > %(ct_ingested_after)s
      OR registered_domain IN
      (
          SELECT DISTINCT root_domain
          FROM corpscout.commoncrawl_domains
          WHERE root_domain != ''
            AND cityHash64(root_domain) %% %(shard_count)s = %(shard_index)s
            AND resolved_at > %(domains_resolved_after)s
            AND resolved_at <= %(domains_resolved_through)s
      )
  )
  AND domain_normalized != ''
  AND fqdn_normalized != domain_normalized
  AND endsWith(fqdn_normalized, concat('.', domain_normalized))
  AND position(fqdn_normalized, '*') = 0
"""

AXFR_HOSTNAME_UPSERT_SQL = """
INSERT INTO corpscout.commoncrawl_domain_hostnames
(
    root_domain,
    label,
    discovery_source,
    first_seen,
    last_seen,
    last_resolved,
    last_not_after
)
SELECT
    domain_normalized AS root_domain,
    substring(
        name_normalized,
        1,
        length(name_normalized) - length(domain_normalized) - 1
    ) AS label,
    'axfr' AS discovery_source,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(observed_at) AS last_resolved,
    toDateTime64(0, 3, 'UTC') AS last_not_after
FROM
(
    SELECT
        lower(trimRight(root_domain, '.')) AS domain_normalized,
        lower(trimRight(name, '.')) AS name_normalized,
        observed_at
    FROM corpscout.commoncrawl_domain_dns_record_observations
    WHERE source = 'axfr'
      AND cityHash64(root_domain) %% %(shard_count)s = %(shard_index)s
      AND loaded_at > %(axfr_loaded_after)s
      AND loaded_at <= %(axfr_loaded_through)s
      AND root_domain IN
      (
          SELECT DISTINCT root_domain
          FROM corpscout.commoncrawl_domains
          WHERE root_domain != ''
      )
)
WHERE domain_normalized != ''
  AND name_normalized != domain_normalized
  AND endsWith(name_normalized, concat('.', domain_normalized))
  AND position(name_normalized, '*') = 0
GROUP BY
    domain_normalized,
    name_normalized
"""

HOSTNAME_SYNC_STATE_SQL = """
SELECT
    count() AS state_rows,
    max(ct_ingested_through) AS ct_ingested_through,
    max(domains_resolved_through) AS domains_resolved_through,
    max(axfr_loaded_through) AS axfr_loaded_through
FROM corpscout.commoncrawl_domain_hostname_sync_state
WHERE shard_index = %(shard_index)s
"""

HOSTNAME_SYNC_WATERMARKS_SQL = """
SELECT
    (
        SELECT max(last_ingested_at)
        FROM ctlogs.hostnames
        WHERE cityHash64(registered_domain) %% %(shard_count)s = %(shard_index)s
    ) AS ct_ingested_through,
    (
        SELECT max(resolved_at)
        FROM corpscout.commoncrawl_domains
        WHERE root_domain != ''
          AND cityHash64(root_domain) %% %(shard_count)s = %(shard_index)s
    ) AS domains_resolved_through,
    (
        SELECT max(loaded_at)
        FROM corpscout.commoncrawl_domain_dns_record_observations
        WHERE source = 'axfr'
          AND cityHash64(root_domain) %% %(shard_count)s = %(shard_index)s
    ) AS axfr_loaded_through
"""

HOSTNAME_SYNC_CHECKPOINT_SQL = """
INSERT INTO corpscout.commoncrawl_domain_hostname_sync_state
(
    shard_index,
    ct_ingested_through,
    domains_resolved_through,
    axfr_loaded_through,
    completed_at,
    completed_run
)
SELECT
    %(shard_index)s,
    %(ct_ingested_through)s,
    %(domains_resolved_through)s,
    %(axfr_loaded_through)s,
    %(completed_at)s,
    argMaxState(toString(%(run_id)s), toDateTime64(%(completed_at)s, 3, 'UTC'))
"""


@dg.asset(
    name="commoncrawl_domain_hostnames",
    deps=[
        CTLOGS_HOSTNAMES_ASSET.key,
        COMMONCRAWL_DOMAINS_ASSET.key,
        DNS_RECORD_OBSERVATIONS_ASSET.key,
    ],
    description=(
        "Adds non-wildcard CT and AXFR-observed subdomains for root domains present in "
        "the Common Crawl DNS domain set. Existing worker discoveries are preserved."
    ),
    group_name=COMMONCRAWL_HOSTNAME_GROUP,
    kinds={"clickhouse", "ct", "dns", "axfr"},
    partitions_def=COMMONCRAWL_DOMAIN_HOSTNAME_SHARDS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=COMMONCRAWL_HOSTNAME_POOL,
)
def commoncrawl_domain_hostnames(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Merge one hostname-source shard into the Common Crawl hostname inventory."""
    shard_index = hostname_shard_index(context.partition_key)
    shard_parameters = {
        "shard_count": HOSTNAME_SHARD_COUNT,
        "shard_index": shard_index,
    }

    context.log.info(
        "Starting Common Crawl hostname sync: partition=%s shard_index=%s",
        context.partition_key,
        shard_index,
    )
    with clickhouse.get_connection() as client:
        (
            state_rows,
            ct_ingested_after,
            domains_resolved_after,
            axfr_loaded_after,
        ) = client.execute(
            HOSTNAME_SYNC_STATE_SQL,
            {"shard_index": shard_index},
        )[0]
        bootstrap = state_rows == 0
        ct_ingested_after = ct_ingested_after or EPOCH
        domains_resolved_after = domains_resolved_after or EPOCH
        axfr_loaded_after = axfr_loaded_after or EPOCH

        (
            ct_ingested_through,
            domains_resolved_through,
            axfr_loaded_through,
        ) = client.execute(
            HOSTNAME_SYNC_WATERMARKS_SQL,
            shard_parameters,
        )[0]
        ct_ingested_through = ct_ingested_through or ct_ingested_after
        domains_resolved_through = domains_resolved_through or domains_resolved_after
        axfr_loaded_through = axfr_loaded_through or axfr_loaded_after

        upsert_parameters = {
            **shard_parameters,
            "is_bootstrap": int(bootstrap),
            "ct_ingested_after": ct_ingested_after,
            "ct_ingested_through": ct_ingested_through,
            "domains_resolved_after": domains_resolved_after,
            "domains_resolved_through": domains_resolved_through,
            "axfr_loaded_after": axfr_loaded_after,
            "axfr_loaded_through": axfr_loaded_through,
        }
        client.execute(CT_HOSTNAME_UPSERT_SQL, upsert_parameters)
        ct_written_rows = client.last_query.progress.written_rows
        ct_written_bytes = client.last_query.progress.written_bytes
        ct_elapsed_seconds = client.last_query.elapsed

        client.execute(AXFR_HOSTNAME_UPSERT_SQL, upsert_parameters)
        axfr_written_rows = client.last_query.progress.written_rows
        axfr_written_bytes = client.last_query.progress.written_bytes
        axfr_elapsed_seconds = client.last_query.elapsed

        completed_at = datetime.now(UTC)
        client.execute(
            HOSTNAME_SYNC_CHECKPOINT_SQL,
            {
                "shard_index": shard_index,
                "ct_ingested_through": ct_ingested_through,
                "domains_resolved_through": domains_resolved_through,
                "axfr_loaded_through": axfr_loaded_through,
                "completed_at": completed_at,
                "run_id": context.run_id,
            },
        )
        metadata = {
            "source_table": "ctlogs.hostnames",
            "axfr_source_table": (
                "corpscout.commoncrawl_domain_dns_record_observations"
            ),
            "membership_table": "corpscout.commoncrawl_domains",
            "destination_table": "corpscout.commoncrawl_domain_hostnames",
            "shard_index": shard_index,
            "shard_count": HOSTNAME_SHARD_COUNT,
            "bootstrap": bootstrap,
            "ct_ingested_after": ct_ingested_after.isoformat(),
            "ct_ingested_through": ct_ingested_through.isoformat(),
            "domains_resolved_after": domains_resolved_after.isoformat(),
            "domains_resolved_through": domains_resolved_through.isoformat(),
            "axfr_loaded_after": axfr_loaded_after.isoformat(),
            "axfr_loaded_through": axfr_loaded_through.isoformat(),
            "ct_written_rows": ct_written_rows,
            "axfr_written_rows": axfr_written_rows,
            "written_rows": ct_written_rows + axfr_written_rows,
            "written_bytes": ct_written_bytes + axfr_written_bytes,
            "elapsed_seconds": ct_elapsed_seconds + axfr_elapsed_seconds,
        }

    context.log.info(
        "Completed Common Crawl hostname sync: partition=%s metadata=%s",
        context.partition_key,
        metadata,
    )
    return dg.MaterializeResult(metadata=metadata)


def hostname_shard_index(partition_key: str) -> int:
    """Return the validated hostname shard index from a Dagster partition key."""
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "shard" or separator == "" or not suffix.isdigit():
        raise ValueError(f"Invalid hostname shard partition: {partition_key!r}")

    shard_index = int(suffix)
    if not 0 <= shard_index < HOSTNAME_SHARD_COUNT:
        raise ValueError(f"Hostname shard index out of range: {shard_index}")
    return shard_index
