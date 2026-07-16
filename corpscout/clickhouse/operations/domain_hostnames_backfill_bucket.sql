-- Backfill one source partition into the incremental hostname state. Run with bucket values 0
-- through 15 after migration 130 and before the public-view cutover. The incremental materialized
-- view is already capturing concurrent inserts, and every aggregate below is idempotent min or max,
-- so overlap and a complete rerun of a bucket preserve the logical result.
INSERT INTO corpscout.domain_hostnames_state
SELECT
    root_domain,
    name AS hostname,
    max(toUInt8(record_type = 'A')) AS has_ipv4,
    max(toUInt8(record_type = 'AAAA')) AS has_ipv6,
    max(toUInt8(record_type = 'CNAME')) AS has_cname,
    max(
        toUInt8(
            multiIf(
                discovery = 'axfr', 3,
                discovery = 'ct', 2,
                discovery = 'static', 1,
                0
            )
        )
    ) AS discovery_rank,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(loaded_at) AS last_loaded_at
FROM corpscout.commoncrawl_domain_dns_record_observations
WHERE cityHash64(root_domain) % 16 = {bucket:UInt8}
  AND record_type IN ('A', 'AAAA', 'CNAME')
  AND root_domain != ''
  AND name != ''
  AND position(name, '*') = 0
  AND
  (
      name = root_domain
      OR endsWith(name, concat('.', root_domain))
  )
GROUP BY
    root_domain,
    hostname;
