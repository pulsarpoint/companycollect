CREATE DATABASE IF NOT EXISTS corpscout;

-- Confirmed hostname inventory derived only from retry-safe DNS observations. A row means that
-- the in-domain record owner was observed with an A, AAAA, or CNAME record at least once. The
-- CNAME target remains in value and is deliberately not promoted to a hostname for this root.
-- Idempotent min and max aggregates make unmerged retry copies harmless without requiring FINAL.
CREATE VIEW IF NOT EXISTS corpscout.domain_hostnames AS
SELECT
    root_domain,
    hostname,
    substring(
        hostname,
        1,
        greatest(
            toInt64(length(hostname)) - toInt64(length(root_domain)) - 1,
            0
        )
    ) AS label,
    max(record_type = 'A') AS has_ipv4,
    max(record_type = 'AAAA') AS has_ipv6,
    max(record_type = 'CNAME') AS has_cname,
    multiIf(
        max(discovery = 'axfr') = 1, 'axfr',
        max(discovery = 'ct') = 1, 'ct',
        max(discovery = 'static') = 1, 'static',
        'unknown'
    ) AS discovery_source,
    min(observed_at) AS first_seen,
    max(observed_at) AS last_seen,
    max(loaded_at) AS last_loaded_at
FROM
(
    -- Both DNS writers persist lowercase owner names without trailing dots. Keeping the stored
    -- root_domain unchanged lets bounded root predicates prune the source table's ORDER BY prefix.
    SELECT
        root_domain,
        name AS hostname,
        record_type,
        discovery,
        observed_at,
        loaded_at
    FROM corpscout.commoncrawl_domain_dns_record_observations
    WHERE record_type IN ('A', 'AAAA', 'CNAME')
)
WHERE root_domain != ''
  AND hostname != ''
  AND position(hostname, '*') = 0
  AND
  (
      hostname = root_domain
      OR endsWith(hostname, concat('.', root_domain))
  )
GROUP BY
    root_domain,
    hostname;
