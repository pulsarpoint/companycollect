CREATE DATABASE IF NOT EXISTS corpscout;

-- Restore the authoritative source-backed view without disabling incremental capture. Keeping the
-- state table and materialized view live makes rollback immediate and preserves a later recutover.
CREATE OR REPLACE VIEW corpscout.domain_hostnames AS
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
