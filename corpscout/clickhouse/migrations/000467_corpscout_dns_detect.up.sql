CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per resolution of a DNS record by the dns-detect service, including
-- records that resolved to nothing, so they are not sent again. A record is
-- re-resolved when its rules_version, or for A/AAAA/SPF its ip_version, is no
-- longer current, and the newest resolved_at wins. record_from/record_to are the
-- record's own seen window.
CREATE TABLE IF NOT EXISTS corpscout.dns_record_resolutions
(
    record_id FixedString(16),
    root_domain String,
    record_name String,
    record_type LowCardinality(String),
    analyzer LowCardinality(String),
    rules_version LowCardinality(String),
    ip_version LowCardinality(String),
    result_count UInt16,
    findings Array(Tuple(code LowCardinality(String), detail String)),
    record_from Date,
    record_to Date,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (root_domain, record_id);

-- One row per service a record proves (dns-detect's Result). Rows are never
-- deleted. A re-resolved record gets new rows, and the views below keep only
-- the rows of each record's latest resolution.
CREATE TABLE IF NOT EXISTS corpscout.dns_record_services
(
    record_id FixedString(16),
    root_domain String,
    record_name String,
    record_type LowCardinality(String),
    analyzer LowCardinality(String),
    subject String,
    service_type LowCardinality(String),
    provider_key String,
    provider_slug LowCardinality(String),
    service_key LowCardinality(String),
    rule_id String,
    confidence Float32,
    fallback UInt8,
    valid_from Date,
    valid_to Date,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY cityHash64(root_domain) % 128
ORDER BY (root_domain, service_type, provider_key, record_id, valid_from);

-- Results of each record's latest resolution.
CREATE VIEW IF NOT EXISTS corpscout.dns_record_services_current AS
SELECT s.*
FROM corpscout.dns_record_services AS s
INNER JOIN
(
    SELECT root_domain, record_id, max(resolved_at) AS resolved_at
    FROM corpscout.dns_record_resolutions
    GROUP BY root_domain, record_id
) AS latest USING (root_domain, record_id, resolved_at);

-- Per domain, service type and provider: the periods it was in use.
-- Fallback rows (SOA MNAME) are dropped wherever a non-fallback row of the
-- same domain and service type overlaps them. Windows of one service that
-- overlap or are less than 45 days apart merge (scans run every 2-4 weeks),
-- so first_seen/last_seen are as precise as the scans.
CREATE VIEW IF NOT EXISTS corpscout.domain_services_history AS
SELECT
    root_domain,
    service_type,
    provider_key,
    anyIf(provider_slug, provider_slug != '') AS provider_slug,
    arraySort(groupUniqArrayIf(service_key, service_key != '')) AS service_keys,
    min(valid_from) AS first_seen,
    max(valid_to) AS last_seen,
    count() AS evidence,
    arraySort(groupUniqArray(analyzer)) AS analyzers,
    arraySort(groupUniqArray(record_type)) AS record_types,
    max(confidence) AS confidence
FROM
(
    SELECT
        *,
        sum(new_island) OVER (PARTITION BY root_domain, service_type, provider_key ORDER BY valid_from, valid_to ROWS UNBOUNDED PRECEDING) AS island
    FROM
    (
        SELECT
            *,
            valid_from > addDays(max(valid_to) OVER (PARTITION BY root_domain, service_type, provider_key ORDER BY valid_from, valid_to ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 45) AS new_island
        FROM
        (
            SELECT c.*
            FROM corpscout.dns_record_services_current AS c
            LEFT JOIN
            (
                SELECT root_domain, service_type, groupArray((valid_from, valid_to)) AS covered
                FROM corpscout.dns_record_services_current
                WHERE fallback = 0
                GROUP BY root_domain, service_type
            ) AS primary USING (root_domain, service_type)
            WHERE c.fallback = 0
               OR NOT arrayExists(w -> tupleElement(w, 1) <= c.valid_to AND tupleElement(w, 2) >= c.valid_from, primary.covered)
        )
    )
)
GROUP BY root_domain, service_type, provider_key, island;

-- The history intervals still in use: those reaching the domain's latest scan
-- of one of their record types (scans of different record types run on
-- different schedules, and a latest scan that resolved to nothing still counts).
CREATE VIEW IF NOT EXISTS corpscout.domain_services_now AS
SELECT h.*
FROM corpscout.domain_services_history AS h
INNER JOIN
(
    SELECT root_domain, CAST((groupArray(record_type), groupArray(last_scan)), 'Map(String, Date)') AS latest_scan
    FROM
    (
        SELECT root_domain, record_type, max(record_to) AS last_scan
        FROM corpscout.dns_record_resolutions
        GROUP BY root_domain, record_type
    )
    GROUP BY root_domain
) AS scans USING (root_domain)
WHERE arrayExists(t -> h.last_seen >= scans.latest_scan[t], h.record_types);
