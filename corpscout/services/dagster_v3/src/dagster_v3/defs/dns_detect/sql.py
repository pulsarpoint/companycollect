"""SQL for the dns-detect asset: candidate selection and result inserts.

Candidates are one hash bucket's DNS records that the resolver routes and that
have no current resolution: never resolved, resolved under older rules, (for
the ip and spf analyzers) under an older IP-range version, or with a window
that has grown since (a later scan extends a record's last_seen).
"""

DNS_RECORDS_TABLE = "commoncrawl_domain_dns_records"
RESOLUTIONS_TABLE = "dns_record_resolutions"
SERVICES_TABLE = "dns_record_services"

# The DNS store is PARTITION BY cityHash64(root_domain) % 16; bucket N of 128
# lives in store partition N % 16, so repeating that expression prunes.
STORE_BUCKETS = 16
PARTITION_COUNT = 128

# Analyzers whose answers depend on the IP ranges (A/AAAA, SPF ip4/ip6).
IP_ANALYZERS = ("ip", "spf")
ROUTABLE_TYPES = ("NS", "SOA", "MX", "CNAME", "TXT", "A", "AAAA")

RESOLUTION_COLUMNS = (
    "record_id", "root_domain", "record_name", "record_type", "analyzer", "rules_version", "ip_version",
    "result_count", "findings", "record_from", "record_to", "resolved_at",
)
SERVICE_COLUMNS = (
    "record_id", "root_domain", "record_name", "record_type", "analyzer", "subject", "service_type", "provider_key",
    "provider_slug", "service_key", "rule_id", "confidence", "fallback", "valid_from", "valid_to", "resolved_at",
)


def partition_keys() -> list[str]:
    return [f"hash_{bucket:03d}" for bucket in range(PARTITION_COUNT)]


def partition_bucket(partition_key: str) -> int:
    bucket = int(partition_key.removeprefix("hash_"))
    if not 0 <= bucket < PARTITION_COUNT:
        raise ValueError(f"partition key {partition_key!r} is out of range")
    return bucket


def watermark_sql(database: str, bucket: int) -> str:
    """The newest load time in the bucket: the next incremental run's watermark."""
    return f"""SELECT max(last_loaded_at)
FROM `{database}`.`{DNS_RECORDS_TABLE}`
WHERE cityHash64(root_domain) %% {STORE_BUCKETS} = {int(bucket) % STORE_BUCKETS}
  AND cityHash64(root_domain) %% {PARTITION_COUNT} = {int(bucket)}"""


def candidates_sql(database: str, bucket: int, *, incremental: bool = False) -> str:
    """The bucket's routable records without a current resolution, as the
    resolver's input fields. Parameters: %(rules_version)s, %(ip_version)s.

    The query goes through clickhouse-driver's %-substitution, so a literal
    modulo is written %%. Records with a blank (or whitespace-only: the resolver
    trims) root_domain, name or value are
    left out: the service would refuse the whole batch for one of them.

    incremental=True keeps only domains with records loaded after
    %(since)s. That set is found from last_loaded_at alone (one column, no
    FINAL), and the IN on the primary key then prunes the expensive FINAL
    read to those domains."""
    changed = ""
    if incremental:
        changed = f"""
      AND root_domain IN (
        SELECT DISTINCT root_domain FROM `{database}`.`{DNS_RECORDS_TABLE}`
        WHERE cityHash64(root_domain) %% {STORE_BUCKETS} = {int(bucket) % STORE_BUCKETS}
          AND cityHash64(root_domain) %% {PARTITION_COUNT} = {int(bucket)}
          AND last_loaded_at > toDateTime64(%(since)s, 3, 'UTC')
      )"""
    types = ", ".join(f"'{t}'" for t in ROUTABLE_TYPES)
    ip_analyzers = ", ".join(f"'{a}'" for a in IP_ANALYZERS)
    return f"""SELECT
    lower(hex(r.record_id)) AS record_id,
    r.root_domain AS root_domain,
    r.name AS name,
    toString(r.record_type) AS type,
    r.value AS value,
    toString(toDate(r.first_seen)) AS first_seen,
    toString(toDate(r.last_seen)) AS last_seen
FROM
(
    SELECT record_id, root_domain, name, record_type, value, first_seen, last_seen
    FROM `{database}`.`{DNS_RECORDS_TABLE}` FINAL
    WHERE cityHash64(root_domain) %% {STORE_BUCKETS} = {int(bucket) % STORE_BUCKETS}
      AND cityHash64(root_domain) %% {PARTITION_COUNT} = {int(bucket)}
      AND record_type IN ({types})
      AND trimBoth(root_domain) != '' AND trimBoth(name) != '' AND trimBoth(value) != ''{changed}
      AND (
        name = root_domain
        OR name = concat('www.', root_domain)
        OR startsWith(name, '_')
        OR position(name, '._domainkey.') > 0
        OR (record_type = 'TXT' AND positionCaseInsensitive(value, 'v=spf1') > 0)
      )
) AS r
LEFT ANTI JOIN
(
    SELECT root_domain, record_id, tupleElement(last, 1) AS record_from, tupleElement(last, 2) AS record_to
    FROM
    (
        SELECT root_domain, record_id,
               argMax(tuple(record_from, record_to, rules_version, ip_version, analyzer), resolved_at) AS last
        FROM `{database}`.`{RESOLUTIONS_TABLE}`
        WHERE cityHash64(root_domain) %% {PARTITION_COUNT} = {int(bucket)}
        GROUP BY root_domain, record_id
    )
    WHERE tupleElement(last, 3) = %(rules_version)s
      AND (tupleElement(last, 4) = %(ip_version)s OR tupleElement(last, 5) NOT IN ({ip_analyzers}))
) AS done
ON done.root_domain = r.root_domain AND done.record_id = r.record_id
   AND done.record_from = toDate(r.first_seen) AND done.record_to = toDate(r.last_seen)"""


def insert_sql(database: str, table: str, columns: tuple[str, ...]) -> str:
    return f"INSERT INTO `{database}`.`{table}` ({', '.join(columns)}) VALUES"


INTERVALS_TABLE = "domain_service_intervals"
COUNTS_TABLE = "provider_service_counts"
INTERVAL_COLUMNS = (
    "root_domain", "service_type", "provider_key", "provider_slug", "service_keys", "first_seen", "last_seen",
    "is_current", "evidence", "analyzers", "record_types", "confidence", "bucket", "computed_at",
)


def _in_bucket(column: str, bucket: int) -> str:
    return f"cityHash64({column}) %% {PARTITION_COUNT} = {int(bucket)}"


def _latest_results(database: str, bucket: int) -> str:
    """Result rows of each record's latest resolution in the bucket."""
    return f"""SELECT s.*
    FROM `{database}`.`{SERVICES_TABLE}` AS s
    INNER JOIN
    (
        SELECT root_domain, record_id, max(resolved_at) AS resolved_at
        FROM `{database}`.`{RESOLUTIONS_TABLE}`
        WHERE {_in_bucket('root_domain', bucket)}
        GROUP BY root_domain, record_id
    ) AS latest USING (root_domain, record_id, resolved_at)
    WHERE {_in_bucket('s.root_domain', bucket)}"""


def current_results_count_sql(database: str, bucket: int) -> str:
    """How many current result rows the bucket has: the empty-stage guard."""
    return f"SELECT count() FROM ({_latest_results(database, bucket)})"


def intervals_insert_sql(database: str, target: str, bucket: int) -> str:
    """The bucket's service periods into target, with the rules of the
    domain_services_history / domain_services_now views (migration 000468)."""
    columns = ", ".join(INTERVAL_COLUMNS)
    return f"""INSERT INTO `{database}`.`{target}` ({columns})
WITH cur AS
(
    {_latest_results(database, bucket)}
),
history AS
(
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
                valid_from >= addDays(max(valid_to) OVER (PARTITION BY root_domain, service_type, provider_key ORDER BY valid_from, valid_to ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 45) AS new_island
            FROM
            (
                SELECT c.*
                FROM cur AS c
                LEFT JOIN
                (
                    SELECT root_domain, service_type, groupArray((valid_from, valid_to)) AS covered
                    FROM cur
                    WHERE fallback = 0
                    GROUP BY root_domain, service_type
                ) AS primary USING (root_domain, service_type)
                WHERE c.fallback = 0
                   OR NOT arrayExists(w -> tupleElement(w, 1) <= c.valid_to AND tupleElement(w, 2) >= c.valid_from, primary.covered)
            )
        )
    )
    GROUP BY root_domain, service_type, provider_key, island
),
scans AS
(
    SELECT root_domain, CAST((groupArray(record_type), groupArray(last_scan)), 'Map(String, Date)') AS latest_scan
    FROM
    (
        SELECT root_domain, record_type, max(record_to) AS last_scan
        FROM `{database}`.`{RESOLUTIONS_TABLE}`
        WHERE {_in_bucket('root_domain', bucket)}
        GROUP BY root_domain, record_type
    )
    GROUP BY root_domain
)
SELECT
    h.root_domain, h.service_type, h.provider_key, h.provider_slug, h.service_keys, h.first_seen, h.last_seen,
    arrayExists(t -> h.last_seen >= s.latest_scan[t], h.record_types) AS is_current,
    h.evidence, h.analyzers, h.record_types, h.confidence, {int(bucket)} AS bucket, now64(3, 'UTC') AS computed_at
FROM history AS h
INNER JOIN scans AS s USING (root_domain)"""


def counts_insert_sql(database: str, target: str, intervals: str, bucket: int) -> str:
    """Distinct domains per provider key and service type, plus a '' service
    type row per provider key for the total, from the staged intervals."""
    return f"""INSERT INTO `{database}`.`{target}` (bucket, provider_slug, provider_key, service_type, domains_now, domains_ever, computed_at)
SELECT {int(bucket)}, provider_slug, provider_key, if(grouping(service_type) = 1, '', service_type) AS any_or_type,
       uniqExactIf(root_domain, is_current = 1), uniqExact(root_domain), now64(3, 'UTC')
FROM `{database}`.`{intervals}`
WHERE bucket = {int(bucket)} AND {_in_bucket('root_domain', bucket)}
GROUP BY GROUPING SETS ((provider_slug, provider_key, service_type), (provider_slug, provider_key))"""
