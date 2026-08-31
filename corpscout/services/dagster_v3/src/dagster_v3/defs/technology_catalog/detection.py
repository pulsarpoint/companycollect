"""DNS technology detection: fingerprints + rules over the DNS record store.

Each dns_* signal type extracts candidate strings from
corpscout.commoncrawl_domain_dns_records (apex-scoped; CNAME also covers www)
and matches ALL of that signal's fingerprint patterns in one Vectorscan pass
(multiMatchAllIndices) — never one regex scan per pattern. Case-insensitivity
comes from an inline (?i) prefix on the bound match patterns (this server's
build has no CaseInsensitive multiMatch variant); the stored matched_pattern
stays the clean original. Matched indices map back to (technology, pattern,
confidence, source) arrays bound as query parameters.

Vectorscan rejects a few regex constructs re2 also lacks (lookarounds,
backreferences); fingerprints using them are skipped with a log line rather
than failing the whole pass.

The self-hosted-email rule is pattern-free: an apex MX host under the
domain itself (excluding null-MX placeholders) marks 'Self-hosted email'.
"""

import re
from dataclasses import dataclass

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE
from dagster_v3.defs.technology_catalog import tables

DNS_RECORDS_TABLE = "commoncrawl_domain_dns_records"

# The DNS record store is PARTITION BY cityHash64(root_domain) % 16 (migration
# 000161); every per-bucket query repeats that expression so ClickHouse prunes
# to a single ~18 GiB partition instead of the whole 280 GiB table.
DNS_RECORDS_HASH_BUCKETS = 16

# The detection asset (and the domain_signal_technologies table, migration
# 000359) is partitioned cityHash64(root_domain) % 128 — a refinement of the
# record store's % 16 key: bucket N's rows live in record-store partition
# N % 16. 128 mirrors the webtech scan's static partitioning.
DETECTION_PARTITION_COUNT = 128


def detection_partition_keys() -> list[str]:
    return [f"hash_{bucket:03d}" for bucket in range(DETECTION_PARTITION_COUNT)]


def partition_bucket(partition_key: str) -> int:
    bucket = int(partition_key.removeprefix("hash_"))
    if not 0 <= bucket < DETECTION_PARTITION_COUNT:
        raise ValueError(f"partition key {partition_key!r} is out of range")
    return bucket

SELF_HOSTED_TECHNOLOGY = "Self-hosted email"
SELF_HOSTED_SIGNAL = "self_hosted_email"
RULE_SOURCE = "rule"

# Vectorscan (like re2) has no lookarounds or backreferences.
_UNSUPPORTED_REGEX = re.compile(r"\(\?<|\(\?=|\(\?!|\\\d")

# Candidate string per signal type: how a matchable value is derived from the
# presentation RDATA. MX values embed "priority host"; hostnames carry an
# optional trailing dot and arbitrary case; TXT values keep surrounding quotes.
_HOSTNAME = "lower(trim(TRAILING '.' FROM value))"
_MX_HOST = "lower(trim(TRAILING '.' FROM substringIndex(value, ' ', -1)))"

_CANDIDATE_EXPRESSIONS = {
    "dns_mx": ("MX", _MX_HOST),
    "dns_txt": ("TXT", "trim(BOTH '\"' FROM value)"),
    "dns_ns": ("NS", _HOSTNAME),
    "dns_soa": ("SOA", "value"),
    "dns_cname": ("CNAME", _HOSTNAME),
}


@dataclass(frozen=True)
class SignalFingerprints:
    signal_type: str
    technologies: list[str]
    patterns: list[str]
    confidences: list[int]
    sources: list[str]

    @property
    def match_patterns(self) -> list[str]:
        """The patterns as bound to multiMatchAllIndices: (?i)-prefixed."""
        return [f"(?i){pattern}" for pattern in self.patterns]


def vectorscan_safe(pattern: str) -> bool:
    return not _UNSUPPORTED_REGEX.search(pattern)


def group_fingerprints(
    rows: list[tuple[str, str, str, int, str]],
) -> tuple[list[SignalFingerprints], list[tuple[str, str]]]:
    """(technology, signal_type, pattern, confidence, source) rows → per-signal
    parallel arrays, plus the (technology, pattern) pairs skipped as unsafe."""
    by_signal: dict[str, SignalFingerprints] = {}
    skipped: list[tuple[str, str]] = []
    for technology, signal_type, pattern, confidence, source in rows:
        if signal_type not in _CANDIDATE_EXPRESSIONS:
            continue
        if not vectorscan_safe(pattern):
            skipped.append((technology, pattern))
            continue
        group = by_signal.setdefault(
            signal_type,
            SignalFingerprints(signal_type, [], [], [], []),
        )
        group.technologies.append(technology)
        group.patterns.append(pattern)
        group.confidences.append(int(confidence))
        group.sources.append(source)
    return [by_signal[signal] for signal in sorted(by_signal)], skipped


def candidates_table_ddl(candidates: str) -> str:
    """Temp table holding one row per distinct apex candidate string.

    The DNS record store sorts by root_domain first, so a per-record-type
    filter is a full 3.4B-row scan; extracting EVERY signal's candidates in a
    single scan and matching against this compact table (sorted by
    signal_type) keeps the asset to one big read instead of five.
    """
    return f"""CREATE TABLE {candidates}
(
    root_domain String,
    record_name String,
    signal_type LowCardinality(String),
    candidate String,
    first_seen DateTime64(3, 'UTC'),
    last_seen DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (signal_type, root_domain)"""


def candidates_insert_sql(candidates: str, bucket: int) -> str:
    """One detection bucket's pass over the DNS record store into the temp table.

    The % 16 clause repeats the record store's partition-key expression
    verbatim so ClickHouse prunes to that single partition; the % 128 clause
    then keeps only this detection bucket's domains.
    """
    signal_case = " ".join(
        f"WHEN '{record_type}' THEN '{signal}'"
        for signal, (record_type, _) in sorted(_CANDIDATE_EXPRESSIONS.items())
    )
    candidate_case = " ".join(
        f"WHEN '{record_type}' THEN {expression}"
        for _, (record_type, expression) in sorted(_CANDIDATE_EXPRESSIONS.items())
    )
    record_types = ", ".join(
        f"'{record_type}'"
        for record_type, _ in sorted(_CANDIDATE_EXPRESSIONS.values())
    )
    return f"""INSERT INTO {candidates}
    (root_domain, record_name, signal_type, candidate, first_seen, last_seen)
SELECT
    root_domain,
    name AS record_name,
    CASE record_type {signal_case} END AS signal_type,
    CASE record_type {candidate_case} END AS candidate,
    min(first_seen) AS first_seen,
    max(last_seen) AS last_seen
FROM `{RESOLVED_DATABASE}`.`{DNS_RECORDS_TABLE}`
WHERE cityHash64(root_domain) % {DNS_RECORDS_HASH_BUCKETS} = {int(bucket) % DNS_RECORDS_HASH_BUCKETS}
  AND cityHash64(root_domain) % {DETECTION_PARTITION_COUNT} = {int(bucket)}
  AND record_type IN ({record_types})
  AND (
    name = root_domain
    OR (record_type = 'CNAME' AND name = concat('www.', root_domain))
  )
  AND candidate != ''
GROUP BY root_domain, record_name, signal_type, candidate"""


def detection_insert_sql(stage: str, candidates: str, signal_type: str) -> str:
    """One Vectorscan pass over the signal's candidates; parameters carry the
    parallel fingerprint arrays and the run/timestamp scalars."""
    column_list = ", ".join(tables.DOMAIN_SIGNAL_TECHNOLOGIES_COLUMNS)
    return f"""INSERT INTO {stage} ({column_list})
SELECT
    root_domain,
    arrayElement(%(technologies)s, match_index) AS technology,
    '{signal_type}' AS signal_type,
    arrayElement(%(patterns)s, match_index) AS matched_pattern,
    candidate AS evidence,
    record_name,
    first_seen,
    last_seen,
    arrayElement(%(confidences)s, match_index) AS confidence,
    arrayElement(%(sources)s, match_index) AS source,
    %(source_run_id)s AS source_run_id,
    %(detected_at)s AS detected_at
FROM (
    -- Filtered in a subquery: the outer SELECT aliases a column named
    -- signal_type, and ClickHouse resolves an outer WHERE against that
    -- alias instead of the table column (this silently broke both the
    -- per-signal scoping and the self-hosted rule on 2026-08-31).
    SELECT root_domain, record_name, candidate, first_seen, last_seen
    FROM {candidates}
    WHERE signal_type = '{signal_type}'
)
ARRAY JOIN multiMatchAllIndices(candidate, %(match_patterns)s) AS match_index"""


def self_hosted_insert_sql(stage: str, candidates: str) -> str:
    """Apex MX under the domain itself → the pattern-free self-hosted rule.

    A subdomain host must end with '.<root_domain>'; the bare-equality branch
    catches MX pointing at the apex. Null-MX placeholders ('' after trimming,
    '~', 'localhost') never mark self-hosting.
    """
    column_list = ", ".join(tables.DOMAIN_SIGNAL_TECHNOLOGIES_COLUMNS)
    return f"""INSERT INTO {stage} ({column_list})
SELECT
    root_domain,
    '{SELF_HOSTED_TECHNOLOGY}' AS technology,
    '{SELF_HOSTED_SIGNAL}' AS signal_type,
    '' AS matched_pattern,
    candidate AS evidence,
    record_name,
    first_seen,
    last_seen,
    100 AS confidence,
    '{RULE_SOURCE}' AS source,
    %(source_run_id)s AS source_run_id,
    %(detected_at)s AS detected_at
FROM (
    -- Subquery for the same alias-shadowing reason as detection_insert_sql.
    SELECT root_domain, record_name, candidate, first_seen, last_seen
    FROM {candidates}
    WHERE signal_type = 'dns_mx'
)
WHERE candidate NOT IN ('~', 'localhost')
  AND (candidate = root_domain OR endsWith(candidate, concat('.', root_domain)))"""


def replace_partition_sql(qualified: str, stage: str, bucket: int) -> str:
    """Atomically swap ONE detection bucket's slice from the stage table.

    Both tables share the % 128 partition key, and the stage was filled from
    candidates of exactly this bucket in the same run, so the swap can never
    touch another bucket's rows.
    """
    return f"ALTER TABLE {qualified} REPLACE PARTITION {int(bucket)} FROM {stage}"
