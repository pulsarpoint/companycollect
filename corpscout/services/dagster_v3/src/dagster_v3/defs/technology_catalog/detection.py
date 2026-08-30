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


def candidate_sql(signal_type: str) -> str:
    """Deduplicated (root_domain, record_name, candidate) rows for one signal."""
    record_type, expression = _CANDIDATE_EXPRESSIONS[signal_type]
    if signal_type == "dns_cname":
        name_filter = "(name = root_domain OR name = concat('www.', root_domain))"
    else:
        name_filter = "name = root_domain"
    return f"""SELECT root_domain, name AS record_name, {expression} AS candidate
FROM `{RESOLVED_DATABASE}`.`{DNS_RECORDS_TABLE}`
WHERE record_type = '{record_type}' AND {name_filter} AND candidate != ''
GROUP BY root_domain, record_name, candidate"""


def detection_insert_sql(stage: str, signal_type: str) -> str:
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
    arrayElement(%(confidences)s, match_index) AS confidence,
    arrayElement(%(sources)s, match_index) AS source,
    %(source_run_id)s AS source_run_id,
    %(detected_at)s AS detected_at
FROM (
    {candidate_sql(signal_type)}
)
ARRAY JOIN multiMatchAllIndices(candidate, %(match_patterns)s) AS match_index"""


def self_hosted_insert_sql(stage: str) -> str:
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
    100 AS confidence,
    '{RULE_SOURCE}' AS source,
    %(source_run_id)s AS source_run_id,
    %(detected_at)s AS detected_at
FROM (
    {candidate_sql("dns_mx")}
)
WHERE candidate NOT IN ('~', 'localhost')
  AND (candidate = root_domain OR endsWith(candidate, concat('.', root_domain)))"""
