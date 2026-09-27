"""INSERT … SELECT statements that normalise provider-recon documents.

The source is the S3-engine table (one JSON document per provider). Every
statement upserts into a ReplacingMergeTree(loaded_at) table and deletes
nothing, so instances that later leave latest.json keep their last state.
"""

from dagster_v3.defs.provider_recon.tables import (
    DOCUMENTS_S3_TABLE,
    IP_RANGES_TABLE,
    RULES_TABLE,
    SERVICES_TABLE,
)

_LIFECYCLE = """
    if(JSONExtractString(item, 'status') = '', 'active', JSONExtractString(item, 'status')) AS status,
    toDateOrZero(JSONExtractString(item, 'first_seen')) AS first_seen,
    toDateOrZero(JSONExtractString(item, 'last_seen')) AS last_seen,
    toDateOrNull(JSONExtractString(item, 'missing_since')) AS missing_since,
    toDateOrNull(JSONExtractString(item, 'removed_at')) AS removed_at,
    JSONExtractString(item, 'removal_action') AS removal_action,
    toDateOrNull(JSONExtractString(item, 'restored_at')) AS restored_at"""


def documents_count_sql(database: str, source_table: str = DOCUMENTS_S3_TABLE) -> str:
    return f"SELECT count() FROM `{database}`.`{source_table}`"


def _services_sql(database: str, source: str) -> str:
    return f"""INSERT INTO `{database}`.`{SERVICES_TABLE}` (provider_slug, provider_name, provider_category, provider_country, provider_website, provider_keys, service_key, service_name, service_types, traits, removed_at, collected_at, content_hash, loaded_at)
SELECT
    JSONExtractString(json, 'slug'),
    JSONExtractString(json, 'display_name'),
    JSONExtractString(json, 'category'),
    JSONExtractString(json, 'country'),
    JSONExtractString(json, 'website'),
    JSONExtract(json, 'provider_keys', 'Array(String)'),
    JSONExtractString(svc, 'service_key'),
    JSONExtractString(svc, 'display_name'),
    JSONExtract(svc, 'service_types', 'Array(String)'),
    JSONExtract(svc, 'traits', 'Array(String)'),
    toDateOrNull(JSONExtractString(svc, 'removed_at')),
    parseDateTime64BestEffort(JSONExtractString(json, 'collection', 'collected_at'), 3, 'UTC'),
    JSONExtractString(json, 'collection', 'content_hash'),
    %(loaded_at)s
FROM `{database}`.`{source}`
ARRAY JOIN JSONExtractArrayRaw(json, 'services') AS svc"""


def _ip_ranges_sql(database: str, source: str) -> str:
    return f"""INSERT INTO `{database}`.`{IP_RANGES_TABLE}` (provider_slug, service_key, cidr, ip_family, range_start, range_end, collector, source, feed_tag, region, confidence, status, first_seen, last_seen, missing_since, removed_at, removal_action, restored_at, source_url, source_version, loaded_at)
SELECT
    provider_slug, service_key, cidr,
    if(is_v4, 4, 6) AS ip_family,
    if(is_v4, toIPv6(IPv4NumToString(tupleElement(IPv4CIDRToRange(toIPv4(addr), prefix_len), 1))),
              tupleElement(IPv6CIDRToRange(toIPv6(addr), prefix_len), 1)) AS range_start,
    if(is_v4, toIPv6(IPv4NumToString(tupleElement(IPv4CIDRToRange(toIPv4(addr), prefix_len), 2))),
              tupleElement(IPv6CIDRToRange(toIPv6(addr), prefix_len), 2)) AS range_end,
    collector, source, feed_tag, region, confidence,
    status, first_seen, last_seen, missing_since, removed_at, removal_action, restored_at,
    source_url, source_version,
    %(loaded_at)s
FROM (
    SELECT
        JSONExtractString(json, 'slug') AS provider_slug,
        JSONExtractString(svc, 'service_key') AS service_key,
        JSONExtractString(item, 'cidr') AS cidr,
        splitByChar('/', cidr)[1] AS addr,
        toUInt8(splitByChar('/', cidr)[2]) AS prefix_len,
        isIPv4String(addr) AS is_v4,
        JSONExtractString(item, 'collector') AS collector,
        JSONExtractString(item, 'source') AS source,
        JSONExtractString(item, 'feed_tag') AS feed_tag,
        JSONExtractString(item, 'region') AS region,
        toFloat32(JSONExtractFloat(item, 'confidence')) AS confidence,
        JSONExtractString(item, 'source_url') AS source_url,
        JSONExtractString(item, 'source_version') AS source_version,{_LIFECYCLE}
    FROM `{database}`.`{source}`
    ARRAY JOIN JSONExtractArrayRaw(json, 'services') AS svc
    ARRAY JOIN JSONExtractArrayRaw(svc, 'evidence', 'ip_ranges') AS item
)"""


# Per kind: the evidence array and the kind-specific column expressions. Any
# rule column not listed takes its type's empty value.
_RULE_KINDS = {
    "asn": ("asns", {
        "rule_key": "concat('AS', toString(JSONExtractUInt(item, 'asn')))",
        "asn": "toUInt32(JSONExtractUInt(item, 'asn'))",
    }),
    "dns": ("dns_rules", {
        "rule_key": "concat(JSONExtractString(item, 'record_type'), ' ', JSONExtractString(item, 'match_field'), ' ', JSONExtractString(item, 'matcher_type'), ' ', JSONExtractString(item, 'pattern'))",
        "record_type": "JSONExtractString(item, 'record_type')",
        "match_field": "JSONExtractString(item, 'match_field')",
        "matcher_type": "JSONExtractString(item, 'matcher_type')",
        "pattern": "JSONExtractString(item, 'pattern')",
        "case_sensitive": "toUInt8(JSONExtractBool(item, 'case_sensitive'))",
        "priority": "toInt32(JSONExtractInt(item, 'priority'))",
    }),
    "http": ("http_rules", {
        "rule_key": "concat(JSONExtractString(item, 'http_part'), ' ', JSONExtractString(item, 'header_name'), ' ', JSONExtractString(item, 'matcher_type'), ' ', JSONExtractString(item, 'pattern'))",
        "http_part": "JSONExtractString(item, 'http_part')",
        "header_name": "JSONExtractString(item, 'header_name')",
        "matcher_type": "JSONExtractString(item, 'matcher_type')",
        "pattern": "JSONExtractString(item, 'pattern')",
        "case_sensitive": "toUInt8(JSONExtractBool(item, 'case_sensitive'))",
        "path_scope": "JSONExtractString(item, 'path_scope')",
        "priority": "toInt32(JSONExtractInt(item, 'priority'))",
    }),
    "ptr": ("ptr_rules", {
        "rule_key": "concat(JSONExtractString(item, 'matcher_type'), ' ', JSONExtractString(item, 'pattern'))",
        "matcher_type": "JSONExtractString(item, 'matcher_type')",
        "pattern": "JSONExtractString(item, 'pattern')",
    }),
    "certificate": ("certificate_identities", {
        "rule_key": "concat(JSONExtractString(item, 'identity_type'), ' ', JSONExtractString(item, 'identity_value'))",
        "identity_type": "JSONExtractString(item, 'identity_type')",
        "pattern": "JSONExtractString(item, 'identity_value')",
    }),
}

# Kind-specific columns in migration order with their empty values.
_RULE_SPECIFIC = (
    ("rule_key", "''"), ("record_type", "''"), ("match_field", "''"), ("http_part", "''"),
    ("header_name", "''"), ("matcher_type", "''"), ("pattern", "''"), ("case_sensitive", "toUInt8(0)"),
    ("path_scope", "''"), ("identity_type", "''"), ("asn", "toUInt32(0)"),
)

RULE_COLUMNS = (
    "provider_slug", "service_key", "kind", *(name for name, _ in _RULE_SPECIFIC),
    "confidence", "priority", "note", "source", "source_url",
    "status", "first_seen", "last_seen", "missing_since", "removed_at", "removal_action", "restored_at",
    "loaded_at",
)


def _rules_select(database: str, source: str, kind: str) -> str:
    array, specific = _RULE_KINDS[kind]
    columns = ",\n    ".join(f"{specific.get(name, empty)} AS {name}" for name, empty in _RULE_SPECIFIC)
    priority = specific.get("priority", "toInt32(0)")
    return f"""SELECT
    JSONExtractString(json, 'slug') AS provider_slug,
    JSONExtractString(svc, 'service_key') AS service_key,
    '{kind}' AS kind,
    {columns},
    toFloat32(JSONExtractFloat(item, 'confidence')) AS confidence,
    {priority} AS priority,
    JSONExtractString(item, 'note') AS note,
    JSONExtractString(item, 'source') AS source,
    JSONExtractString(item, 'source_url') AS source_url,{_LIFECYCLE},
    %(loaded_at)s AS loaded_at
FROM `{database}`.`{source}`
ARRAY JOIN JSONExtractArrayRaw(json, 'services') AS svc
ARRAY JOIN JSONExtractArrayRaw(svc, 'evidence', '{array}') AS item"""


def _rules_sql(database: str, source: str) -> str:
    union = "\nUNION ALL\n".join(_rules_select(database, source, kind) for kind in _RULE_KINDS)
    return f"INSERT INTO `{database}`.`{RULES_TABLE}` ({', '.join(RULE_COLUMNS)})\n{union}"


def load_statements(database: str, source_table: str = DOCUMENTS_S3_TABLE) -> list[tuple[str, str]]:
    """(target table, statement) in load order; each binds %(loaded_at)s."""
    return [
        (SERVICES_TABLE, _services_sql(database, source_table)),
        (IP_RANGES_TABLE, _ip_ranges_sql(database, source_table)),
        (RULES_TABLE, _rules_sql(database, source_table)),
    ]
