"""Schema-conformance helpers for the canonical contact/domain table standard.

Any <src>_company_contacts / <src>_company_domains migration must match the
canonical DDL modulo table name — new sources cannot drift (spec: Testing).
"""

import re

from dagster_v3.contact_extraction import COMPANY_CONTACTS_COLUMNS, COMPANY_DOMAINS_COLUMNS

_CONTACTS_TYPES = {
    "country_iso2": "LowCardinality(String)", "source_slug": "LowCardinality(String)",
    "source_run_id": "String", "source_record_id": "String", "registry_id": "String",
    "contact_type": "LowCardinality(String)", "contact_type_raw": "LowCardinality(String)",
    "contact_value": "String", "source_field": "LowCardinality(String)",
    "is_current": "UInt8", "valid_to": "Nullable(Date)", "source_url": "String",
    "resolved_at": "DateTime64(3, 'UTC')",
}
_DOMAINS_TYPES = {
    "country_iso2": "LowCardinality(String)", "source_slug": "LowCardinality(String)",
    "source_run_id": "String", "source_record_id": "String", "registry_id": "String",
    "domain": "String", "domain_source": "LowCardinality(String)",
    "validation_method": "LowCardinality(String)", "confidence": "Float32",
    "website_url": "String", "website_normalized_url": "String", "website_host": "String",
    "is_current": "UInt8", "is_primary": "UInt8", "resolved_at": "DateTime64(3, 'UTC')",
}


def _extract_create(sql: str, table: str) -> re.Match:
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS corpscout\.{re.escape(table)}\s*\((.*?)\)\s*"
        rf"ENGINE\s*=\s*([^;]+);",
        sql, re.DOTALL,
    )
    assert match, f"no canonical CREATE for corpscout.{table}"
    return match


def _assert_ddl(sql, table, columns, types, order_by):
    match = _extract_create(sql, table)
    body, engine = match.group(1), " ".join(match.group(2).split())
    parsed = [line.strip().rstrip(",") for line in body.strip().splitlines() if line.strip()]
    names = [p.split()[0] for p in parsed]
    assert names == list(columns), f"{table}: column order mismatch: {names}"
    for parsed_line, name in zip(parsed, names):
        declared = " ".join(parsed_line.split()[1:])
        assert declared == types[name], f"{table}.{name}: {declared!r} != {types[name]!r}"
    assert engine == f"ReplacingMergeTree(resolved_at) ORDER BY {order_by}", engine


def assert_canonical_contacts_ddl(sql: str, table: str) -> None:
    _assert_ddl(sql, table, COMPANY_CONTACTS_COLUMNS, _CONTACTS_TYPES,
                "(registry_id, contact_type, contact_value)")


def assert_canonical_domains_ddl(sql: str, table: str) -> None:
    _assert_ddl(sql, table, COMPANY_DOMAINS_COLUMNS, _DOMAINS_TYPES,
                "(registry_id, domain)")
