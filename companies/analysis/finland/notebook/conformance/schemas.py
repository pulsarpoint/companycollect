"""Canonical table column sets (subset Finland populates) — the contract in
`companies/analysis/_canonical/canonical_schema.md`, as Polars-checkable schemas.
Only required columns and their dtypes are asserted by validate.py.
"""

import polars as pl

REGISTRATIONS: dict[str, pl.DataType] = {
    "registration_uid": pl.Utf8, "company_uid": pl.Utf8, "country": pl.Utf8,
    "registration_number": pl.Utf8, "registry_source": pl.Utf8, "is_primary": pl.UInt8,
    "entity_role": pl.Utf8, "legal_name": pl.Utf8, "legal_form_code": pl.Utf8,
    "lifecycle_status": pl.Utf8, "is_active": pl.UInt8,
    "incorporation_date": pl.Date, "dissolution_date": pl.Date,
    "addr_street": pl.Utf8, "addr_post_code": pl.Utf8, "addr_city": pl.Utf8,
    "addr_municipality_code": pl.Utf8, "addr_country": pl.Utf8,
    "activity_code": pl.Utf8, "activity_scheme": pl.Utf8,
    "vat_number": pl.Utf8, "eu_id": pl.Utf8, "lei": pl.Utf8, "primary_website": pl.Utf8,
    "source_run_id": pl.Utf8, "ingested_at": pl.Datetime, "updated_at": pl.Datetime,
}

COMPANY: dict[str, pl.DataType] = {
    "company_uid": pl.Utf8, "uid_scheme": pl.Utf8, "lei": pl.Utf8,
    "primary_name": pl.Utf8, "status": pl.Utf8, "legal_form_code": pl.Utf8,
    "home_country": pl.Utf8, "incorporation_date": pl.Date, "dissolution_date": pl.Date,
    "registration_count": pl.UInt16, "operating_countries": pl.List(pl.Utf8),
    "primary_website": pl.Utf8, "sources": pl.List(pl.Utf8),
    "resolution_version": pl.Utf8, "first_seen_at": pl.Datetime, "updated_at": pl.Datetime,
}

FINANCIALS: dict[str, pl.DataType] = {
    "company_uid": pl.Utf8, "registration_uid": pl.Utf8, "country": pl.Utf8,
    "statement_id": pl.Utf8, "period_start": pl.Date, "period_end": pl.Date,
    "period_type": pl.Utf8, "period_reference": pl.Utf8, "basis": pl.Utf8,
    "currency": pl.Utf8, "metric_code": pl.Utf8, "value": pl.Float64,
    "source_metric_id": pl.Utf8, "registry_source": pl.Utf8, "mapping_version": pl.Utf8,
    "source_run_id": pl.Utf8, "ingested_at": pl.Datetime, "updated_at": pl.Datetime,
}

COMPANY_WEBSITES: dict[str, pl.DataType] = {
    "website_uid": pl.Utf8, "company_uid": pl.Utf8, "registration_uid": pl.Utf8,
    "country": pl.Utf8, "scope": pl.Utf8, "url": pl.Utf8, "normalized_url": pl.Utf8,
    "host": pl.Utf8, "is_primary": pl.UInt8, "source_kind": pl.Utf8,
    "discovery_method": pl.Utf8, "registry_source": pl.Utf8, "confidence": pl.Float32,
    "is_live": pl.UInt8, "first_seen_at": pl.Datetime, "last_seen_at": pl.Datetime,
    "updated_at": pl.Datetime,
}

# Finland fills these 4. The other 4 contract tables (persons, company_people,
# company_contacts, company_relationships) are KNOWN-ABSENT in Finland open data.
POPULATED = {"registrations": REGISTRATIONS, "company": COMPANY,
             "financials": FINANCIALS, "company_websites": COMPANY_WEBSITES}
