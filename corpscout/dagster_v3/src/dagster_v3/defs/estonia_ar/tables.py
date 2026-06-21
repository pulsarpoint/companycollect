from typing import Any

DLT_DATASET_NAME = "estonia_ar"
ENTITIES_TABLE = "entities"

# DuckDB entities (dlt) schema. Column ORDER defines the ClickHouse export order
# (minus CLICKHOUSE_EXCLUDED_COLUMNS) and must match migration 000024.
ESTONIA_AR_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "reg_code": {"data_type": "text", "nullable": False},
    "name": {"data_type": "text"},
    "vat_id": {"data_type": "text"},
    "legal_form_original": {"data_type": "text"},
    "legal_form_en": {"data_type": "text"},
    "legal_form_subtype_original": {"data_type": "text"},
    "legal_form_subtype_en": {"data_type": "text"},
    "status_code": {"data_type": "text"},
    "status_original": {"data_type": "text"},
    "status_en": {"data_type": "text"},
    "is_active": {"data_type": "bool"},
    "first_entry_date": {"data_type": "date"},
    "address": {"data_type": "text"},
    "ehak_code": {"data_type": "text"},
    "location": {"data_type": "text"},
    "postal_code": {"data_type": "text"},
    "address_id": {"data_type": "text"},
    "company_url": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "raw_entity": {"data_type": "text"},
}


def copy_dlt_columns(columns: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: dict(spec) for name, spec in columns.items()}


# ClickHouse export target (schema owned by clickhouse/migrations/000024_corpscout_ee_companies).
ESTONIA_AR_DATABASE = "corpscout"
EE_COMPANIES_TABLE = "ee_companies"
QUALIFIED_EE_COMPANIES_TABLE = f"{ESTONIA_AR_DATABASE}.{EE_COMPANIES_TABLE}"

EE_COMPANIES_COLUMNS = tuple(ESTONIA_AR_ENTITIES_COLUMNS)


# Columns kept in DuckDB staging but NOT exported to ClickHouse (raw source JSON +
# incompressible per-row hash; nothing queries them). See dagster_v3/CLAUDE.md.
# For this fresh module they are simply absent from the migration DDL.
CLICKHOUSE_EXCLUDED_COLUMNS = frozenset(
    {"raw_entity", "raw_financial_record", "source_payload_hash"}
)


def _export_columns(columns: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(c for c in columns if c not in CLICKHOUSE_EXCLUDED_COLUMNS)


EE_COMPANIES_EXPORT_COLUMNS = _export_columns(EE_COMPANIES_COLUMNS)
