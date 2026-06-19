from typing import Any

DLT_DATASET_NAME = "latvia_ur"
ENTITIES_TABLE = "entities"

LATVIA_UR_ENTITIES_COLUMNS: dict[str, dict[str, Any]] = {
    "country_iso2": {"data_type": "text"},
    "source_slug": {"data_type": "text"},
    "source_run_id": {"data_type": "text"},
    "source_line_number": {"data_type": "bigint"},
    "source_record_id": {"data_type": "text"},
    "source_payload_hash": {"data_type": "text"},
    "regcode": {"data_type": "text", "nullable": False},
    "vat_id": {"data_type": "text"},
    "sepa": {"data_type": "text"},
    "legal_name": {"data_type": "text"},
    "name_in_quotes": {"data_type": "text"},
    "legal_form_code": {"data_type": "text"},
    "legal_form_text": {"data_type": "text"},
    "legal_form_description_en": {"data_type": "text"},
    "regtype_code": {"data_type": "text"},
    "regtype_text": {"data_type": "text"},
    "registered_date": {"data_type": "text"},
    "terminated_date": {"data_type": "text"},
    "closed_flag": {"data_type": "text"},
    "status": {"data_type": "text"},
    "is_active": {"data_type": "bool"},
    "address": {"data_type": "text"},
    "postal_code": {"data_type": "text"},
    "address_id": {"data_type": "text"},
    "region_code": {"data_type": "text"},
    "city_code": {"data_type": "text"},
    "atvk_code": {"data_type": "text"},
    "reregistration_term": {"data_type": "text"},
    "source_url": {"data_type": "text"},
    "raw_entity": {"data_type": "text"},
}


def copy_dlt_columns(columns: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {name: dict(spec) for name, spec in columns.items()}
