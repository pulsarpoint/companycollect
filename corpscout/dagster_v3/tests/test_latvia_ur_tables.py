from dagster_v3.defs.latvia_ur import tables


def test_entities_columns_has_regcode_primary_key():
    columns = tables.LATVIA_UR_ENTITIES_COLUMNS
    assert columns["regcode"] == {"data_type": "text", "nullable": False}


def test_entities_columns_cover_expected_fields():
    expected = {
        "country_iso2",
        "source_slug",
        "source_run_id",
        "source_line_number",
        "source_record_id",
        "source_payload_hash",
        "regcode",
        "vat_id",
        "sepa",
        "legal_name",
        "name_in_quotes",
        "legal_form_code",
        "legal_form_text",
        "legal_form_description_en",
        "regtype_code",
        "regtype_text",
        "registered_date",
        "terminated_date",
        "closed_flag",
        "status",
        "is_active",
        "address",
        "postal_code",
        "address_id",
        "region_code",
        "city_code",
        "atvk_code",
        "reregistration_term",
        "source_url",
        "raw_entity",
    }
    assert set(tables.LATVIA_UR_ENTITIES_COLUMNS) == expected


def test_copy_dlt_columns_is_a_deep_copy():
    copied = tables.copy_dlt_columns(tables.LATVIA_UR_ENTITIES_COLUMNS)
    copied["regcode"]["nullable"] = True
    assert tables.LATVIA_UR_ENTITIES_COLUMNS["regcode"]["nullable"] is False
