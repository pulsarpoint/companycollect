import json

from dagster_v3.defs.latvia_ur import resources

SAMPLE_CSV = (
    "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
    "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
    "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
    # active SIA, address contains a quoted semicolon and doubled quotes
    "40103550818;LV95ZZZ40103550818;\"SIA \"\"Psihologs; Tava kabata\"\"\";SIA;"
    "\"Psihologs; Tava kabata\";\"\";0;K;Komercreģistrs;SIA;"
    "Sabiedrība ar ierobežotu atbildību;2012-05-31;;;\"Valmiera, \"\"Centrs\"\"\";"
    "4201;103045133;100015821;0;0885162;\n"
    # terminated + closed IK
    "41202013815;LV53ZZZ41202013815;\"IK \"\"KRASTNIEKI A I\"\"\";IK;KRASTNIEKI A I;"
    "\"\";0;K;Komercreģistrs;IK;Individuālais komersants;1998-02-26;2014-04-10;L;"
    "\"Dundagas nov., Kolka\";3275;103045134;100015821;0;0885162;\n"
)


def _rows(csv_text: str) -> list[dict]:
    return list(
        resources.iter_latvia_ur_entity_rows_from_text(csv_text, run_id="run-1")
    )


def test_parses_quoted_semicolons_and_doubled_quotes():
    rows = _rows(SAMPLE_CSV)
    assert len(rows) == 2
    first = rows[0]
    assert first["regcode"] == "40103550818"
    assert first["legal_name"] == 'SIA "Psihologs; Tava kabata"'
    assert first["name_in_quotes"] == "Psihologs; Tava kabata"
    assert first["address"] == 'Valmiera, "Centrs"'


def test_active_entity_status_and_vat():
    first = _rows(SAMPLE_CSV)[0]
    assert first["status"] == "active"
    assert first["is_active"] is True
    assert first["terminated_date"] == ""
    assert first["closed_flag"] == ""
    assert first["vat_id"] == "LV40103550818"
    assert first["legal_form_code"] == "SIA"
    assert first["legal_form_description_en"] == "Private limited company"
    assert first["postal_code"] == "4201"
    assert first["atvk_code"] == "0885162"
    assert first["country_iso2"] == "LV"
    assert first["source_slug"] == "latvia_ur_register"
    assert first["source_run_id"] == "run-1"
    assert first["source_record_id"] == "40103550818"
    assert first["source_line_number"] == 1


def test_terminated_entity_status():
    second = _rows(SAMPLE_CSV)[1]
    assert second["regcode"] == "41202013815"
    assert second["status"] == "terminated"
    assert second["is_active"] is False
    assert second["terminated_date"] == "2014-04-10"
    assert second["closed_flag"] == "L"
    assert second["legal_form_code"] == "IK"
    assert second["legal_form_description_en"] == "Individual merchant (sole trader)"


def test_closed_without_termination_is_inactive():
    csv_text = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
        "40000000001;;ZS Test;ZS;Test;;0;K;Komercreģistrs;ZS;Zemnieka saimniecība;"
        "2000-01-01;;L;Some address;1001;1;1;1;0010000;\n"
    )
    row = _rows(csv_text)[0]
    assert row["status"] == "closed"
    assert row["is_active"] is False


def test_raw_entity_is_json_of_source_columns():
    first = _rows(SAMPLE_CSV)[0]
    raw = json.loads(first["raw_entity"])
    assert raw["regcode"] == "40103550818"
    assert raw["type"] == "SIA"


def test_blank_regcode_rows_are_skipped():
    csv_text = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
        ";;No regcode;;;;0;K;K;SIA;SIA;2000-01-01;;;addr;1001;1;1;1;0010000;\n"
        "40000000002;;Good;;Good;;0;K;K;SIA;SIA;2000-01-01;;;addr;1001;1;1;1;0010000;\n"
    )
    rows = _rows(csv_text)
    assert [r["regcode"] for r in rows] == ["40000000002"]


def test_malformed_overlong_row_does_not_crash():
    # A row with more fields than the header must not crash the load (the extra
    # values land under the "_extra" rest-key and the row is still emitted).
    csv_text = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
        "40000000003;;Bad Row;;Bad;;0;K;K;SIA;SIA;2000-01-01;;;addr;1001;1;1;1;"
        "0010000;;extra1;extra2\n"
    )
    rows = _rows(csv_text)
    assert len(rows) == 1
    assert rows[0]["regcode"] == "40000000003"
    assert rows[0]["status"] == "active"


def test_empty_csv_yields_no_rows():
    header_only = (
        "regcode;sepa;name;name_before_quotes;name_in_quotes;name_after_quotes;"
        "without_quotes;regtype;regtype_text;type;type_text;registered;terminated;"
        "closed;address;index;addressid;region;city;atvk;reregistration_term\n"
    )
    assert _rows(header_only) == []
