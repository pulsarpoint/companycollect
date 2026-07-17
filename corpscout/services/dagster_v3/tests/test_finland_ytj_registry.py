import json

from dagster_v3.defs.finland_ytj.registry import legal_form_json, registration_flags_json

RAW = json.dumps(
    {
        "businessId": {"value": "0112038-9", "registrationDate": "1978-03-15"},
        "companyForms": [
            {
                "type": "16",
                "descriptions": [
                    {"languageCode": "1", "description": "Osakeyhtiö"},
                    {"languageCode": "2", "description": "Aktiebolag"},
                    {"languageCode": "3", "description": "Limited company"},
                ],
                "registrationDate": "1980-01-01",
                "endDate": "1997-08-31",
                "version": 1,
            },
            {
                "type": "17",
                "descriptions": [
                    {"languageCode": "1", "description": "Julkinen osakeyhtiö"},
                    {"languageCode": "3", "description": "Public limited company"},
                ],
                "registrationDate": "1997-09-01",
                "version": 1,
            },
        ],
        "registeredEntries": [
            {"type": "1", "register": "1", "registrationDate": "1896-12-19", "authority": "2"},
            {"type": "80", "register": "6", "registrationDate": "1994-06-01", "authority": "1"},
            {"type": "55", "register": "5", "registrationDate": "1995-03-01", "authority": "1",
             "endDate": "2020-01-01"},
        ],
    }
)


def test_legal_form_picks_current_form_with_all_languages():
    payload = json.loads(legal_form_json(RAW))
    assert payload["code"] == "17"
    assert payload["description_fi"] == "Julkinen osakeyhtiö"
    assert payload["description_en"] == "Public limited company"
    assert payload["description_sv"] is None  # absent language stays null
    assert payload["registration_date"] == "1997-09-01"


def test_legal_form_prefers_latest_when_multiple_current():
    raw = json.loads(RAW)
    for form in raw["companyForms"]:
        form.pop("endDate", None)
    payload = json.loads(legal_form_json(json.dumps(raw)))
    assert payload["code"] == "17"  # later registrationDate wins


def test_registration_flags_respect_end_dates():
    payload = json.loads(registration_flags_json(RAW))
    assert payload["is_vat_registered"] == 1        # register 6, current
    assert payload["is_prepayment_registered"] == 0  # register 5, END-DATED
    assert payload["is_employer_registered"] == 0    # no register 7 entry


def test_null_and_garbage_inputs():
    assert legal_form_json(None) is None
    assert registration_flags_json(None) is None
    assert legal_form_json("not json") is None
    assert registration_flags_json("not json") is None
    assert legal_form_json("{}") is None  # no companyForms
    assert json.loads(registration_flags_json("{}"))["is_vat_registered"] == 0


def test_legal_form_survives_non_numeric_version():
    raw = json.dumps(
        {"companyForms": [{"type": "16", "registrationDate": "2020-01-01", "version": "not-an-int"}]}
    )
    payload = json.loads(legal_form_json(raw))
    assert payload["code"] == "16"
