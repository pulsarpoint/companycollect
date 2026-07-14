import json
from decimal import Decimal
from typing import Any

import dagster_v3.defs.norway_brreg.assets as brreg_assets
from dagster_v3.defs.norway_brreg import resources as brreg_resources


def _entity_record(**overrides: Any) -> dict[str, Any]:
    record = {
        "organisasjonsnummer": "923609016",
        "navn": "EQUINOR ASA",
        "organisasjonsform": {"kode": "ASA", "beskrivelse": "Allmennaksjeselskap"},
        "hjemmeside": "www.equinor.com",
        "registreringsdatoEnhetsregisteret": "1995-03-12",
        "registrertIMvaregisteret": True,
        "naeringskode1": {"kode": "06.100", "beskrivelse": "Utvinning av raolje"},
        "naeringskode2": {"kode": "06.200", "beskrivelse": "Utvinning av naturgass"},
        "naeringskode3": {
            "kode": "19.200",
            "beskrivelse": "Produksjon av raffinerte petroleumsprodukter",
        },
        "vedtektsfestetFormaal": [
            "Aa utvikle, produsere og markedsfoere energi",
            "samt annen virksomhet",
        ],
        "aktivitet": [
            "Selv, eller gjennom andre selskaper aa utvikle energi",
            "og avledede produkter og tjenester",
        ],
        "antallAnsatte": 21467,
        "harRegistrertAntallAnsatte": True,
        "forretningsadresse": {
            "adresse": ["Forusbeen 50"],
            "postnummer": "4035",
            "poststed": "STAVANGER",
            "kommune": "STAVANGER",
            "kommunenummer": "1103",
            "landkode": "NO",
        },
        "stiftelsesdato": "1972-09-18",
        "registrertIForetaksregisteret": True,
        "sisteInnsendteAarsregnskap": "2024",
        "konkurs": False,
        "underAvvikling": False,
        "underTvangsavviklingEllerTvangsopplosning": False,
        "erIKonsern": True,
        "overordnetEnhet": "000000000",
        "_links": {
            "self": {
                "href": "https://data.brreg.no/enhetsregisteret/api/enheter/923609016"
            }
        },
    }
    record.update(overrides)
    return record


def test_streaming_json_dependency_is_available() -> None:
    import ijson

    assert ijson


def test_entity_resource_declares_explicit_table_schema() -> None:
    columns = brreg_resources.BRREG_ENTITIES_COLUMNS
    row = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")[0]

    assert set(columns) == set(row)
    assert columns["org_number"]["data_type"] == "text"
    assert columns["source_line_number"]["data_type"] == "bigint"
    assert columns["employee_count"]["data_type"] == "bigint"
    assert columns["is_active"]["data_type"] == "bool"
    assert columns["raw_entity"]["data_type"] == "text"


def test_norway_brreg_assets_do_not_expose_dlt_source_helpers() -> None:
    assert "norway_brreg_entities_source" not in brreg_assets.__dict__
    assert "_entities_resource" not in brreg_assets.__dict__
    assert "norway_brreg_pipeline" not in brreg_assets.__dict__


def test_norway_brreg_resources_do_not_expose_dlt_source_helpers() -> None:
    assert "norway_brreg_entities_source" not in brreg_resources.__dict__
    assert "_entities_resource" not in brreg_resources.__dict__
    assert "norway_brreg_financial_fetches_source" not in brreg_resources.__dict__
    assert "_financial_fetches_resource" not in brreg_resources.__dict__


def test_norway_brreg_assets_do_not_expose_pipeline_helpers() -> None:
    assert "run_norway_brreg_entities_dlt_pipeline" not in brreg_assets.__dict__
    assert "run_norway_brreg_financial_fetches_dlt_pipeline" not in brreg_assets.__dict__
    assert "run_norway_brreg_entities_dlt_pipeline" not in brreg_resources.__dict__
    assert "run_norway_brreg_financial_fetches_dlt_pipeline" not in brreg_resources.__dict__


def test_entity_rows_extract_company_spine_fields() -> None:
    rows = brreg_resources.build_entity_rows([_entity_record()], run_id="test-run")

    assert rows[0]["country_iso2"] == "NO"
    assert rows[0]["source_slug"] == "norway_brregenhet"
    assert rows[0]["source_run_id"] == "test-run"
    assert rows[0]["source_line_number"] == 1
    assert rows[0]["source_record_id"] == "923609016"
    assert rows[0]["org_number"] == "923609016"
    assert rows[0]["vat_id"] == "NO923609016MVA"
    assert rows[0]["legal_name"] == "EQUINOR ASA"
    assert rows[0]["legal_form_code"] == "ASA"
    assert rows[0]["legal_form_description_original"] == "Allmennaksjeselskap"
    assert rows[0]["legal_form_description_en"] == "Public limited company"
    assert rows[0]["nace1_code"] == "06.100"
    assert rows[0]["nace1_description_original"] == "Utvinning av raolje"
    assert rows[0]["nace1_description_en"] == ""
    assert rows[0]["nace2_code"] == "06.200"
    assert rows[0]["nace2_description_original"] == "Utvinning av naturgass"
    assert rows[0]["nace2_description_en"] == ""
    assert rows[0]["nace3_code"] == "19.200"
    assert (
        rows[0]["nace3_description_original"]
        == "Produksjon av raffinerte petroleumsprodukter"
    )
    assert rows[0]["nace3_description_en"] == ""
    assert (
        rows[0]["articles_purpose_original"]
        == "Aa utvikle, produsere og markedsfoere energi\nsamt annen virksomhet"
    )
    assert rows[0]["articles_purpose_en"] == ""
    assert (
        rows[0]["activity_text_original"]
        == "Selv, eller gjennom andre selskaper aa utvikle energi\n"
        "og avledede produkter og tjenester"
    )
    assert rows[0]["activity_text_en"] == ""
    assert rows[0]["employee_count"] == 21467
    assert rows[0]["status"] == "active"
    assert rows[0]["is_active"] is True
    assert rows[0]["last_submitted_accounts_year"] == "2024"
    assert json.loads(rows[0]["raw_entity"])["organisasjonsnummer"] == "923609016"


def test_norway_legal_form_description_en_uses_deterministic_mapping() -> None:
    rows = brreg_resources.build_entity_rows(
        [
            _entity_record(
                organisasjonsform={
                    "kode": "ENK",
                    "beskrivelse": "Enkeltpersonforetak",
                }
            )
        ],
        run_id="test-run",
    )

    assert rows[0]["legal_form_description_en"] == "Sole proprietorship"


def test_unknown_norway_legal_form_description_en_is_empty() -> None:
    rows = brreg_resources.build_entity_rows(
        [
            _entity_record(
                organisasjonsform={
                    "kode": "UNKNOWN",
                    "beskrivelse": "Ukjent",
                }
            )
        ],
        run_id="test-run",
    )

    assert rows[0]["legal_form_description_en"] == ""


def test_entity_rows_serialize_decimal_values_from_streaming_parser() -> None:
    rows = brreg_resources.build_entity_rows(
        [_entity_record(sourceDecimal=Decimal("1.25"))],
        run_id="test-run",
    )

    assert len(rows[0]["source_payload_hash"]) == 64
    assert json.loads(rows[0]["raw_entity"])["sourceDecimal"] == 1.25


def test_entity_status_derivation_handles_liquidation_and_bankruptcy() -> None:
    rows = brreg_resources.build_entity_rows(
        [
            _entity_record(organisasjonsnummer="1", konkurs=True),
            _entity_record(organisasjonsnummer="2", underAvvikling=True),
            _entity_record(
                organisasjonsnummer="3",
                underTvangsavviklingEllerTvangsopplosning=True,
            ),
        ],
        run_id="test-run",
    )

    assert [row["status"] for row in rows] == [
        "bankrupt",
        "liquidation",
        "compulsory_liquidation",
    ]
    assert [row["is_active"] for row in rows] == [False, False, False]

