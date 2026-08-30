import hashlib
import json
from contextlib import contextmanager
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from dagster_v3.defs.sweden_ratsit.normalization import (
    RATSIT_COMPANY_COLUMNS,
    RATSIT_COMPANY_INDUSTRY_CODE_COLUMNS,
    RATSIT_ESTABLISHMENT_COLUMNS,
    RATSIT_FINANCIAL_REPORT_COLUMNS,
    RATSIT_FINANCIAL_PERIOD_COLUMNS,
    RATSIT_NORMALIZED_TABLES,
    RATSIT_NORMALIZER_VERSION,
    RATSIT_RESPONSIBLE_PERSON_COLUMNS,
    LatestRatsitReport,
    insert_normalized_ratsit_reports,
    load_nace_rev_2_1_class_codes,
    normalize_ratsit_report,
    select_latest_unnormalized_ratsit_reports,
)

COMPANY_ID = "5560004615"
REQUESTED_URL = f"https://www.ratsit.se/{COMPANY_ID}"
SOURCE_URL = f"{REQUESTED_URL}-Skanska_AB"
NORMALIZED_AT = datetime(2026, 8, 28, 14, 0, tzinfo=UTC)


def _report_document() -> bytes:
    value = {
        "schema_version": 1,
        "parser_version": "ratsit-html-v1",
        "company_id": COMPANY_ID,
        "requested_url": REQUESTED_URL,
        "source_url": SOURCE_URL,
        "report": {
            "company": {
                "name": "Skanska AB",
                "organization_number": "556000-4615",
                "legal_form": "Aktiebolag",
                "status": "Aktiv",
                "address": {
                    "street": "Warfvinges väg 25",
                    "postal_code": "112 74",
                    "locality": "Stockholm",
                    "county": "Stockholms län",
                },
                "industry_codes": [
                    {
                        "code": "62900",
                        "description": "Annan it- och dataverksamhet",
                    },
                    {"code": "Uppgift saknas", "description": None},
                ],
                "business_description": "Building construction.",
                "summary": [
                    "First company-specific paragraph.",
                    (
                        "Informationen kommer från myndigheter, privata aktörer och "
                        "egna insamlingar från Ratsit. Läs mer om datan här."
                    ),
                    "Läs mer om intressant företagsstatistik i Stockholm kommun .",
                ],
            },
            "responsible_people": [
                {
                    "display_name": "Example Person (51)",
                    "role": "Ledamot",
                    "profile_url": "https://www.ratsit.se/Example-Person",
                },
                {"display_name": None, "role": "Extern firmatecknare"},
            ],
            "workplaces": [
                {
                    "name": "Skanska AB Stockholm",
                    "identifier": "12345678",
                    "industry": {"code": "41200", "description": "Construction"},
                    "address": {
                        "street": "Warfvinges väg 25",
                        "postal_code": "112 74",
                        "locality": "Stockholm",
                        "county": "Stockholms län",
                    },
                    "number_of_employees": "20-49 anställda",
                }
            ],
            "financials": [
                {
                    "scope": "company",
                    "monetary_unit": "MSEK",
                    "periods": [
                        {
                            "fiscal_year": 2025,
                            "period_start": "2025-01-01",
                            "period_end": "2025-12-31",
                            "period_months": 12,
                            "income_statement": {
                                "revenue": 177034.0,
                                "operating_costs": -165000.0,
                                "operating_profit": 12034.0,
                                "profit_after_financial_items": 11000.0,
                                "net_income": 5772.0,
                            },
                            "balance_sheet": {
                                "current_assets": 100.0,
                                "fixed_assets": 200.0,
                                "share_capital": 10.0,
                                "equity": 80.0,
                                "untaxed_reserves": 5.0,
                                "provisions": 4.0,
                                "long_term_liabilities": 70.0,
                                "current_liabilities": 120.0,
                                "liabilities": 190.0,
                                "total_assets": 300.0,
                                "balance_sheet_total": 300.0,
                            },
                            "key_ratios": {
                                "cash_liquidity_percent": 120.5,
                                "equity_ratio_percent": 26.7,
                                "net_profit_margin_percent": 3.3,
                                "ebitda": 15000.0,
                                "personnel_cost_per_employee_msek": 0.9,
                                "revenue_per_employee_msek": 2.1,
                                "revenue_change_percent": 4.2,
                                "average_salary": 52000.0,
                            },
                            "dividend": 3.5,
                            "employee_count": 30000,
                        },
                        {"fiscal_year": 2024},
                        {"fiscal_year": 2023, "employee_count": 29000},
                    ],
                }
            ],
            "people_at_address": [
                {
                    "name": "Address Person",
                    "age": 50,
                    "profile_url": "https://www.ratsit.se/Address-Person",
                }
            ],
            "coordinates": {"latitude": 59.335, "longitude": 18.01},
            "source_url": SOURCE_URL,
            "date_modified": "2026-08-28",
        },
    }
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _encoded_document(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _latest_report(document: bytes) -> LatestRatsitReport:
    return LatestRatsitReport(
        company_id=COMPANY_ID,
        result_sha256=hashlib.sha256(document).hexdigest(),
        result_bucket="source-sweden-ratsit",
        result_object_key=(
            f"sweden_ratsit/pilot/company_id={COMPANY_ID}/scan-1_report.json"
        ),
        result_size_bytes=len(document),
        schema_version=1,
        parser_version="ratsit-html-v1",
        requested_url=REQUESTED_URL,
        source_url=SOURCE_URL,
    )


def _row(columns: tuple[str, ...], values: tuple[object, ...]) -> dict[str, object]:
    return dict(zip(columns, values, strict=True))


def test_normalizer_maps_every_ratsit_json_segment() -> None:
    document = _report_document()

    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"4120", "6290"}),
    )

    company = _row(RATSIT_COMPANY_COLUMNS, normalized.company)
    assert company["organization_number"] == COMPANY_ID
    assert company["address_postal_code"] == "11274"
    assert company["source_date_modified"] == date(2026, 8, 28)
    assert company["industry_code_count"] == 1
    assert company["summary_count"] == 1
    assert company["responsible_people_count"] == 2
    assert company["establishment_count"] == 1
    assert company["financial_report_count"] == 1
    assert company["financial_period_count"] == 2
    assert "people_at_address_count" not in company

    assert len(normalized.company_industry_codes) == 1
    assert len(normalized.company_summaries) == 1
    assert len(normalized.responsible_people) == 2
    assert len(normalized.establishments) == 1
    assert len(normalized.financial_reports) == 1
    assert len(normalized.financial_periods) == 2
    assert not hasattr(normalized, "people_at_address")

    industry = _row(
        RATSIT_COMPANY_INDUSTRY_CODE_COLUMNS,
        normalized.company_industry_codes[0],
    )
    assert industry["source_industry_code"] == "62900"
    assert industry["source_industry_code_set"] == "SNI_2025"
    assert industry["industry_description_original"] == ("Annan it- och dataverksamhet")
    assert industry["nace_revision"] == "NACE_REV_2_1"
    assert industry["nace_code"] == "62.90"
    assert industry["nace_normalized_code"] == "6290"
    assert industry["nace_mapping_method"] == "sni_four_digit_prefix"
    assert industry["nace_mapping_status"] == "mapped"

    summary = _row(
        (
            "company_id",
            "result_sha256",
            "normalizer_version",
            "summary_index",
            "summary_text",
            "normalized_at",
        ),
        normalized.company_summaries[0],
    )
    assert summary["summary_index"] == 0
    assert summary["summary_text"] == "First company-specific paragraph."

    identified_person = _row(
        RATSIT_RESPONSIBLE_PERSON_COLUMNS,
        normalized.responsible_people[0],
    )
    assert identified_person["display_name_raw"] == "Example Person (51)"
    assert identified_person["name"] == "Example Person"
    assert identified_person["age"] == 51
    assert identified_person["identity_available"] is True

    role_only_person = _row(
        RATSIT_RESPONSIBLE_PERSON_COLUMNS,
        normalized.responsible_people[1],
    )
    assert role_only_person["name"] is None
    assert role_only_person["age"] is None
    assert role_only_person["identity_available"] is False
    assert role_only_person["role"] == "Extern firmatecknare"

    establishment = _row(
        RATSIT_ESTABLISHMENT_COLUMNS,
        normalized.establishments[0],
    )
    assert establishment["establishment_index"] == 0
    assert establishment["address_postal_code"] == "11274"
    assert establishment["number_of_employees_raw"] == "20-49 anställda"
    assert establishment["employee_count_min"] == 20
    assert establishment["employee_count_max"] == 49
    assert establishment["employee_count_open_ended"] is False

    financial_report = _row(
        RATSIT_FINANCIAL_REPORT_COLUMNS,
        normalized.financial_reports[0],
    )
    assert financial_report["period_count"] == 2

    financial_period = _row(
        RATSIT_FINANCIAL_PERIOD_COLUMNS,
        normalized.financial_periods[0],
    )
    assert financial_period["scope"] == "company"
    assert financial_period["period_kind"] == "financial_and_employment"
    assert financial_period["monetary_unit"] == "MSEK"
    assert financial_period["fiscal_year"] == 2025
    assert financial_period["revenue_amount"] == Decimal("177034.0")
    assert financial_period["equity_ratio_percent"] == Decimal("26.7")
    assert financial_period["employee_count"] == 30000

    employment_period = _row(
        RATSIT_FINANCIAL_PERIOD_COLUMNS,
        normalized.financial_periods[1],
    )
    assert employment_period["period_index"] == 2
    assert employment_period["period_kind"] == "employment_only"
    assert employment_period["employee_count"] == 29000

    assert normalized.statistics == {
        "summary_source_count": 3,
        "summary_retained_count": 1,
        "summary_ratsit_source_disclaimer_filtered_count": 1,
        "summary_municipality_statistics_cta_filtered_count": 1,
        "industry_source_count": 3,
        "industry_missing_marker_count": 1,
        "industry_mapped_count": 2,
        "industry_unmapped_count": 0,
        "employee_ranges_parsed_count": 1,
        "employee_ranges_unparsed_count": 0,
        "responsible_identity_available_count": 1,
        "responsible_role_only_count": 1,
        "responsible_name_unparsed_count": 0,
        "financial_periods_source_count": 3,
        "financial_periods_empty_omitted_count": 1,
        "financial_periods_employment_only_count": 1,
        "financial_periods_financial_only_count": 0,
        "financial_periods_financial_and_employment_count": 1,
    }


def test_normalizer_rejects_s3_bytes_that_do_not_match_catalog_hash() -> None:
    document = _report_document()
    report = _latest_report(document)
    wrong_hash_report = LatestRatsitReport(
        company_id=report.company_id,
        result_sha256="0" * 64,
        result_bucket=report.result_bucket,
        result_object_key=report.result_object_key,
        result_size_bytes=report.result_size_bytes,
        schema_version=report.schema_version,
        parser_version=report.parser_version,
        requested_url=report.requested_url,
        source_url=report.source_url,
    )

    with pytest.raises(ValueError, match="SHA-256"):
        normalize_ratsit_report(
            wrong_hash_report,
            document=document,
            normalized_at=NORMALIZED_AT,
            nace_class_codes=frozenset({"6290"}),
        )


@pytest.mark.parametrize(
    ("source_value", "expected_min", "expected_max", "expected_open_ended"),
    (
        ("0 anställda", 0, 0, False),
        ("1-4 anställda", 1, 4, False),
        ("10000- anställda", 10000, None, True),
        ("okänt", None, None, False),
    ),
)
def test_establishment_employee_range_normalization(
    source_value: str,
    expected_min: int | None,
    expected_max: int | None,
    expected_open_ended: bool,
) -> None:
    value = json.loads(_report_document())
    value["report"]["workplaces"][0]["number_of_employees"] = source_value
    document = _encoded_document(value)

    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"6290", "4120"}),
    )

    establishment = _row(
        RATSIT_ESTABLISHMENT_COLUMNS,
        normalized.establishments[0],
    )
    assert establishment["employee_count_min"] == expected_min
    assert establishment["employee_count_max"] == expected_max
    assert establishment["employee_count_open_ended"] is expected_open_ended
    assert normalized.statistics["employee_ranges_unparsed_count"] == int(
        source_value == "okänt"
    )


def test_normalizer_rejects_a_malformed_nonempty_postal_code() -> None:
    value = json.loads(_report_document())
    value["report"]["workplaces"][0]["address"]["postal_code"] = "SE-11274"
    document = _encoded_document(value)

    with pytest.raises(ValueError, match="postal code"):
        normalize_ratsit_report(
            _latest_report(document),
            document=document,
            normalized_at=NORMALIZED_AT,
            nace_class_codes=frozenset({"6290", "4120"}),
        )


def test_valid_unknown_sni_code_is_retained_as_unmapped() -> None:
    value = json.loads(_report_document())
    value["report"]["company"]["industry_codes"] = [
        {"code": "99999", "description": "Unknown valid code"}
    ]
    document = _encoded_document(value)

    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"4120"}),
    )

    industry = _row(
        RATSIT_COMPANY_INDUSTRY_CODE_COLUMNS,
        normalized.company_industry_codes[0],
    )
    assert industry["nace_normalized_code"] == "9999"
    assert industry["nace_mapping_status"] == "unmapped"
    assert normalized.statistics["industry_unmapped_count"] == 1


def test_normalizer_replays_establishment_sni_with_empty_description_delimiter() -> None:
    value = json.loads(_report_document())
    value["report"]["workplaces"][0]["industry"] = {
        "code": "84111 -",
        "description": None,
    }
    document = _encoded_document(value)

    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"6290", "8411"}),
    )

    establishment = _row(
        RATSIT_ESTABLISHMENT_COLUMNS,
        normalized.establishments[0],
    )
    assert establishment["source_industry_code"] == "84111"
    assert establishment["industry_description_original"] is None
    assert establishment["nace_code"] == "84.11"
    assert establishment["nace_normalized_code"] == "8411"
    assert establishment["nace_mapping_status"] == "mapped"


def test_company_text_containing_the_cta_phrase_is_not_filtered() -> None:
    value = json.loads(_report_document())
    value["report"]["company"]["summary"] = [
        (
            "Bolaget publicerar länkar med texten Läs mer om intressant "
            "företagsstatistik i sin egen tjänst."
        )
    ]
    document = _encoded_document(value)

    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"4120", "6290"}),
    )

    assert len(normalized.company_summaries) == 1
    assert normalized.statistics["summary_retained_count"] == 1


def test_financial_values_without_employment_are_financial_only() -> None:
    value = json.loads(_report_document())
    value["report"]["financials"][0]["periods"][0]["employee_count"] = None
    document = _encoded_document(value)

    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"4120", "6290"}),
    )

    period = _row(
        RATSIT_FINANCIAL_PERIOD_COLUMNS,
        normalized.financial_periods[0],
    )
    assert period["period_kind"] == "financial_only"
    assert normalized.statistics["financial_periods_financial_only_count"] == 1


class FakeClickHouseClient:
    def __init__(
        self,
        *,
        latest_rows: list[tuple[object, ...]],
        existing_rows: list[tuple[object, ...]],
        nace_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.latest_rows = latest_rows
        self.existing_rows = existing_rows
        self.nace_rows = nace_rows or []
        self.calls: list[tuple[str, object | None]] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(
        self,
        sql: str,
        parameters: object | None = None,
    ) -> list[tuple[object, ...]]:
        self.calls.append((sql, parameters))
        if "FROM system.tables" in sql:
            assert isinstance(parameters, dict)
            return [(table,) for table in parameters["tables"]]
        if "FROM corpscout.se_company_ratsit FINAL" in sql:
            return self.latest_rows
        if "FROM corpscout.se_ratsit_company FINAL" in sql:
            return self.existing_rows
        if "FROM corpscout.nace_categories FINAL" in sql:
            return self.nace_rows
        if sql.lstrip().startswith("INSERT INTO"):
            assert isinstance(parameters, list)
            self.inserts.append((sql, parameters))
            return []
        raise AssertionError(sql)


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def test_nace_loader_requires_current_rev_2_1_classes() -> None:
    client = FakeClickHouseClient(
        latest_rows=[],
        existing_rows=[],
        nace_rows=[("4120",), ("6290",)],
    )

    codes = load_nace_rev_2_1_class_codes(
        FakeClickHouseResource(client)  # type: ignore[arg-type]
    )

    assert codes == frozenset({"4120", "6290"})
    sql = next(
        sql for sql, _ in client.calls if "FROM corpscout.nace_categories FINAL" in sql
    )
    assert "classification_version = 'NACE_REV_2_1'" in sql
    assert "level = 'class'" in sql


def test_nace_loader_rejects_an_empty_reference() -> None:
    client = FakeClickHouseClient(latest_rows=[], existing_rows=[])

    with pytest.raises(ValueError, match="NACE Rev. 2.1 class reference is empty"):
        load_nace_rev_2_1_class_codes(
            FakeClickHouseResource(client)  # type: ignore[arg-type]
        )


def test_latest_success_selection_skips_an_already_normalized_hash() -> None:
    document = _report_document()
    latest = _latest_report(document)
    second_hash = "b" * 64
    client = FakeClickHouseClient(
        latest_rows=[
            (
                latest.company_id,
                latest.result_sha256,
                latest.result_bucket,
                latest.result_object_key,
                latest.result_size_bytes,
                latest.schema_version,
                latest.parser_version,
                latest.requested_url,
                latest.source_url,
            ),
            (
                "5560125790",
                second_hash,
                latest.result_bucket,
                "sweden_ratsit/pilot/company_id=5560125790/scan-2_report.json",
                100,
                1,
                "ratsit-html-v1",
                "https://www.ratsit.se/5560125790",
                "https://www.ratsit.se/5560125790-AB_Volvo",
            ),
        ],
        existing_rows=[(latest.company_id, latest.result_sha256)],
    )

    selection = select_latest_unnormalized_ratsit_reports(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        bucket_count=128,
        bucket_index=65,
    )

    assert selection.latest_success_count == 2
    assert selection.already_normalized_count == 1
    assert [report.company_id for report in selection.reports] == ["5560125790"]
    latest_sql = next(
        sql
        for sql, _ in client.calls
        if "FROM corpscout.se_company_ratsit FINAL" in sql
    )
    assert "WHERE outcome = 'success'" in latest_sql
    assert "modulo(CRC32(company_id), %(bucket_count)s)" in latest_sql
    assert "argMax(" in latest_sql
    assert "GROUP BY company_id" in latest_sql
    latest_parameters = next(
        parameters
        for sql, parameters in client.calls
        if "FROM corpscout.se_company_ratsit FINAL" in sql
    )
    assert latest_parameters == {"bucket_count": 128, "bucket_index": 65}
    existing_sql, existing_parameters = next(
        (sql, parameters)
        for sql, parameters in client.calls
        if "FROM corpscout.se_ratsit_company FINAL" in sql
    )
    assert "modulo(CRC32(company_id), %(bucket_count)s)" in existing_sql
    assert existing_parameters == {
        "normalizer_version": RATSIT_NORMALIZER_VERSION,
        "bucket_count": 128,
        "bucket_index": 65,
    }


def test_normalized_rows_are_inserted_with_company_completion_marker_last() -> None:
    document = _report_document()
    normalized = normalize_ratsit_report(
        _latest_report(document),
        document=document,
        normalized_at=NORMALIZED_AT,
        nace_class_codes=frozenset({"6290"}),
    )
    client = FakeClickHouseClient(latest_rows=[], existing_rows=[])

    counts = insert_normalized_ratsit_reports(
        FakeClickHouseResource(client),  # type: ignore[arg-type]
        reports=(normalized,),
    )

    assert counts == {
        "se_ratsit_company": 1,
        "se_ratsit_company_industry_codes": 1,
        "se_ratsit_company_summaries": 1,
        "se_ratsit_responsible_people": 2,
        "se_ratsit_establishments": 1,
        "se_ratsit_financial_reports": 1,
        "se_ratsit_financial_periods": 2,
    }
    inserted_tables = [
        next(table for table in RATSIT_NORMALIZED_TABLES if f".{table} " in sql)
        for sql, _ in client.inserts
    ]
    assert inserted_tables[-1] == "se_ratsit_company"
    assert set(inserted_tables) == set(RATSIT_NORMALIZED_TABLES)
    for sql, rows in client.inserts:
        table = next(table for table in RATSIT_NORMALIZED_TABLES if f".{table} " in sql)
        assert len(rows) == counts[table]


def test_normalizer_version_is_explicit() -> None:
    assert RATSIT_NORMALIZER_VERSION == "ratsit-normalizer-v2"
