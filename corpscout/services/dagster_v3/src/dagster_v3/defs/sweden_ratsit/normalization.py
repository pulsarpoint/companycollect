import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Literal

from dagster_clickhouse import ClickhouseResource
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

RATSIT_CLICKHOUSE_DATABASE = "corpscout"
RATSIT_SCAN_RESULT_TABLE = "se_company_ratsit"
RATSIT_NACE_CATEGORIES_TABLE = "nace_categories"
RATSIT_NORMALIZER_VERSION = "ratsit-normalizer-v2"
RATSIT_SUPPORTED_SCHEMA_VERSION = 1
RATSIT_SOURCE_INDUSTRY_CODE_SET = "SNI_2025"
RATSIT_NACE_REVISION = "NACE_REV_2_1"
RATSIT_NACE_MAPPING_METHOD = "sni_four_digit_prefix"

RATSIT_SOURCE_DISCLAIMER = (
    "Informationen kommer från myndigheter, privata aktörer och egna insamlingar "
    "från Ratsit. Läs mer om datan här."
)
RATSIT_MUNICIPALITY_STATISTICS_PREFIX = "Läs mer om intressant företagsstatistik i "
RATSIT_NORMALIZATION_STATISTIC_KEYS = (
    "summary_source_count",
    "summary_retained_count",
    "summary_ratsit_source_disclaimer_filtered_count",
    "summary_municipality_statistics_cta_filtered_count",
    "industry_source_count",
    "industry_missing_marker_count",
    "industry_mapped_count",
    "industry_unmapped_count",
    "employee_ranges_parsed_count",
    "employee_ranges_unparsed_count",
    "responsible_identity_available_count",
    "responsible_role_only_count",
    "responsible_name_unparsed_count",
    "financial_periods_source_count",
    "financial_periods_empty_omitted_count",
    "financial_periods_employment_only_count",
    "financial_periods_financial_only_count",
    "financial_periods_financial_and_employment_count",
)

RATSIT_COMPANY_TABLE = "se_ratsit_company"
RATSIT_COMPANY_INDUSTRY_CODES_TABLE = "se_ratsit_company_industry_codes"
RATSIT_COMPANY_SUMMARIES_TABLE = "se_ratsit_company_summaries"
RATSIT_RESPONSIBLE_PEOPLE_TABLE = "se_ratsit_responsible_people"
RATSIT_ESTABLISHMENTS_TABLE = "se_ratsit_establishments"
RATSIT_FINANCIAL_REPORTS_TABLE = "se_ratsit_financial_reports"
RATSIT_FINANCIAL_PERIODS_TABLE = "se_ratsit_financial_periods"

RATSIT_NORMALIZED_TABLES = (
    RATSIT_COMPANY_TABLE,
    RATSIT_COMPANY_INDUSTRY_CODES_TABLE,
    RATSIT_COMPANY_SUMMARIES_TABLE,
    RATSIT_RESPONSIBLE_PEOPLE_TABLE,
    RATSIT_ESTABLISHMENTS_TABLE,
    RATSIT_FINANCIAL_REPORTS_TABLE,
    RATSIT_FINANCIAL_PERIODS_TABLE,
)
RATSIT_INSERT_ORDER = (
    RATSIT_COMPANY_INDUSTRY_CODES_TABLE,
    RATSIT_COMPANY_SUMMARIES_TABLE,
    RATSIT_RESPONSIBLE_PEOPLE_TABLE,
    RATSIT_ESTABLISHMENTS_TABLE,
    RATSIT_FINANCIAL_REPORTS_TABLE,
    RATSIT_FINANCIAL_PERIODS_TABLE,
    RATSIT_COMPANY_TABLE,
)

RATSIT_COMPANY_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "schema_version",
    "parser_version",
    "requested_url",
    "source_url",
    "result_bucket",
    "result_object_key",
    "name",
    "organization_number",
    "legal_form",
    "status",
    "address_street",
    "address_postal_code",
    "address_locality",
    "address_county",
    "business_description",
    "latitude",
    "longitude",
    "source_date_modified",
    "industry_code_count",
    "summary_count",
    "responsible_people_count",
    "establishment_count",
    "financial_report_count",
    "financial_period_count",
    "normalized_at",
)
RATSIT_COMPANY_INDUSTRY_CODE_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "industry_index",
    "industry_code",
    "industry_description",
    "source_industry_code",
    "source_industry_code_set",
    "industry_description_original",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_mapping_method",
    "nace_mapping_status",
    "normalized_at",
)
RATSIT_COMPANY_SUMMARY_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "summary_index",
    "summary_text",
    "normalized_at",
)
RATSIT_RESPONSIBLE_PERSON_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "person_index",
    "display_name",
    "display_name_raw",
    "name",
    "age",
    "identity_available",
    "role",
    "profile_url",
    "normalized_at",
)
RATSIT_ESTABLISHMENT_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "establishment_index",
    "name",
    "identifier",
    "industry_code",
    "industry_description",
    "source_industry_code",
    "source_industry_code_set",
    "industry_description_original",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_mapping_method",
    "nace_mapping_status",
    "address_street",
    "address_postal_code",
    "address_locality",
    "address_county",
    "number_of_employees_raw",
    "employee_count_min",
    "employee_count_max",
    "employee_count_open_ended",
    "normalized_at",
)
RATSIT_FINANCIAL_REPORT_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "financial_report_index",
    "scope",
    "monetary_unit",
    "period_count",
    "normalized_at",
)
RATSIT_FINANCIAL_PERIOD_COLUMNS = (
    "company_id",
    "result_sha256",
    "normalizer_version",
    "financial_report_index",
    "period_index",
    "period_kind",
    "scope",
    "monetary_unit",
    "fiscal_year",
    "period_start",
    "period_end",
    "period_months",
    "revenue_amount",
    "operating_costs_amount",
    "operating_profit_amount",
    "profit_after_financial_items_amount",
    "net_income_amount",
    "current_assets_amount",
    "fixed_assets_amount",
    "share_capital_amount",
    "equity_amount",
    "untaxed_reserves_amount",
    "provisions_amount",
    "long_term_liabilities_amount",
    "current_liabilities_amount",
    "liabilities_amount",
    "total_assets_amount",
    "balance_sheet_total_amount",
    "cash_liquidity_percent",
    "equity_ratio_percent",
    "net_profit_margin_percent",
    "ebitda_amount",
    "personnel_cost_per_employee_msek",
    "revenue_per_employee_msek",
    "revenue_change_percent",
    "average_salary",
    "dividend_amount",
    "employee_count",
    "normalized_at",
)
RATSIT_TABLE_COLUMNS = {
    RATSIT_COMPANY_TABLE: RATSIT_COMPANY_COLUMNS,
    RATSIT_COMPANY_INDUSTRY_CODES_TABLE: RATSIT_COMPANY_INDUSTRY_CODE_COLUMNS,
    RATSIT_COMPANY_SUMMARIES_TABLE: RATSIT_COMPANY_SUMMARY_COLUMNS,
    RATSIT_RESPONSIBLE_PEOPLE_TABLE: RATSIT_RESPONSIBLE_PERSON_COLUMNS,
    RATSIT_ESTABLISHMENTS_TABLE: RATSIT_ESTABLISHMENT_COLUMNS,
    RATSIT_FINANCIAL_REPORTS_TABLE: RATSIT_FINANCIAL_REPORT_COLUMNS,
    RATSIT_FINANCIAL_PERIODS_TABLE: RATSIT_FINANCIAL_PERIOD_COLUMNS,
}

type ClickHouseRow = tuple[object, ...]
type MonetaryUnit = Literal["SEK", "TSEK", "MSEK"]


@dataclass(frozen=True)
class LatestRatsitReport:
    company_id: str
    result_sha256: str
    result_bucket: str
    result_object_key: str
    result_size_bytes: int
    schema_version: int
    parser_version: str
    requested_url: str
    source_url: str


@dataclass(frozen=True)
class RatsitNormalizationSelection:
    latest_success_count: int
    already_normalized_count: int
    reports: tuple[LatestRatsitReport, ...]


@dataclass(frozen=True)
class NormalizedRatsitReport:
    company: ClickHouseRow
    company_industry_codes: tuple[ClickHouseRow, ...]
    company_summaries: tuple[ClickHouseRow, ...]
    responsible_people: tuple[ClickHouseRow, ...]
    establishments: tuple[ClickHouseRow, ...]
    financial_reports: tuple[ClickHouseRow, ...]
    financial_periods: tuple[ClickHouseRow, ...]
    statistics: dict[str, int]

    def rows_by_table(self) -> dict[str, tuple[ClickHouseRow, ...]]:
        return {
            RATSIT_COMPANY_TABLE: (self.company,),
            RATSIT_COMPANY_INDUSTRY_CODES_TABLE: self.company_industry_codes,
            RATSIT_COMPANY_SUMMARIES_TABLE: self.company_summaries,
            RATSIT_RESPONSIBLE_PEOPLE_TABLE: self.responsible_people,
            RATSIT_ESTABLISHMENTS_TABLE: self.establishments,
            RATSIT_FINANCIAL_REPORTS_TABLE: self.financial_reports,
            RATSIT_FINANCIAL_PERIODS_TABLE: self.financial_periods,
        }


@dataclass(frozen=True)
class _NormalizedIndustry:
    source_code: str
    description_original: str | None
    nace_code: str
    nace_normalized_code: str
    mapping_status: Literal["mapped", "unmapped"]


class _RatsitJsonModel(BaseModel):
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)


class _Address(_RatsitJsonModel):
    street: str | None = None
    postal_code: str | None = None
    locality: str | None = None
    county: str | None = None


class _Industry(_RatsitJsonModel):
    code: str | None = None
    description: str | None = None


class _Company(_RatsitJsonModel):
    name: str
    organization_number: str
    legal_form: str | None = None
    status: str | None = None
    address: _Address = Field(default_factory=_Address)
    industry_codes: list[_Industry] = Field(default_factory=list)
    business_description: str | None = None
    summary: list[str] = Field(default_factory=list)


class _ResponsiblePerson(_RatsitJsonModel):
    display_name: str | None = None
    role: str | None = None
    profile_url: str | None = None


class _Establishment(_RatsitJsonModel):
    name: str | None = None
    identifier: str | None = None
    industry: _Industry | None = None
    address: _Address = Field(default_factory=_Address)
    number_of_employees: str | None = None


class _IncomeStatement(_RatsitJsonModel):
    revenue: Decimal | None = None
    operating_costs: Decimal | None = None
    operating_profit: Decimal | None = None
    profit_after_financial_items: Decimal | None = None
    net_income: Decimal | None = None


class _BalanceSheet(_RatsitJsonModel):
    current_assets: Decimal | None = None
    fixed_assets: Decimal | None = None
    share_capital: Decimal | None = None
    equity: Decimal | None = None
    untaxed_reserves: Decimal | None = None
    provisions: Decimal | None = None
    long_term_liabilities: Decimal | None = None
    current_liabilities: Decimal | None = None
    liabilities: Decimal | None = None
    total_assets: Decimal | None = None
    balance_sheet_total: Decimal | None = None


class _KeyRatios(_RatsitJsonModel):
    cash_liquidity_percent: Decimal | None = None
    equity_ratio_percent: Decimal | None = None
    net_profit_margin_percent: Decimal | None = None
    ebitda: Decimal | None = None
    personnel_cost_per_employee_msek: Decimal | None = None
    revenue_per_employee_msek: Decimal | None = None
    revenue_change_percent: Decimal | None = None
    average_salary: Decimal | None = None


class _FinancialPeriod(_RatsitJsonModel):
    fiscal_year: int = Field(ge=1800, le=2200)
    period_start: date | None = None
    period_end: date | None = None
    period_months: int | None = Field(default=None, gt=0, le=65535)
    income_statement: _IncomeStatement = Field(default_factory=_IncomeStatement)
    balance_sheet: _BalanceSheet = Field(default_factory=_BalanceSheet)
    key_ratios: _KeyRatios = Field(default_factory=_KeyRatios)
    dividend: Decimal | None = None
    employee_count: int | None = Field(default=None, ge=0, le=4294967295)


class _FinancialReport(_RatsitJsonModel):
    scope: str
    monetary_unit: MonetaryUnit | None = None
    periods: list[_FinancialPeriod] = Field(default_factory=list)


class _PersonAtAddress(_RatsitJsonModel):
    name: str
    age: int | None = Field(default=None, ge=0, le=65535)
    profile_url: str | None = None


class _Coordinates(_RatsitJsonModel):
    latitude: float | None = Field(default=None, ge=-90, le=90)
    longitude: float | None = Field(default=None, ge=-180, le=180)


class _Report(_RatsitJsonModel):
    company: _Company
    responsible_people: list[_ResponsiblePerson] = Field(default_factory=list)
    workplaces: list[_Establishment] = Field(default_factory=list)
    financials: list[_FinancialReport] = Field(default_factory=list)
    people_at_address: list[_PersonAtAddress] = Field(default_factory=list)
    coordinates: _Coordinates = Field(default_factory=_Coordinates)
    source_url: str
    date_modified: date | None = None


class _ReportEnvelope(_RatsitJsonModel):
    schema_version: int = Field(gt=0)
    parser_version: str
    company_id: str
    requested_url: str
    source_url: str
    report: _Report


def select_latest_unnormalized_ratsit_reports(
    clickhouse: ClickhouseResource,
) -> RatsitNormalizationSelection:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_SCAN_RESULT_TABLE, *RATSIT_NORMALIZED_TABLES),
    )
    with clickhouse.get_connection() as client:
        latest_rows = client.execute(
            f"""
            SELECT
                company_id,
                latest.1 AS result_sha256,
                latest.2 AS result_bucket,
                latest.3 AS result_object_key,
                latest.4 AS result_size_bytes,
                latest.5 AS schema_version,
                latest.6 AS parser_version,
                latest.7 AS requested_url,
                latest.8 AS source_url
            FROM
            (
                SELECT
                    company_id,
                    argMax(
                        tuple(
                            toString(result_sha256),
                            result_bucket,
                            result_object_key,
                            result_size_bytes,
                            schema_version,
                            parser_version,
                            requested_url,
                            source_url
                        ),
                        tuple(fetched_at, recorded_at, scan_id)
                    ) AS latest
                FROM {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_SCAN_RESULT_TABLE} FINAL
                WHERE outcome = 'success'
                GROUP BY company_id
            )
            ORDER BY company_id
            """
        )
        existing_rows = client.execute(
            f"""
            SELECT company_id, toString(result_sha256)
            FROM {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_COMPANY_TABLE} FINAL
            WHERE normalizer_version = %(normalizer_version)s
            """,
            {"normalizer_version": RATSIT_NORMALIZER_VERSION},
        )

    normalized_keys = {(str(row[0]), str(row[1])) for row in existing_rows}
    latest_reports = tuple(
        _latest_report_from_clickhouse_row(row) for row in latest_rows
    )
    candidates = tuple(
        report
        for report in latest_reports
        if (report.company_id, report.result_sha256) not in normalized_keys
    )
    return RatsitNormalizationSelection(
        latest_success_count=len(latest_reports),
        already_normalized_count=len(latest_reports) - len(candidates),
        reports=candidates,
    )


def load_nace_rev_2_1_class_codes(
    clickhouse: ClickhouseResource,
) -> frozenset[str]:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RATSIT_CLICKHOUSE_DATABASE,
        tables=(RATSIT_NACE_CATEGORIES_TABLE,),
    )
    with clickhouse.get_connection() as client:
        rows = client.execute(
            f"""
            SELECT normalized_code
            FROM {RATSIT_CLICKHOUSE_DATABASE}.{RATSIT_NACE_CATEGORIES_TABLE} FINAL
            WHERE classification_version = '{RATSIT_NACE_REVISION}'
              AND level = 'class'
            """
        )
    codes = frozenset(str(row[0]) for row in rows)
    if not codes:
        raise ValueError("NACE Rev. 2.1 class reference is empty")
    return codes


def normalize_ratsit_report(
    source: LatestRatsitReport,
    *,
    document: bytes,
    normalized_at: datetime,
    nace_class_codes: frozenset[str],
) -> NormalizedRatsitReport:
    _validate_source_document(source, document=document)
    _require_aware_timestamp(normalized_at)
    envelope = _parse_report_envelope(source, document=document)
    _validate_envelope(source, envelope=envelope)

    common = (
        source.company_id,
        source.result_sha256,
        RATSIT_NORMALIZER_VERSION,
    )
    industry_rows, company_industry_statistics = _industry_rows(
        common,
        industries=envelope.report.company.industry_codes,
        normalized_at=normalized_at,
        nace_class_codes=nace_class_codes,
    )
    summary_rows, summary_statistics = _summary_rows(
        common,
        summaries=envelope.report.company.summary,
        normalized_at=normalized_at,
    )
    responsible_rows, responsible_statistics = _responsible_person_rows(
        common,
        people=envelope.report.responsible_people,
        normalized_at=normalized_at,
    )
    establishment_rows, establishment_statistics = _establishment_rows(
        common,
        establishments=envelope.report.workplaces,
        normalized_at=normalized_at,
        nace_class_codes=nace_class_codes,
    )
    (
        financial_report_rows,
        financial_period_rows,
        financial_statistics,
    ) = _financial_rows(
        common,
        reports=envelope.report.financials,
        normalized_at=normalized_at,
    )

    company = envelope.report.company
    normalized_organization_number = re.sub(r"\D", "", company.organization_number)
    company_row = (
        *common,
        envelope.schema_version,
        envelope.parser_version,
        envelope.requested_url,
        envelope.source_url,
        source.result_bucket,
        source.result_object_key,
        _required_text(company.name, label="company name"),
        normalized_organization_number,
        _nullable_text(company.legal_form),
        _nullable_text(company.status),
        _nullable_text(company.address.street),
        _postal_code(company.address.postal_code, label="company"),
        _nullable_text(company.address.locality),
        _nullable_text(company.address.county),
        _nullable_text(company.business_description),
        envelope.report.coordinates.latitude,
        envelope.report.coordinates.longitude,
        envelope.report.date_modified,
        len(industry_rows),
        len(summary_rows),
        len(responsible_rows),
        len(establishment_rows),
        len(financial_report_rows),
        len(financial_period_rows),
        normalized_at,
    )
    _validate_row_width(RATSIT_COMPANY_TABLE, company_row)

    return NormalizedRatsitReport(
        company=company_row,
        company_industry_codes=industry_rows,
        company_summaries=summary_rows,
        responsible_people=responsible_rows,
        establishments=establishment_rows,
        financial_reports=financial_report_rows,
        financial_periods=financial_period_rows,
        statistics={
            **summary_statistics,
            "industry_source_count": (
                company_industry_statistics["source_count"]
                + establishment_statistics["industry_source_count"]
            ),
            "industry_missing_marker_count": (
                company_industry_statistics["missing_marker_count"]
                + establishment_statistics["industry_missing_marker_count"]
            ),
            "industry_mapped_count": (
                company_industry_statistics["mapped_count"]
                + establishment_statistics["industry_mapped_count"]
            ),
            "industry_unmapped_count": (
                company_industry_statistics["unmapped_count"]
                + establishment_statistics["industry_unmapped_count"]
            ),
            "employee_ranges_parsed_count": establishment_statistics[
                "employee_ranges_parsed_count"
            ],
            "employee_ranges_unparsed_count": establishment_statistics[
                "employee_ranges_unparsed_count"
            ],
            **responsible_statistics,
            **financial_statistics,
        },
    )


def insert_normalized_ratsit_reports(
    clickhouse: ClickhouseResource,
    *,
    reports: tuple[NormalizedRatsitReport, ...],
) -> dict[str, int]:
    rows_by_table: dict[str, list[ClickHouseRow]] = {
        table: [] for table in RATSIT_NORMALIZED_TABLES
    }
    for report in reports:
        for table, rows in report.rows_by_table().items():
            rows_by_table[table].extend(rows)

    counts = {table: len(rows_by_table[table]) for table in RATSIT_NORMALIZED_TABLES}
    with clickhouse.get_connection() as client:
        for table in RATSIT_INSERT_ORDER:
            rows = rows_by_table[table]
            if not rows:
                continue
            for row in rows:
                _validate_row_width(table, row)
            client.execute(
                f"INSERT INTO {RATSIT_CLICKHOUSE_DATABASE}.{table} "
                f"({', '.join(RATSIT_TABLE_COLUMNS[table])}) VALUES",
                rows,
            )
    return counts


def _latest_report_from_clickhouse_row(row: tuple[object, ...]) -> LatestRatsitReport:
    if len(row) != 9:
        raise ValueError(
            f"Expected 9 columns for latest Ratsit report, received {len(row)}"
        )
    return LatestRatsitReport(
        company_id=str(row[0]),
        result_sha256=str(row[1]),
        result_bucket=str(row[2]),
        result_object_key=str(row[3]),
        result_size_bytes=int(row[4]),
        schema_version=int(row[5]),
        parser_version=str(row[6]),
        requested_url=str(row[7]),
        source_url=str(row[8]),
    )


def _validate_source_document(source: LatestRatsitReport, *, document: bytes) -> None:
    if len(document) != source.result_size_bytes:
        raise ValueError(
            f"Ratsit S3 object size differs from catalog for {source.company_id}"
        )
    actual_sha256 = hashlib.sha256(document).hexdigest()
    if actual_sha256 != source.result_sha256:
        raise ValueError(
            f"Ratsit S3 object SHA-256 differs from catalog for {source.company_id}"
        )


def _parse_report_envelope(
    source: LatestRatsitReport,
    *,
    document: bytes,
) -> _ReportEnvelope:
    try:
        return _ReportEnvelope.model_validate_json(document)
    except ValidationError as error:
        issues = "; ".join(
            f"{'.'.join(str(part) for part in issue['loc'])}: {issue['msg']}"
            for issue in error.errors(include_input=False)
        )
        raise ValueError(
            f"Invalid Ratsit report JSON for {source.company_id}: {issues}"
        ) from error


def _validate_envelope(
    source: LatestRatsitReport,
    *,
    envelope: _ReportEnvelope,
) -> None:
    if envelope.schema_version != RATSIT_SUPPORTED_SCHEMA_VERSION:
        raise ValueError(
            f"Unsupported Ratsit schema version {envelope.schema_version} "
            f"for {source.company_id}"
        )
    expected_values = (
        ("company ID", envelope.company_id, source.company_id),
        ("schema version", envelope.schema_version, source.schema_version),
        ("parser version", envelope.parser_version, source.parser_version),
        ("requested URL", envelope.requested_url, source.requested_url),
        ("source URL", envelope.source_url, source.source_url),
        ("nested source URL", envelope.report.source_url, source.source_url),
    )
    for label, actual, expected in expected_values:
        if actual != expected:
            raise ValueError(
                f"Ratsit {label} differs from catalog for {source.company_id}"
            )

    requested_company_id = source.company_id[-10:]
    if envelope.requested_url != f"https://www.ratsit.se/{requested_company_id}":
        raise ValueError(f"Invalid Ratsit requested URL for {source.company_id}")
    if not envelope.source_url.startswith("https://www.ratsit.se/"):
        raise ValueError(f"Invalid Ratsit source URL for {source.company_id}")
    normalized_organization_number = re.sub(
        r"\D", "", envelope.report.company.organization_number
    )
    if normalized_organization_number != requested_company_id:
        raise ValueError(
            f"Ratsit organization number differs from company ID {source.company_id}"
        )
    expected_key_prefix = f"sweden_ratsit/pilot/company_id={source.company_id}/"
    if not source.result_object_key.startswith(expected_key_prefix):
        raise ValueError(f"Invalid Ratsit S3 object key for {source.company_id}")
    if not source.result_bucket:
        raise ValueError(f"Missing Ratsit S3 bucket for {source.company_id}")


def _industry_rows(
    common: tuple[str, str, str],
    *,
    industries: list[_Industry],
    normalized_at: datetime,
    nace_class_codes: frozenset[str],
) -> tuple[tuple[ClickHouseRow, ...], dict[str, int]]:
    _validate_array_size("industry_codes", len(industries))
    rows: list[ClickHouseRow] = []
    missing_marker_count = 0
    mapped_count = 0
    unmapped_count = 0
    for index, industry in enumerate(industries):
        normalized = _normalized_industry(
            industry,
            nace_class_codes=nace_class_codes,
            label=f"company industry {index}",
        )
        if normalized is None:
            missing_marker_count += 1
            continue
        mapped_count += int(normalized.mapping_status == "mapped")
        unmapped_count += int(normalized.mapping_status == "unmapped")
        rows.append(
            (
                *common,
                index,
                normalized.source_code,
                normalized.description_original,
                normalized.source_code,
                RATSIT_SOURCE_INDUSTRY_CODE_SET,
                normalized.description_original,
                RATSIT_NACE_REVISION,
                normalized.nace_code,
                normalized.nace_normalized_code,
                RATSIT_NACE_MAPPING_METHOD,
                normalized.mapping_status,
                normalized_at,
            )
        )
    return tuple(rows), {
        "source_count": len(industries),
        "missing_marker_count": missing_marker_count,
        "mapped_count": mapped_count,
        "unmapped_count": unmapped_count,
    }


def _summary_rows(
    common: tuple[str, str, str],
    *,
    summaries: list[str],
    normalized_at: datetime,
) -> tuple[tuple[ClickHouseRow, ...], dict[str, int]]:
    _validate_array_size("summary", len(summaries))
    rows: list[ClickHouseRow] = []
    filtered_counts = {
        "ratsit_source_disclaimer": 0,
        "municipality_statistics_cta": 0,
    }
    for index, summary in enumerate(summaries):
        text = _required_text(summary, label=f"summary paragraph {index}")
        reason = _summary_boilerplate_reason(text)
        if reason is not None:
            filtered_counts[reason] += 1
            continue
        rows.append((*common, index, text, normalized_at))
    return tuple(rows), {
        "summary_source_count": len(summaries),
        "summary_retained_count": len(rows),
        "summary_ratsit_source_disclaimer_filtered_count": filtered_counts[
            "ratsit_source_disclaimer"
        ],
        "summary_municipality_statistics_cta_filtered_count": filtered_counts[
            "municipality_statistics_cta"
        ],
    }


def _summary_boilerplate_reason(
    text: str,
) -> Literal["ratsit_source_disclaimer", "municipality_statistics_cta"] | None:
    normalized = re.sub(r"\s+", " ", text).strip()
    if normalized == RATSIT_SOURCE_DISCLAIMER:
        return "ratsit_source_disclaimer"
    if normalized.startswith(RATSIT_MUNICIPALITY_STATISTICS_PREFIX):
        return "municipality_statistics_cta"
    return None


def _responsible_person_rows(
    common: tuple[str, str, str],
    *,
    people: list[_ResponsiblePerson],
    normalized_at: datetime,
) -> tuple[tuple[ClickHouseRow, ...], dict[str, int]]:
    _validate_array_size("responsible_people", len(people))
    rows: list[ClickHouseRow] = []
    identity_available_count = 0
    role_only_count = 0
    name_unparsed_count = 0
    for index, person in enumerate(people):
        display_name_raw = _nullable_text(person.display_name)
        role = _nullable_text(person.role)
        profile_url = _profile_url(person.profile_url)
        if display_name_raw is None and role is None and profile_url is None:
            raise ValueError(f"Ratsit responsible person {index} has no values")
        name, age, name_was_unparsed = _responsible_name(display_name_raw)
        identity_available = display_name_raw is not None or profile_url is not None
        identity_available_count += int(identity_available)
        role_only_count += int(not identity_available)
        name_unparsed_count += int(name_was_unparsed)
        rows.append(
            (
                *common,
                index,
                display_name_raw,
                display_name_raw,
                name,
                age,
                identity_available,
                role,
                profile_url,
                normalized_at,
            )
        )
    return tuple(rows), {
        "responsible_identity_available_count": identity_available_count,
        "responsible_role_only_count": role_only_count,
        "responsible_name_unparsed_count": name_unparsed_count,
    }


def _responsible_name(
    display_name_raw: str | None,
) -> tuple[str | None, int | None, bool]:
    if display_name_raw is None:
        return None, None, False
    match = re.fullmatch(r"(?P<name>.+?)\s+\((?P<age>\d{1,3})\)", display_name_raw)
    if match is None:
        return display_name_raw, None, True
    return match.group("name").strip(), int(match.group("age")), False


def _establishment_rows(
    common: tuple[str, str, str],
    *,
    establishments: list[_Establishment],
    normalized_at: datetime,
    nace_class_codes: frozenset[str],
) -> tuple[tuple[ClickHouseRow, ...], dict[str, int]]:
    _validate_array_size("workplaces", len(establishments))
    rows: list[ClickHouseRow] = []
    industry_source_count = 0
    industry_missing_marker_count = 0
    industry_mapped_count = 0
    industry_unmapped_count = 0
    employee_ranges_parsed_count = 0
    employee_ranges_unparsed_count = 0
    for index, establishment in enumerate(establishments):
        industry = None
        if establishment.industry is not None:
            industry_source_count += 1
            industry = _normalized_industry(
                establishment.industry,
                nace_class_codes=nace_class_codes,
                label=f"establishment {index} industry",
            )
            if industry is None:
                industry_missing_marker_count += 1
            else:
                industry_mapped_count += int(industry.mapping_status == "mapped")
                industry_unmapped_count += int(industry.mapping_status == "unmapped")
        employee_count_raw = _nullable_text(establishment.number_of_employees)
        employee_min, employee_max, employee_open_ended, employee_parsed = (
            _employee_range(employee_count_raw)
        )
        employee_ranges_parsed_count += int(employee_parsed)
        employee_ranges_unparsed_count += int(
            employee_count_raw is not None and not employee_parsed
        )
        row = (
            *common,
            index,
            _nullable_text(establishment.name),
            _nullable_text(establishment.identifier),
            industry.source_code if industry is not None else None,
            industry.description_original if industry is not None else None,
            industry.source_code if industry is not None else None,
            RATSIT_SOURCE_INDUSTRY_CODE_SET if industry is not None else "",
            industry.description_original if industry is not None else None,
            RATSIT_NACE_REVISION if industry is not None else "",
            industry.nace_code if industry is not None else None,
            industry.nace_normalized_code if industry is not None else None,
            RATSIT_NACE_MAPPING_METHOD if industry is not None else "",
            industry.mapping_status if industry is not None else "",
            _nullable_text(establishment.address.street),
            _postal_code(
                establishment.address.postal_code,
                label=f"establishment {index}",
            ),
            _nullable_text(establishment.address.locality),
            _nullable_text(establishment.address.county),
            employee_count_raw,
            employee_min,
            employee_max,
            employee_open_ended,
            normalized_at,
        )
        if all(value is None or value == "" for value in row[4:-1]):
            raise ValueError(f"Ratsit establishment {index} has no values")
        rows.append(row)
    return tuple(rows), {
        "industry_source_count": industry_source_count,
        "industry_missing_marker_count": industry_missing_marker_count,
        "industry_mapped_count": industry_mapped_count,
        "industry_unmapped_count": industry_unmapped_count,
        "employee_ranges_parsed_count": employee_ranges_parsed_count,
        "employee_ranges_unparsed_count": employee_ranges_unparsed_count,
    }


def _financial_rows(
    common: tuple[str, str, str],
    *,
    reports: list[_FinancialReport],
    normalized_at: datetime,
) -> tuple[
    tuple[ClickHouseRow, ...],
    tuple[ClickHouseRow, ...],
    dict[str, int],
]:
    _validate_array_size("financials", len(reports))
    report_rows: list[ClickHouseRow] = []
    period_rows: list[ClickHouseRow] = []
    period_kind_counts = {
        "employment_only": 0,
        "financial_only": 0,
        "financial_and_employment": 0,
    }
    source_period_count = 0
    empty_period_count = 0
    for report_index, report in enumerate(reports):
        _validate_array_size(
            f"financials[{report_index}].periods",
            len(report.periods),
        )
        scope = _required_text(
            report.scope,
            label=f"financial report {report_index} scope",
        )
        retained_period_rows: list[ClickHouseRow] = []
        for period_index, period in enumerate(report.periods):
            source_period_count += 1
            period_kind = _financial_period_kind(period)
            if period_kind is None:
                empty_period_count += 1
                continue
            period_kind_counts[period_kind] += 1
            retained_period_rows.append(
                _financial_period_row(
                    common,
                    report_index=report_index,
                    period_index=period_index,
                    period_kind=period_kind,
                    scope=scope,
                    monetary_unit=report.monetary_unit,
                    period=period,
                    normalized_at=normalized_at,
                )
            )
        report_rows.append(
            (
                *common,
                report_index,
                scope,
                report.monetary_unit,
                len(retained_period_rows),
                normalized_at,
            )
        )
        period_rows.extend(retained_period_rows)
    return (
        tuple(report_rows),
        tuple(period_rows),
        {
            "financial_periods_source_count": source_period_count,
            "financial_periods_empty_omitted_count": empty_period_count,
            "financial_periods_employment_only_count": period_kind_counts[
                "employment_only"
            ],
            "financial_periods_financial_only_count": period_kind_counts[
                "financial_only"
            ],
            "financial_periods_financial_and_employment_count": period_kind_counts[
                "financial_and_employment"
            ],
        },
    )


def _financial_period_kind(
    period: _FinancialPeriod,
) -> (
    Literal[
        "employment_only",
        "financial_only",
        "financial_and_employment",
    ]
    | None
):
    financial_values = (
        *period.income_statement.model_dump().values(),
        *period.balance_sheet.model_dump().values(),
        *period.key_ratios.model_dump().values(),
        period.dividend,
    )
    has_financial_values = any(value is not None for value in financial_values)
    has_employee_count = period.employee_count is not None
    if has_financial_values and has_employee_count:
        return "financial_and_employment"
    if has_financial_values:
        return "financial_only"
    if has_employee_count:
        return "employment_only"
    return None


def _financial_period_row(
    common: tuple[str, str, str],
    *,
    report_index: int,
    period_index: int,
    period_kind: Literal[
        "employment_only",
        "financial_only",
        "financial_and_employment",
    ],
    scope: str,
    monetary_unit: MonetaryUnit | None,
    period: _FinancialPeriod,
    normalized_at: datetime,
) -> ClickHouseRow:
    income = period.income_statement
    balance = period.balance_sheet
    ratios = period.key_ratios
    return (
        *common,
        report_index,
        period_index,
        period_kind,
        scope,
        monetary_unit,
        period.fiscal_year,
        period.period_start,
        period.period_end,
        period.period_months,
        income.revenue,
        income.operating_costs,
        income.operating_profit,
        income.profit_after_financial_items,
        income.net_income,
        balance.current_assets,
        balance.fixed_assets,
        balance.share_capital,
        balance.equity,
        balance.untaxed_reserves,
        balance.provisions,
        balance.long_term_liabilities,
        balance.current_liabilities,
        balance.liabilities,
        balance.total_assets,
        balance.balance_sheet_total,
        ratios.cash_liquidity_percent,
        ratios.equity_ratio_percent,
        ratios.net_profit_margin_percent,
        ratios.ebitda,
        ratios.personnel_cost_per_employee_msek,
        ratios.revenue_per_employee_msek,
        ratios.revenue_change_percent,
        ratios.average_salary,
        period.dividend,
        period.employee_count,
        normalized_at,
    )


def _profile_url(value: str | None) -> str | None:
    profile_url = _nullable_text(value)
    if profile_url is not None and not profile_url.startswith("https://www.ratsit.se/"):
        raise ValueError("Ratsit profile URL must use https://www.ratsit.se/")
    return profile_url


def _normalized_industry(
    industry: _Industry,
    *,
    nace_class_codes: frozenset[str],
    label: str,
) -> _NormalizedIndustry | None:
    code = _nullable_text(industry.code)
    description = _nullable_text(industry.description)
    if code is None and description is None:
        raise ValueError(f"Ratsit {label} has no code or description")
    if code is not None and (
        code.casefold() == "uppgift saknas"
        or re.fullmatch(r"00000\s*-?", code) is not None
    ):
        return None
    if code is None or re.fullmatch(r"\d{5}", code) is None:
        raise ValueError(f"Ratsit {label} has invalid SNI code {code!r}")
    nace_normalized_code = code[:4]
    return _NormalizedIndustry(
        source_code=code,
        description_original=description,
        nace_code=f"{code[:2]}.{code[2:4]}",
        nace_normalized_code=nace_normalized_code,
        mapping_status=(
            "mapped" if nace_normalized_code in nace_class_codes else "unmapped"
        ),
    )


def _employee_range(
    value: str | None,
) -> tuple[int | None, int | None, bool, bool]:
    if value is None:
        return None, None, False, False
    normalized = re.sub(r"\s+", " ", value).strip()
    normalized = re.sub(r"\s+anställd(?:a)?$", "", normalized, flags=re.IGNORECASE)
    exact_match = re.fullmatch(r"(?P<count>\d+)", normalized)
    if exact_match is not None:
        count = int(exact_match.group("count"))
        if count <= 4294967295:
            return count, count, False, True
        return None, None, False, False
    range_match = re.fullmatch(
        r"(?P<minimum>\d+)\s*-\s*(?P<maximum>\d*)",
        normalized,
    )
    if range_match is None:
        return None, None, False, False
    minimum = int(range_match.group("minimum"))
    maximum_text = range_match.group("maximum")
    maximum = int(maximum_text) if maximum_text else None
    if minimum > 4294967295 or (
        maximum is not None and (maximum > 4294967295 or maximum < minimum)
    ):
        return None, None, False, False
    return minimum, maximum, maximum is None, True


def _postal_code(value: str | None, *, label: str) -> str | None:
    postal_code = _nullable_text(value)
    if postal_code is None:
        return None
    normalized = re.sub(r"\s+", "", postal_code)
    if re.fullmatch(r"\d{5}", normalized) is None:
        raise ValueError(f"Ratsit {label} postal code must contain five digits")
    return normalized


def _nullable_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def _required_text(value: str, *, label: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError(f"Ratsit {label} must not be blank")
    return stripped


def _validate_array_size(label: str, size: int) -> None:
    if size > 65535:
        raise ValueError(f"Ratsit {label} exceeds the UInt16 row-index limit")


def _require_aware_timestamp(value: datetime) -> None:
    if value.utcoffset() is None:
        raise ValueError("Ratsit normalization timestamp must include a timezone")


def _validate_row_width(table: str, row: ClickHouseRow) -> None:
    expected = len(RATSIT_TABLE_COLUMNS[table])
    if len(row) != expected:
        raise ValueError(
            f"Ratsit row for {table} has {len(row)} values, expected {expected}"
        )
