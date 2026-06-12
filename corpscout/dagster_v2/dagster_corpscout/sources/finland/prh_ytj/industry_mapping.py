"""Refresh Finland PRH YTJ industry-to-NACE mappings."""

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

INDUSTRY_NACE_MAPPING_TABLE = "fi_prhytj_industry_nace_mappings"
NACE_REFERENCE_DATABASE = "corpscout_reference"
NACE_CODES_TABLE = "nace_codes"
BUSINESS_LINES_TABLE = "fi_prhytj_business_lines"
BUSINESS_LINE_DESCRIPTIONS_TABLE = "fi_prhytj_business_line_descriptions"

INDUSTRY_NACE_MAPPING_COLUMNS = [
    "source_code_set",
    "source_code",
    "source_code_prefix4",
    "source_code_dotted4",
    "source_extra_digit",
    "source_description_en",
    "nace_revision",
    "nace_code",
    "nace_normalized_code",
    "nace_section_code",
    "nace_division_code",
    "nace_group_code",
    "nace_class_code",
    "nace_title_en",
    "mapping_method",
    "mapping_status",
    "mapped_at",
]


@dataclass(frozen=True)
class IndustryNACEMappingRefreshResult:
    mapping_table: str
    rows: int
    mapped_rows: int
    unmapped_rows: int
    mapped_at: datetime


def refresh_industry_nace_mappings(
    clickhouse,
    *,
    mapped_at: datetime | None = None,
) -> IndustryNACEMappingRefreshResult:
    database = clickhouse.database
    client = clickhouse.client()

    reference_rows = int(
        client.query(
            "SELECT count() "
            f"FROM {_qualified_table(NACE_REFERENCE_DATABASE, NACE_CODES_TABLE)} "
            "WHERE `revision` IN ('2', '2.1') "
            "AND `level_name` = 'class' "
            "AND `active` = true"
        ).result_rows[0][0]
    )
    if reference_rows == 0:
        raise ValueError("active NACE class reference rows are required for revisions 2 or 2.1")

    mapped_at = mapped_at or datetime.now(timezone.utc)
    mapped_at = mapped_at.astimezone(timezone.utc).replace(
        microsecond=(mapped_at.microsecond // 1000) * 1000
    )

    temp_table = _temp_table_name()
    temp_qualified = _qualified_table(database, temp_table)
    mapping_qualified = _qualified_table(database, INDUSTRY_NACE_MAPPING_TABLE)

    client.command(f"DROP TABLE IF EXISTS {temp_qualified}")
    temp_created = False
    try:
        client.command(f"CREATE TABLE {temp_qualified} AS {mapping_qualified}")
        temp_created = True
        client.command(f"TRUNCATE TABLE IF EXISTS {temp_qualified}")
        client.command(
            build_industry_nace_mapping_insert_query(
                database=database,
                target_table=temp_table,
                mapped_at=mapped_at,
            )
        )
        rows, mapped_rows, unmapped_rows = client.query(
            "SELECT count(), "
            "countIf(`mapping_status` = 'mapped'), "
            f"countIf(`mapping_status` = 'unmapped') FROM {temp_qualified}"
        ).result_rows[0]
        client.command(f"EXCHANGE TABLES {mapping_qualified} AND {temp_qualified}")
    finally:
        if temp_created:
            client.command(f"DROP TABLE IF EXISTS {temp_qualified}")

    return IndustryNACEMappingRefreshResult(
        mapping_table=f"{database}.{INDUSTRY_NACE_MAPPING_TABLE}",
        rows=int(rows),
        mapped_rows=int(mapped_rows),
        unmapped_rows=int(unmapped_rows),
        mapped_at=mapped_at,
    )


def build_industry_nace_mapping_insert_query(
    *,
    database: str,
    target_table: str,
    mapped_at: datetime,
) -> str:
    columns = ", ".join(_quote_ident(column) for column in INDUSTRY_NACE_MAPPING_COLUMNS)
    return f"""INSERT INTO {_qualified_table(database, target_table)} ({columns})
WITH
descriptions_en AS (
  SELECT
    business_id,
    source_run_id,
    business_line_item_hash,
    argMax(nullIf(description, ''), ingested_at) AS source_description_en
  FROM {_qualified_table(database, BUSINESS_LINE_DESCRIPTIONS_TABLE)}
  WHERE language_code = '3'
  GROUP BY business_id, source_run_id, business_line_item_hash
),
raw_industries AS (
  SELECT
    nullIf(bl.business_line_code_set, '') AS source_code_set,
    nullIf(bl.business_line_type, '') AS source_code,
    d.source_description_en AS source_description_en,
    bl.ingested_at AS ingested_at
  FROM {_qualified_table(database, BUSINESS_LINES_TABLE)} AS bl
  LEFT JOIN descriptions_en AS d
    ON d.business_id = bl.business_id
   AND d.source_run_id = bl.source_run_id
   AND d.business_line_item_hash = bl.source_item_hash
  WHERE nullIf(bl.business_line_type, '') IS NOT NULL
),
industries AS (
  SELECT
    source_code_set,
    source_code,
    argMax(source_description_en, ingested_at) AS source_description_en
  FROM raw_industries
  GROUP BY source_code_set, source_code
),
candidates AS (
  SELECT
    source_code_set,
    source_code,
    if(length(ifNull(source_code, '')) >= 4, substring(ifNull(source_code, ''), 1, 4), NULL) AS source_code_prefix4,
    if(length(ifNull(source_code, '')) >= 4, concat(substring(ifNull(source_code, ''), 1, 2), '.', substring(ifNull(source_code, ''), 3, 2)), NULL) AS source_code_dotted4,
    if(length(ifNull(source_code, '')) >= 5, substring(ifNull(source_code, ''), 5), NULL) AS source_extra_digit,
    source_description_en,
    multiIf(source_code_set = 'TOIMI4', '2.1', source_code_set = 'TOIMI3', '2', '') AS candidate_revision
  FROM industries
)
SELECT
  source_code_set,
  source_code,
  source_code_prefix4,
  source_code_dotted4,
  source_extra_digit,
  source_description_en,
  nullIf(candidate_revision, '') AS nace_revision,
  nullIf(nace.code, '') AS nace_code,
  nullIf(nace.normalized_code, '') AS nace_normalized_code,
  nullIf(nace.section_code, '') AS nace_section_code,
  nullIf(nace.division_code, '') AS nace_division_code,
  nullIf(nace.group_code, '') AS nace_group_code,
  nullIf(nace.class_code, '') AS nace_class_code,
  nullIf(nace.title, '') AS nace_title_en,
  multiIf(nace.normalized_code != '', 'toimi_5_digit_prefix', candidate_revision != '' AND source_code_prefix4 IS NOT NULL, 'toimi_prefix_unmatched', 'unsupported_code_set') AS mapping_method,
  if(nace.normalized_code != '', 'mapped', 'unmapped') AS mapping_status,
  {_datetime64_literal(mapped_at)} AS `mapped_at`
FROM candidates
LEFT JOIN {_qualified_table(NACE_REFERENCE_DATABASE, NACE_CODES_TABLE)} AS nace
  ON nace.revision = candidate_revision
 AND nace.level_name = 'class'
 AND nace.normalized_code = source_code_prefix4
 AND nace.active = true"""


def _temp_table_name() -> str:
    return f"{INDUSTRY_NACE_MAPPING_TABLE}_refresh_{uuid.uuid4().hex}"


def _qualified_table(database: str, table: str) -> str:
    return f"{_quote_ident(database)}.{_quote_ident(table)}"


def _quote_ident(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _datetime64_literal(value: datetime) -> str:
    formatted = value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:23]
    return f"toDateTime64('{formatted}', 3, 'UTC')"
