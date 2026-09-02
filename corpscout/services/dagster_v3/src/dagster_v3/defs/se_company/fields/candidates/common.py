"""Shared contract of the SE company field-candidate extractors.

One candidate row per (company, field, source, source record). Every extractor SELECT
projects CANDIDATE_SELECT_COLUMNS in that order; candidate_rows_from_result binds the
result positionally into CandidateRows; publish_candidates appends them through
publish_with_stage's anti-join on (company_id, field, source, source_record_uid,
evidence_hash) so unchanged evidence is never rewritten. materialize_candidates is the one
driver every SQL extractor asset calls: page the changed companies (or the explicit scope),
extract, preview or publish.

value_json has two writers -- SQL for the six table extractors, Python for the LLM one -- so
the conventions live here twice, side by side: compare_key_text / compare_key_text_sql,
value_json_for / json_object_sql. Keys are sorted in both; counts, amounts and fiscal years
are JSON numbers in both (never quoted -- the projection reads them with JSONExtractRaw /
typed JSONExtract, which return NULL for quoted numbers); absent members are null in both.
"""

import json
import re
import unicodedata
from collections import defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import (
    DATABASE,
    EPOCH,
    SE_COMPANY_ID_PATTERN,
    normalized_se_company_ids,
    publish_with_stage,
)
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD_CANDIDATE,
    SE_COMPANY_FIELD_CANDIDATE_COLUMNS,
)

GROUP_NAME = "se_company_fields"
# The bare table name publish_with_stage / assert_clickhouse_tables_exist qualify themselves.
CANDIDATE_TABLE = SE_COMPANY_FIELD_CANDIDATE.split(".")[-1]
# Every extractor SELECT projects exactly these, in this order; candidate_rows_from_result
# binds by position, so a reordered projection would transpose values, not fail.
CANDIDATE_SELECT_COLUMNS = ("company_id", "field", "source_record_uid", "observed_at", "value", "value_json")
# The candidate table's identity: its ORDER BY plus the MATERIALIZED evidence hash.
CANDIDATE_ANTI_JOIN_COLUMNS = ("company_id", "field", "source", "source_record_uid", "evidence_hash")
CANDIDATE_INVALID_CONDITION = (
    "trim(company_id) = '' OR trim(field) = '' OR trim(source) = '' OR trim(source_record_uid) = '' "
    "OR trim(value) = '' OR NOT isValidJSON(value_json) OR JSONExtractString(value_json, 'compare_key') = ''"
)
# One explicit company_ids slice per statement: clickhouse-driver substitutes the id list
# client-side and the SCB statement embeds it three times; 5,000 ids x 3 copies is ~212 KB
# against ClickHouse's 262,144-byte default max_query_size (info.py measured it).
IDS_PER_STATEMENT = 5_000
SE_COMPANY_ID_MATCH = f"match(company_id, '{SE_COMPANY_ID_PATTERN}')"
SINCE_SQL = "parseDateTime64BestEffort(%(since)s, 3, 'UTC')"
WATERMARK_EPOCH_SQL = f"toDateTime64('{EPOCH}', 3, 'UTC')"
# Text a register writes when it has nothing to say; never a candidate.
PLACEHOLDER_VALUES = ("", "-", "--", ".", "n/a", "null", "none")


def changed_companies_scope_sql(*, source: str, changes_sql: str) -> str:
    """The scan every SQL extractor pages (parameters after_company_id, page_size, since):
    companies whose newest source change stamp is newer than BOTH their own newest
    extracted_at for this source and the ``since`` floor. Per company, so a run capped by
    max_companies leaves the remainder selected for the next run. ``changes_sql`` is the
    UNION ALL of (company_id, changed_at) members."""
    return f"""SELECT changes.company_id AS company_id
FROM (
    SELECT company_id, max(changed_at) AS changed_at
    FROM (
{changes_sql}
    )
    GROUP BY company_id
) AS changes
LEFT JOIN (
    SELECT company_id, max(extracted_at) AS extracted_at
    FROM {DATABASE}.{CANDIDATE_TABLE}
    WHERE source = '{source}'
    GROUP BY company_id
) AS watermark ON watermark.company_id = changes.company_id
WHERE match(changes.company_id, '{SE_COMPANY_ID_PATTERN}') AND changes.company_id > %(after_company_id)s
  AND changes.changed_at > greatest(ifNull(watermark.extracted_at, {WATERMARK_EPOCH_SQL}), {SINCE_SQL})
ORDER BY company_id
LIMIT %(page_size)s"""


@dataclass(frozen=True)
class CandidateRow:
    company_id: str
    field: str
    source: str
    source_record_uid: str
    value: str
    value_json: str
    observed_at: datetime
    extractor_version: str


class CandidateExtractConfig(dg.Config):
    """Run config shared by every extractor asset.

    ``execute`` False = preview: run the scan and the extraction, report what would be
    published, write nothing -- so a bare "Materialize" click in the Dagster UI is
    harmless, exactly as for se_company_info_clickhouse. ``company_ids`` bypasses the
    scan (the named companies are re-extracted whether or not they changed; the
    anti-join makes that free for unchanged evidence). ``since`` is an ISO timestamp
    ("2026-08-01 12:00:00.000") overriding the default watermark, which is the newest
    ``extracted_at`` this source ever wrote.
    """

    execute: bool = False
    company_ids: list[str] = Field(default_factory=list)
    max_companies: int | None = Field(default=None, ge=1)
    company_batch_size: int = Field(default=20_000, ge=1, le=20_000)
    since: str | None = None


# --- value_json, Python side ------------------------------------------------------------

_WHITESPACE = re.compile(r"\s+")


def compare_key_text(value: str) -> str:
    """NFKC, whitespace collapsed, trimmed, casefolded -- the agreement key for free text."""
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFKC", value)).strip().casefold()


def value_json_for(*, compare_key: str, **members: Any) -> str:
    """Compact JSON with sorted keys; ``compare_key`` is mandatory and never empty."""
    if not compare_key:
        raise ValueError("value_json needs a non-empty compare_key")
    return json.dumps({**members, "compare_key": compare_key}, ensure_ascii=False,
                      separators=(",", ":"), sort_keys=True)


# --- value_json, SQL side ---------------------------------------------------------------


def compare_key_text_sql(expr: str) -> str:
    """The SQL twin of compare_key_text. lowerUTF8 is not casefold (a German sharp s stays
    itself); the difference is confined to agreement counting between an LLM row and a
    table row for a handful of code points."""
    return f"lowerUTF8(trim(replaceRegexpAll(normalizeUTF8NFKC({expr}), '[[:space:]]+', ' ')))"


def clean_text_sql(expr: str) -> str:
    """Trimmed text, or '' when NULL, blank or a register placeholder."""
    placeholders = ", ".join(f"'{value}'" for value in PLACEHOLDER_VALUES)
    return f"if(lowerUTF8(trim(ifNull({expr}, ''))) IN ({placeholders}), '', trim(ifNull({expr}, '')))"


def json_string_sql(expr: str) -> str:
    """A JSON string token (or null for a NULL Nullable(String))."""
    return f"toJSONString({expr})"


def json_object_sql(members: Mapping[str, str]) -> str:
    """A JSON object from expressions that already yield JSON tokens, keys sorted like
    value_json_for: ``concat('{"a":', <a>, ',"b":', <b>, '}')``."""
    pieces: list[str] = []
    for index, (name, expr) in enumerate(sorted(members.items())):
        prefix = "{" if index == 0 else ","
        pieces.append(f"'{prefix}\"{name}\":'")
        pieces.append(expr)
    pieces.append("'}'")
    return "concat(" + ", ".join(pieces) + ")"


def nace_digits_sql(expr: str) -> str:
    """The published form of a NACE class code: dot-less four digits (64.19 -> 6419), the
    form se_company_info carries today and nace_categories.normalized_code is keyed by."""
    return f"replaceAll({expr}, '.', '')"


def nace_labels_cte_sql() -> str:
    """The current NACE class labels, code prefix stripped ("62.01 Computer programming" ->
    "Computer programming"), keyed by (classification_version, normalized_code)."""
    return (
        "SELECT classification_version, normalized_code, "
        "replaceRegexpOne(description_en, '^[0-9][0-9.]*[[:space:]]+', '') AS label_en\n"
        f"    FROM {DATABASE}.nace_categories FINAL\n"
        "    WHERE level = 'class' AND is_current = 1"
    )


def employee_count_json_sql(*, count: str, as_of: str, period: str) -> str:
    """count: an integer expression; as_of / period: String or Nullable(String) expressions."""
    return json_object_sql({
        "compare_key": json_string_sql(f"toString({count})"),
        "count": f"toString({count})",
        "as_of": json_string_sql(as_of),
        "period": json_string_sql(period),
    })


def latest_revenue_json_sql(*, amount: str, currency: str, amount_usd: str, fiscal_year: str, period_end: str) -> str:
    """amount: Decimal128(2); amount_usd: Nullable(Decimal128(2)); currency / period_end:
    String; fiscal_year: integer. Amounts travel as JSON NUMBERS with two decimals
    (toString of a Decimal128(2) renders 48000000000.00), amount_usd as null when unknown --
    the projection reads them with toDecimal128OrNull(JSONExtractRaw(...)), which returns
    NULL for a quoted number."""
    return json_object_sql({
        "compare_key": json_string_sql(f"concat(lowerUTF8({currency}), ':', toString({amount}), ':', toString({fiscal_year}))"),
        "amount": f"toString({amount})",
        "amount_usd": f"ifNull(toString({amount_usd}), 'null')",
        "currency": json_string_sql(currency),
        "fiscal_year": f"toString({fiscal_year})",
        "period_end": json_string_sql(period_end),
    })


def revenue_value_sql(*, amount: str, currency: str, fiscal_year: str) -> str:
    """The display form: ``SEK 48000000000.00 FY2024``."""
    return f"concat({currency}, ' ', toString({amount}), ' FY', toString({fiscal_year}))"


def financial_view_ctes_sql(view: str) -> str:
    """The CTEs shared by the two ``se_financials_*_current`` views (identical column
    names): one row per (company, fiscal year) narrowed to the newest period that carries
    each field. Views have no FINAL. source_record_uid is the sorted array's first element
    -- one element for Bolagsverket, and for ESEF every element normally names the same
    filing package."""
    return f"""financials AS (
    SELECT company_id, arraySort(source_record_uids)[1] AS source_record_uid,
        assumeNotNull(toDateTime64(report_period_end, 3, 'UTC')) AS observed_at,
        ifNull(toString(report_period_end), '') AS period_end,
        fiscal_year, currency,
        toDecimal128(revenue_amount_original, 2) AS amount,
        toDecimal128(revenue_amount_usd, 2) AS amount_usd,
        employees
    FROM {DATABASE}.{view}
    WHERE company_id IN %(company_ids)s AND report_period_end IS NOT NULL AND notEmpty(source_record_uids)
),
latest_employees AS (
    SELECT company_id, source_record_uid, observed_at, period_end, fiscal_year, assumeNotNull(employees) AS employees
    FROM financials
    WHERE employees IS NOT NULL
    ORDER BY observed_at DESC, fiscal_year DESC, source_record_uid DESC
    LIMIT 1 BY company_id
),
latest_revenue AS (
    SELECT company_id, source_record_uid, observed_at, period_end, fiscal_year, currency,
        assumeNotNull(amount) AS amount, amount_usd
    FROM financials
    WHERE amount IS NOT NULL AND currency != ''
    ORDER BY observed_at DESC, fiscal_year DESC, source_record_uid DESC
    LIMIT 1 BY company_id
)"""


FINANCIAL_MEMBERS_SQL = f"""SELECT company_id, 'employee_count' AS field, source_record_uid, observed_at, toString(employees) AS value,
    {employee_count_json_sql(count="employees", as_of="period_end", period="toString(fiscal_year)")} AS value_json
FROM latest_employees
UNION ALL
SELECT company_id, 'latest_revenue', source_record_uid, observed_at, {revenue_value_sql(amount="amount", currency="currency", fiscal_year="fiscal_year")},
    {latest_revenue_json_sql(amount="amount", currency="currency", amount_usd="amount_usd", fiscal_year="fiscal_year", period_end="period_end")}
FROM latest_revenue"""


# --- rows and publishing ---------------------------------------------------------------


def candidate_rows_from_result(
    rows: Sequence[Sequence[Any]], *, source: str, extractor_version: str
) -> list[CandidateRow]:
    """Bind a CANDIDATE_SELECT_COLUMNS result positionally. An empty value is a bug in the
    SQL (the table's has_value CHECK would reject it anyway) and is raised, not skipped."""
    out: list[CandidateRow] = []
    for row in rows:
        company_id, field, uid, observed_at, value, value_json = (
            str(row[0]), str(row[1]), str(row[2]), row[3], str(row[4]), str(row[5]))
        if not value.strip():
            raise ValueError(f"{source} candidate {company_id}/{field}/{uid} has an empty value; the SQL must filter it")
        out.append(CandidateRow(company_id, field, source, uid, value, value_json, observed_at, extractor_version))
    return out


def publish_candidates(
    clickhouse: ClickhouseResource, rows: Sequence[CandidateRow], *, source_run_id: str, extracted_at: datetime
) -> int:
    """Append ``rows`` whose (company_id, field, source, source_record_uid, evidence_hash)
    is not already in the table; returns how many were inserted. ``extracted_at`` is the
    ReplacingMergeTree version, so a changed value for an existing key wins at merge."""
    if not rows:
        return 0
    tuples = [
        (row.company_id, row.field, row.source, row.source_record_uid, row.value, row.value_json,
         row.observed_at, extracted_at, row.extractor_version, source_run_id)
        for row in rows
    ]
    counts = publish_with_stage(
        clickhouse=clickhouse, target=CANDIDATE_TABLE, insert_columns=SE_COMPANY_FIELD_CANDIDATE_COLUMNS,
        rows=tuples, invalid_condition=CANDIDATE_INVALID_CONDITION, new_versions_only=True,
        anti_join_columns=CANDIDATE_ANTI_JOIN_COLUMNS)
    return counts.inserted


# --- scan and paging -------------------------------------------------------------------


def clickhouse_stamp(moment: datetime) -> str:
    """Millisecond text for parseDateTime64BestEffort(..., 3, 'UTC'); the tz travels separately."""
    return moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


@dataclass
class PageWalk:
    selected: int = 0
    stopped_at_cap: bool = False


def iter_company_pages(
    clickhouse: ClickhouseResource, *, walk: PageWalk, scope: Sequence[str], scope_sql: str,
    scope_params: Mapping[str, Any], max_companies: int | None, company_batch_size: int,
) -> Iterator[list[str]]:
    """Pages of company ids: slices of the explicit ``scope`` when given (capped by
    max_companies), else pages of ``scope_sql`` resumed from the last id -- the same
    after_company_id / page_size paging info.py's scan uses, so a run capped below the
    table size stops with ``walk.stopped_at_cap`` rather than pretending it finished."""
    if scope:
        limited = scope if max_companies is None else scope[:max_companies]
        walk.stopped_at_cap = len(limited) < len(scope)
        for start in range(0, len(limited), company_batch_size):
            page = list(limited[start:start + company_batch_size])
            walk.selected += len(page)
            yield page
        return
    after = ""
    while True:
        remaining = None if max_companies is None else max_companies - walk.selected
        if remaining is not None and remaining <= 0:
            walk.stopped_at_cap = True
            return
        page_size = company_batch_size if remaining is None else min(company_batch_size, remaining)
        with clickhouse.get_connection() as client:
            page = [str(row[0]) for row in client.execute(
                scope_sql, {**scope_params, "after_company_id": after, "page_size": page_size})]
        if not page:
            return
        after = page[-1]
        walk.selected += len(page)
        yield page
        if len(page) < page_size:
            return  # a short page means the scan is exhausted


@dataclass(frozen=True)
class CandidateExtractor:
    source: str
    extractor_version: str
    source_tables: tuple[str, ...]
    build_scope_sql: Callable[[], str]
    build_candidates_sql: Callable[[], str]


def materialize_candidates(
    *, clickhouse: ClickhouseResource, extractor: CandidateExtractor, config: CandidateExtractConfig,
    source_run_id: str, extracted_at: datetime, log: Callable[..., object] | None = None,
) -> dict[str, object]:
    """Scan (or take the explicit scope), extract page by page, publish when ``execute``."""
    scope = normalized_se_company_ids(config.company_ids)
    assert_clickhouse_tables_exist(clickhouse, database=DATABASE, tables=(*extractor.source_tables, CANDIDATE_TABLE))
    since = (config.since or "").strip() or EPOCH
    candidates_sql = extractor.build_candidates_sql()
    metrics: dict[str, int] = defaultdict(int)
    metrics["selected_company_count"] = 0
    metrics["candidate_row_count"] = 0
    metrics["inserted_count"] = 0
    per_field: dict[str, int] = defaultdict(int)
    walk = PageWalk()
    pages = iter_company_pages(
        clickhouse, walk=walk, scope=scope, scope_sql=extractor.build_scope_sql(), scope_params={"since": since},
        max_companies=config.max_companies, company_batch_size=config.company_batch_size)
    for page in pages:
        rows: list[CandidateRow] = []
        for start in range(0, len(page), IDS_PER_STATEMENT):
            with clickhouse.get_connection() as client:
                result = client.execute(candidates_sql, {"company_ids": tuple(page[start:start + IDS_PER_STATEMENT])})
            rows.extend(candidate_rows_from_result(result, source=extractor.source, extractor_version=extractor.extractor_version))
        metrics["selected_company_count"] += len(page)
        metrics["candidate_row_count"] += len(rows)
        for row in rows:
            per_field[row.field] += 1
        if config.execute and rows:
            metrics["inserted_count"] += publish_candidates(clickhouse, rows, source_run_id=source_run_id, extracted_at=extracted_at)
        if log is not None:
            log("se_company_field_candidates_%s page: companies=%s rows=%s inserted=%s",
                extractor.source, len(page), len(rows), metrics["inserted_count"])
    if walk.stopped_at_cap and log is not None:
        log("se_company_field_candidates_%s stopped at the max_companies cap (%s); the scan's per-company "
            "watermark leaves the remainder selected for the next run", extractor.source, config.max_companies)
    return {
        **metrics, "rows_per_field": dict(sorted(per_field.items())), "preview": not config.execute,
        "stopped_at_cap": walk.stopped_at_cap, "since": since, "source": extractor.source,
        "extractor_version": extractor.extractor_version, "source_run_id": source_run_id, "company_scope": list(scope),
    }


def define_candidate_asset(
    extractor: CandidateExtractor, *, deps: Sequence[str], description: str
) -> dg.AssetsDefinition:
    """One non-partitioned asset per source, all in group se_company_fields, all writing the
    same table -- the ``source`` metadata key is what tells them apart in the UI."""
    table = f"{DATABASE}.{CANDIDATE_TABLE}"

    @dg.asset(
        name=f"se_company_field_candidates_{extractor.source}",
        deps=[dg.AssetKey(dep) for dep in deps],
        group_name=GROUP_NAME,
        kinds={"clickhouse", "python"},
        metadata={"table": table, "source": extractor.source},
        description=description,
    )
    def _candidates(
        context: dg.AssetExecutionContext, config: CandidateExtractConfig, clickhouse: ClickhouseResource
    ) -> dg.MaterializeResult:
        metadata = materialize_candidates(
            clickhouse=clickhouse, extractor=extractor, config=config, source_run_id=context.run_id,
            extracted_at=datetime.now(UTC), log=context.log.info)
        return dg.MaterializeResult(metadata={**metadata, "table": table})

    return _candidates
