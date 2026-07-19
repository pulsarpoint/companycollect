"""Extracts Swedish company audit firm + audit-opinion form from XBRL facts.

Swedish annual reports carry two audit-related iXBRL fact families:

* ``ValtRevisionsbolagNamn`` / ``ValtRevisionsbolagsnamn`` -- the chosen
  audit firm's name (the taxonomy carries BOTH a capital-N ``Namn`` and a
  lowercase-n ``namn`` spelling across filing years/taxonomy revisions; a
  correct extraction must match either). Verified live against
  ``corpscout.se_financial_facts`` (2026-07-19): 12,697 statements carry
  facts under BOTH spellings, and 608 of those carry a DIFFERENT
  ``audit_firm`` value across the two spellings (e.g. "Ernst & Young AB"
  under ``ValtRevisionsbolagNamn`` vs "Ernst & Young Aktiebolag" under
  ``ValtRevisionsbolagsnamn`` for the SAME statement) -- a plain ``anyIf``
  over both concepts has no deterministic winner for those 608 groups,
  relying on undocumented ClickHouse row-scan order. ``audit_firm`` is
  therefore resolved with ``argMaxIf`` and an explicit tuple comparator key,
  ``(concept_local_name = 'ValtRevisionsbolagsnamn', fact_ordinal)`` -- the
  same boolean-guard-plus-ordinal tiebreak pattern as
  ``company_people/tables.py``'s ``_SE_XBRL_SIGNATURES_SELECT``
  (``argMax(x, (role_kind != 'unknown', statement_key))``): deterministic
  preference is ``ValtRevisionsbolagsnamn`` (the newer-taxonomy spelling)
  first, then the highest ``fact_ordinal`` (the latest fact in document
  order) breaks ties within one spelling.
* ``RevisorspateckningRevisionsberattelseEnligtStandardutformning`` /
  ``RevisorspateckningRevisionsberattelseAvvikerStandardutformning`` --
  boolean-ish "signed" facts marking whether the auditor's report follows
  the standard wording (``Enligt`` -- "according to") or deviates from it
  (``Avviker`` -- "deviates"). Real filings emit at most one of the two per
  statement, but this extraction does not trust that invariant: the
  ``opinion_kind`` ``multiIf`` checks ``Avviker`` FIRST, so if a statement
  somehow carried both facts (a malformed/edge-case filing), a deviating
  opinion always wins over a standard one -- 'modified' is the more
  significant fact for a consumer (a downstream risk/quality signal), so a
  spurious co-occurring 'standard' fact must never mask it. Only when
  NEITHER fact is present (no pateckning at all, e.g. a firm named but the
  auditor's own sign-off wasn't parsed/published) does the row fall back to
  ``'unknown'``.

This module extracts one row per (company, fiscal_year, statement_key): the
audit firm's name (``argMaxIf`` over the two firm-name concepts, tiebroken
deterministically -- see the dual-spelling bullet above -- coalesced to
``''`` when absent), the opinion classification (``'modified'`` |
``'standard'`` | ``'unknown'``), and the opinion date. ``opinion_date`` is a
``coalesce`` of two sources: (a) ``maxIf(date_value, ...)`` over whichever of
the two pateckning facts is a ``date``-kind fact (at most one is expected, so
this ``maxIf`` is just "the one date if any, else NULL", not a real
aggregation choice), and (b) a best-effort recovery for ``text``-kind
pateckning facts -- see "Text-typed opinion date recovery" below.

Text-typed opinion date recovery: the two pateckning concepts are usually
``date``-kind (``date_value`` populated), but ``value_kind`` is sometimes
``text`` instead, with the date only present as Swedish prose in
``text_value``/``raw_value`` (e.g. ``"15 maj 2024"``) -- ``date_value`` is
NULL for those rows, so a plain ``maxIf(date_value, ...)`` silently drops
them. Verified live against ``corpscout.se_financial_facts`` (2026-07-19):
50,412 of the 305,560 total pateckning facts (255,148 ``date``-kind +
50,412 ``text``-kind) are ``text``-kind -- roughly 50k of ~305k resolved
rows losing their opinion date entirely under the old ``maxIf``-only logic.
``opinion_date``'s second ``coalesce`` branch recovers these:
``extractGroups(lowerUTF8(v), '(\\d{1,2})\\s+(<swedish months>)\\s+(\\d{4})')``
(``v`` = ``trim(coalesce(text_value, raw_value))``) captures the
day/month-name/year, a ``multiIf`` maps the (already-lowercased) Swedish
month name to its two-digit number, and
``toDate32OrNull(concat(year, '-', month, '-', leftPad(day, 2, '0')))``
builds the date. ``toDate32OrNull`` (not ``makeDate32``) is used
deliberately: a non-matching/garbled capture makes every ``extractGroups``
group default to ``''`` (a failed match returns an empty array, and
indexing past its end returns the empty-string default rather than
raising), which yields a nonsense ``'--00-00'``-shaped date string --
``toDate32OrNull`` degrades that to NULL rather than raising, whereas
``makeDate32`` with an invalid month/day argument is not guaranteed to.
Verified live against all 50,412 real ``text``-kind pateckning facts
(2026-07-19): 100% parse successfully (e.g. "15 maj 2024" ->
``2024-05-15``), confirming the pattern and month mapping are correct
against real data, not just synthetic examples. ClickHouse string literals
need a backslash escaped to itself to reliably yield one literal backslash
for the regex engine, so every backslash in the pattern above is doubled
(``\\d``, ``\\s``) -- verified live to parse correctly.

Mirrors ``officers.py``'s module shape (qualified table constants, a
columns-tuple contract pinned to the migration file, a ``build_..._sql``
function, a ``replace_..._clickhouse`` stage-then-``EXCHANGE TABLES``
rebuild, the same ``source_run_id``/``resolved_at``/``allow_shrink``
calling convention and shared shrink guard) -- this table is a from-scratch
CH-to-CH rebuild (no DuckDB stage), exactly like ``officers.py``.

Null fiscal year handling (rows are KEPT, not filtered): ``fiscal_year`` is
``toInt32(coalesce(toYear(report_period_end), 0))`` -- a statement with no
resolved ``report_period_end`` gets ``fiscal_year = 0`` and its audit row is
still inserted, for the same reason ``officers.py`` keeps its
placeholder-year rows: audit-firm/opinion provenance is a real filing fact
regardless of whether the period end resolved. Downstream consumers that
need a real fiscal year filter ``fiscal_year > 0`` themselves; the
``null_fiscal_year_count`` quality-metadata column surfaces how many rows
carry the placeholder ``0``. Similarly, ``null_opinion_date_count`` (rows
with a resolved ``opinion_kind != 'unknown'`` but a NULL ``opinion_date`` --
i.e. neither the ``date``-kind ``maxIf`` nor the ``text``-kind recovery
above resolved a date) surfaces how many rows still lose their opinion date
despite a known opinion kind, so a materialization can be monitored for an
unexpected spike.

Row-keep rule: the ``HAVING audit_firm != '' OR opinion_kind != 'unknown'``
clause references ``audit_firm`` and ``opinion_kind``, both aggregate
aliases defined earlier in the SAME select list -- the same alias-scope
pattern ``officers.py``'s module docstring flags for ``role_kind``
referencing ``role_original``, verified there to resolve cleanly in
ClickHouse by substitution. A (company, fiscal_year, statement_key) group is
kept if it has a known audit firm OR a resolved opinion kind; a group with
neither (some other fact briefly touched this GROUP BY key with neither firm
nor opinion resolved) is dropped rather than inserted as an all-empty row.
Verified live via a read-only smoke SELECT against
``corpscout.se_financial_facts`` (2026-07-19): the HAVING-references-SELECT-
alias pattern resolves with no error, and the opinion_kind distribution over
real data (~304k 'standard', ~91k 'unknown', 743 'modified') confirms the
Avviker branch is genuinely exercised, not just a theoretical fallback.

Percent-sign contract: this INSERT is executed via
``client.execute(sql, {...})`` in ``replace_se_company_audits_clickhouse``
(a params dict IS passed, to carry ``resolved_at``), so clickhouse_driver
Python-%-formats the whole query text against that dict before sending it --
the same discipline ``officers.py`` documents. Unlike ``officers.py``
(``LIKE``/``ILIKE`` patterns needing ``%%`` doubling) this SQL contains NO
``LIKE``/``ILIKE`` literal at all, so there is nothing to double: the only
``%`` in the whole query is the single driver placeholder
``%(resolved_at)s``, and a regression test locks in that no bare/undoubled
``%`` sneaks in some other way. The text-date recovery regex added for
Finding 2 was checked against this same discipline: ``\d``, ``\s`` and the
digit-count quantifiers it uses contain no ``%`` character at all, so it
introduces nothing new to double.
"""

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)

SWEDEN_FINANCIAL_DATABASE = "corpscout"
SE_FINANCIAL_FACTS_TABLE = "se_financial_facts"
SE_COMPANY_AUDITS_TABLE = "se_company_audits"
QUALIFIED_SE_COMPANY_AUDITS_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_COMPANY_AUDITS_TABLE}"
)

# Schema order -- MUST match migration 000146's column order exactly (a
# contract test greps the migration file for each of these).
SE_COMPANY_AUDITS_COLUMNS = (
    "company_id",
    "fiscal_year",
    "statement_key",
    "audit_firm",
    "opinion_kind",
    "opinion_date",
    "resolved_at",
)

# The two spellings the taxonomy uses for the chosen-audit-firm-name concept
# -- see the module docstring. Kept as a named tuple so the SQL builder and
# its tests share a single source of truth.
_AUDIT_FIRM_CONCEPTS = (
    "ValtRevisionsbolagNamn",
    "ValtRevisionsbolagsnamn",
)

# The newer-taxonomy spelling -- preferred by the audit_firm argMaxIf
# tiebreak below whenever a statement carries a DIFFERENT value under both
# spellings (608 real statements, verified live; see the module docstring's
# dual-spelling bullet).
_AUDIT_FIRM_PREFERRED_CONCEPT = _AUDIT_FIRM_CONCEPTS[1]

# The two mutually-exclusive (in well-formed filings) audit-opinion-form
# concepts -- see the module docstring's "Avviker-wins" reasoning for why
# the modified concept is checked FIRST in the opinion_kind multiIf below.
_OPINION_STANDARD_CONCEPT = (
    "RevisorspateckningRevisionsberattelseEnligtStandardutformning"
)
_OPINION_MODIFIED_CONCEPT = (
    "RevisorspateckningRevisionsberattelseAvvikerStandardutformning"
)
_OPINION_CONCEPTS = (_OPINION_STANDARD_CONCEPT, _OPINION_MODIFIED_CONCEPT)

_ALL_AUDIT_CONCEPTS = _AUDIT_FIRM_CONCEPTS + _OPINION_CONCEPTS

# Swedish month names in calendar order -- index+1 is the month number.
# Shared by the text-date regex below (the alternation) and the
# month-name-to-number multiIf built from this same tuple in
# build_audits_insert_sql, so the two can never drift apart.
_SWEDISH_MONTH_NAMES = (
    "januari",
    "februari",
    "mars",
    "april",
    "maj",
    "juni",
    "juli",
    "augusti",
    "september",
    "oktober",
    "november",
    "december",
)

# Matches Swedish prose opinion dates like "15 maj 2024" -- day (1-2
# digits), a lowercase Swedish month name, and a 4-digit year -- applied to
# lowerUTF8(...) of the fact's text. See the module docstring's "Text-typed
# opinion date recovery" section for why every backslash here is doubled
# (ClickHouse string literals need a backslash escaped to itself to
# reliably yield one literal backslash for the regex engine; verified
# live).
_OPINION_DATE_TEXT_PATTERN = (
    r"(\\d{1,2})\\s+(" + "|".join(_SWEDISH_MONTH_NAMES) + r")\\s+(\\d{4})"
)

_QUALITY_COLUMNS = (
    "row_count",
    "company_count",
    "modified_opinion_count",
    "unknown_opinion_count",
    "null_fiscal_year_count",
    "null_opinion_date_count",
)


def build_audits_insert_sql(qualified_stage_table: str) -> str:
    """Return the full ``INSERT INTO <stage> (<columns>) ...`` audits SQL.

    The caller (``replace_se_company_audits_clickhouse``, below) creates
    ``qualified_stage_table`` as ``CREATE TABLE ... AS
    corpscout.se_company_audits`` first, executes this INSERT with a params
    dict carrying ``resolved_at``, validates row counts, then ``EXCHANGE
    TABLES`` with the live table -- the same stage-then-swap pattern as
    ``build_officers_insert_sql``.
    """
    columns = ",\n    ".join(SE_COMPANY_AUDITS_COLUMNS)
    audit_firm_concepts = ",\n              ".join(
        f"'{concept}'" for concept in _AUDIT_FIRM_CONCEPTS
    )
    opinion_concepts = ",\n              ".join(
        f"'{concept}'" for concept in _OPINION_CONCEPTS
    )
    all_concepts = ",\n    ".join(f"'{concept}'" for concept in _ALL_AUDIT_CONCEPTS)
    # The text-date extraction is referenced multiple times below (once per
    # captured group); kept as a single Python-level expression string so
    # the regex literal itself (built from _OPINION_DATE_TEXT_PATTERN) is
    # not duplicated in this source file even though it appears repeated in
    # the generated SQL text.
    text_date_groups = (
        "extractGroups(lowerUTF8(trim(coalesce(text_value, raw_value))), "
        f"'{_OPINION_DATE_TEXT_PATTERN}')"
    )
    month_number_multiif = ",\n                        ".join(
        f"{text_date_groups}[2] = '{month}', '{month_number:02d}'"
        for month_number, month in enumerate(_SWEDISH_MONTH_NAMES, start=1)
    )
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
SELECT
    company_id,
    toInt32(coalesce(toYear(report_period_end), 0)) AS fiscal_year,
    statement_key,
    coalesce(
        argMaxIf(trim(coalesce(text_value, raw_value)),
                 (concept_local_name = '{_AUDIT_FIRM_PREFERRED_CONCEPT}', fact_ordinal),
                 concept_local_name IN ({audit_firm_concepts})),
        ''
    ) AS audit_firm,
    multiIf(
        countIf(concept_local_name = '{_OPINION_MODIFIED_CONCEPT}') > 0, 'modified',
        countIf(concept_local_name = '{_OPINION_STANDARD_CONCEPT}') > 0, 'standard',
        'unknown'
    ) AS opinion_kind,
    coalesce(
        maxIf(date_value,
              concept_local_name IN ({opinion_concepts})),
        maxIf(
            toDate32OrNull(
                concat(
                    {text_date_groups}[3],
                    '-',
                    multiIf(
                        {month_number_multiif},
                        '00'
                    ),
                    '-',
                    leftPad({text_date_groups}[1], 2, '0')
                )
            ),
            concept_local_name IN ({opinion_concepts})
        )
    ) AS opinion_date,
    %(resolved_at)s AS resolved_at
FROM corpscout.se_financial_facts
WHERE concept_local_name IN (
    {all_concepts}
)
GROUP BY company_id, fiscal_year, statement_key
HAVING audit_firm != '' OR opinion_kind != 'unknown'"""


def _audits_quality_sql(qualified_stage_table: str) -> str:
    """Cheap post-INSERT quality SELECT against the stage table itself
    (mirrors ``officers.py``'s ``_officers_quality_sql``): total rows,
    distinct companies, the count of rows resolved to a 'modified' opinion,
    the count that fell back to 'unknown' (no pateckning fact matched), the
    count carrying the placeholder ``fiscal_year = 0`` (see the module
    docstring's "Null fiscal year handling" section), and the count of rows
    with a resolved opinion kind but still no opinion date (see "Text-typed
    opinion date recovery").
    """
    return f"""SELECT
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    countIf(opinion_kind = 'modified') AS modified_opinion_count,
    countIf(opinion_kind = 'unknown') AS unknown_opinion_count,
    countIf(fiscal_year = 0) AS null_fiscal_year_count,
    countIf(opinion_kind != 'unknown' AND opinion_date IS NULL) AS null_opinion_date_count
FROM {qualified_stage_table}"""


def _quality_metadata(row: tuple[Any, ...]) -> dict[str, int]:
    return {
        column: int(value)
        for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
    }


def _validate_quality(quality: dict[str, int]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "Sweden company audits extraction produced no rows; refusing "
            f"to replace {QUALIFIED_SE_COMPANY_AUDITS_TABLE}"
        )


def replace_se_company_audits_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> dict[str, int | str | None]:
    """Atomically rebuild ``se_company_audits`` in ClickHouse.

    Mirrors ``replace_se_company_officers_clickhouse``'s stage + ``EXCHANGE
    TABLES`` pattern: a uuid-suffixed stage table is created as a
    schema-only copy of the live target, the built audit rows are INSERTed
    into it, a non-empty guard runs (refusing to ever replace a populated
    table with an empty one), the shared shrink guard
    (``guard_against_clickhouse_table_shrink``) refuses a replace that would
    leave the table with less than half its current row count (unless
    ``allow_shrink=True``), then ``EXCHANGE TABLES`` swaps the stage and
    target atomically. The stage table is dropped in a ``finally`` block so
    a mid-run failure never leaves an orphaned stage table behind.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_COMPANY_AUDITS_TABLE, SE_FINANCIAL_FACTS_TABLE),
    )
    stage_table = f"_tmp_{SE_COMPANY_AUDITS_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_COMPANY_AUDITS_TABLE}`"
    )
    if log is not None:
        log(
            "Building Sweden company audits in ClickHouse: target=%s source_run_id=%s",
            QUALIFIED_SE_COMPANY_AUDITS_TABLE,
            source_run_id,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(
                build_audits_insert_sql(qualified_stage_table),
                {"resolved_at": resolved_at, "source_run_id": source_run_id},
            )
            quality_row = client.execute(
                _audits_quality_sql(qualified_stage_table)
            )[0]
            quality: dict[str, int | str | None] = dict(
                _quality_metadata(quality_row)
            )
            _validate_quality(quality)
            existing_row_count = clickhouse_table_row_count(
                client, qualified_target_table
            )
            guard_against_clickhouse_table_shrink(
                qualified_table=QUALIFIED_SE_COMPANY_AUDITS_TABLE,
                existing_row_count=existing_row_count,
                staged_row_count=int(quality["row_count"]),
                allow_shrink=allow_shrink,
            )
            client.execute(
                f"EXCHANGE TABLES {qualified_stage_table} AND {qualified_target_table}"
            )
        except Exception as exc:
            primary_error = exc
            raise
        finally:
            try:
                client.execute(f"DROP TABLE IF EXISTS {qualified_stage_table}")
            except Exception:
                if primary_error is None:
                    raise

    quality["table"] = QUALIFIED_SE_COMPANY_AUDITS_TABLE
    quality["source_run_id"] = source_run_id
    if log is not None:
        log(
            "Finished Sweden company audits: rows=%s companies=%s "
            "modified_opinion=%s unknown_opinion=%s null_fiscal_year=%s "
            "null_opinion_date=%s",
            quality["row_count"],
            quality["company_count"],
            quality["modified_opinion_count"],
            quality["unknown_opinion_count"],
            quality["null_fiscal_year_count"],
            quality["null_opinion_date_count"],
        )
    return quality
