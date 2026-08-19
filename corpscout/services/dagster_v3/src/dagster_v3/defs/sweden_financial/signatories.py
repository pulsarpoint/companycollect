"""Extracts Swedish financial-report signatories from XBRL facts.

Swedish annual reports embed a mandatory signature block -- board members,
CEO, and (separately) the auditor -- as inline-XBRL facts under a handful of
``Underskrift*`` concepts, one triplet (first name / last name / role) per
person per signatory kind:

* ``UnderskriftHandling*`` -- the filing's own signature page.
* ``UnderskriftArsredovisningForetradare*`` -- the annual-report
  representative signatures.
* ``UnderskriftFaststallelseintygForetradare*`` -- the certification
  ("faststallelseintyg") signatures.
* ``UnderskriftRevisionsberattelseRevisor*`` -- the auditor's own signature
  on the audit report (``Titel`` stands in for ``Roll`` here).

This module extracts those facts into one row per (statement, signatory
kind, person): ``sig`` tags each fact with its signatory kind and which of
the three fields (first name / last name / role) it carries, ``grouped``
assigns a stable ``person_seq`` per person via a running sum of
``is_first_name`` over ``fact_ordinal`` (facts for one person are emitted
contiguously by the parser, first name first), and the final ``GROUP BY``
collapses each (statement, signatory kind, person_seq) triplet into one row
via ``anyIf``. ``role_kind`` normalizes the free-text ``role_original``
(e.g. "Styrelseledamot", "Verkstallande direktor") into a small fixed
vocabulary for downstream querying; empty/unmapped roles fall back to
``'unknown'``/``'other'`` rather than being dropped -- real filings
sometimes omit the role facts entirely (row 14 of the Axfood filing sample
during design was one such case) while still carrying a name.

Mirrors ``history.py``'s module shape (qualified table constants, a
columns-tuple contract pinned to the migration file, a ``build_..._sql``
function, a ``replace_..._clickhouse`` stage-then-``EXCHANGE TABLES``
rebuild) and ``metrics.py``'s calling convention/guard wiring
(``source_run_id``/``resolved_at``/``allow_shrink`` parameters, the shared
``guard_against_clickhouse_table_shrink`` shrink guard from
``clickhouse.py``) -- this table is a from-scratch CH-to-CH rebuild (no
DuckDB stage) exactly like both siblings.

Alias-scope note: ``role_kind``'s ``multiIf`` references ``role_original``,
itself an aggregate alias (``coalesce(anyIf(v, is_role = 1), '')``) defined
earlier in the SAME select list. This was flagged as a live risk during
design (ClickHouse might reject referencing an aggregate alias from another
expression in the same SELECT). Verified live via a read-only smoke SELECT
against ``corpscout.se_financial_facts`` (2026-07-19): ClickHouse resolves
the alias by substitution with no error, so no outer-SELECT wrapper is
needed here.

Percent-sign contract: this INSERT is executed via
``client.execute(sql, {...})`` in
``replace_se_financial_report_signatories_clickhouse``
(a params dict IS passed, to carry ``resolved_at``), so clickhouse_driver
Python-%-formats the whole query text against that dict before sending it.
Every literal ``%`` in the ``LIKE``/``ILIKE`` patterns below is therefore
doubled to ``%%`` -- the same reason ``metrics.py``'s LIKE patterns are
doubled while ``history.py``'s (executed with NO params dict) are not.

Grouping-window safety (why sharing one partition is safe): ``UnderskriftHandling*``
and ``UnderskriftArsredovisningForetradare*`` BOTH classify to
``signatory_kind = 'board_signature'`` (see the ``multiIf`` in
``build_signatories_insert_sql``), so they share a single window partition --
``PARTITION BY statement_key, signatory_kind ORDER BY fact_ordinal`` folds
both families' facts into one running-sum sequence per statement.
``fact_ordinal`` is genuine document order (``parsing.py`` enumerates facts
via ``enumerate(facts, start=1)`` over the parsed DOM, not a synthetic
per-concept counter), so the running sum over ``is_first_name`` only
misattributes a last-name/role fact to the wrong person if the two families'
fact triples ever INTERLEAVED within a single person's own triple (e.g. an
``UnderskriftHandlingTilltalsnamn`` immediately followed by an
``UnderskriftArsredovisningForetradareEfternamn`` before that same person's
own ``UnderskriftHandlingEfternamn``). In the normal case each family
occupies its own contiguous block in the source document -- iXBRL renders
one signature block at a time as a contiguous run of facts, never splicing
two blocks fact-by-fact -- so the running sum transitions cleanly from one
family's last person to the next family's first person at the block
boundary. A person who signs in BOTH blocks (e.g. a board member who is also
the annual-report representative) therefore yields TWO separate
``person_seq`` groups, one per family: this is an ACCEPTED duplicate (two
rows for one physical person), not a correctness bug -- a consumer that
wants a deduplicated person list groups on (first_name, last_name) itself.
Cross-family interleaving mid-triple is considered structurally impossible
given how iXBRL renders these blocks and is NOT something this SQL defends
against; the running sum has no way to tell which family a row belongs to
within the shared partition, so if interleaving were ever observed it would
misattribute a name/role from one family onto a person from the other. See
``test_person_grouping_simulation_separates_board_signature_blocks_cleanly``
in ``tests/test_sweden_financial_report_signatories.py`` for a pure-Python
simulation
of this exact running-sum + collapse semantics against a fabricated
cross-family (but block-separated) fact sequence.

Null fiscal year handling (rows are KEPT, not filtered): ``fiscal_year`` is
``toInt32(coalesce(toYear(report_period_end), 0))`` -- a statement with no
resolved ``report_period_end`` gets ``fiscal_year = 0`` and its signatory rows
are still inserted. This is a deliberate divergence from ``history.py``,
which filters ``reports.fiscal_year IS NOT NULL`` in its ``eligible_reports``
CTE: history rows are inherently year-keyed (a comparative-year figure with
no year is meaningless), but a signature is provenance about a REAL person
regardless of whether the filing's period end resolved -- dropping the row
would silently discard a name/role that a consumer might still want (e.g.
"who has ever signed for this company", independent of year). Downstream
consumers that need a real fiscal year filter ``fiscal_year > 0`` themselves;
the ``null_fiscal_year_count`` quality-metadata column (see
``_signatories_quality_sql``) surfaces how many rows carry the placeholder
``0`` so a materialization can be monitored for an unexpected spike.
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
SE_FINANCIAL_REPORT_SIGNATORIES_TABLE = "se_financial_report_signatories"
QUALIFIED_SE_FINANCIAL_REPORT_SIGNATORIES_TABLE = (
    f"{SWEDEN_FINANCIAL_DATABASE}.{SE_FINANCIAL_REPORT_SIGNATORIES_TABLE}"
)

# Schema order -- MUST match migration 000143's column order exactly (a
# contract test greps the migration file for each of these).
SE_FINANCIAL_REPORT_SIGNATORIES_COLUMNS = (
    "company_id",
    "fiscal_year",
    "statement_key",
    "signatory_kind",
    "person_seq",
    "first_name",
    "last_name",
    "role_original",
    "role_kind",
    "resolved_at",
)

# The four concept-triplets (first name / last name / role) that make up a
# signature-block person -- see the module docstring for what each family
# represents. Kept as named tuples so the SQL builder and its tests share a
# single source of truth for the concept names.
_HANDLING_CONCEPTS = (
    "UnderskriftHandlingTilltalsnamn",
    "UnderskriftHandlingEfternamn",
    "UnderskriftHandlingRoll",
)
_ARSREDOVISNING_FORETRADARE_CONCEPTS = (
    "UnderskriftArsredovisningForetradareTilltalsnamn",
    "UnderskriftArsredovisningForetradareEfternamn",
    "UnderskriftArsredovisningForetradareForetradarroll",
)
_FASTSTALLELSEINTYG_FORETRADARE_CONCEPTS = (
    "UnderskriftFaststallelseintygForetradareTilltalsnamn",
    "UnderskriftFaststallelseintygForetradareEfternamn",
    "UnderskriftFaststallelseintygForetradareForetradarroll",
)
_REVISIONSBERATTELSE_REVISOR_CONCEPTS = (
    "UnderskriftRevisionsberattelseRevisorTilltalsnamn",
    "UnderskriftRevisionsberattelseRevisorEfternamn",
    "UnderskriftRevisionsberattelseRevisorTitel",
)

_SIGNATURE_CONCEPT_TRIPLES = (
    _HANDLING_CONCEPTS,
    _ARSREDOVISNING_FORETRADARE_CONCEPTS,
    _FASTSTALLELSEINTYG_FORETRADARE_CONCEPTS,
    _REVISIONSBERATTELSE_REVISOR_CONCEPTS,
)

_QUALITY_COLUMNS = (
    "row_count",
    "company_count",
    "statement_count",
    "board_signature_count",
    "certification_count",
    "auditor_signatory_count",
    "unknown_role_count",
    "null_fiscal_year_count",
)


def build_signatories_insert_sql(qualified_stage_table: str) -> str:
    """Return the full ``INSERT INTO <stage> (<columns>) ...`` signatory SQL.

    The caller (``replace_se_financial_report_signatories_clickhouse``, below) creates
    ``qualified_stage_table`` as ``CREATE TABLE ... AS
    corpscout.se_financial_report_signatories`` first, executes this INSERT with a
    params dict carrying ``resolved_at``, validates row counts, then
    ``EXCHANGE TABLES`` with the live table -- the same stage-then-swap
    pattern as ``build_history_insert_sql``/
    ``build_se_bolagsverket_financial_metrics_insert_sql``.
    """
    columns = ",\n    ".join(SE_FINANCIAL_REPORT_SIGNATORIES_COLUMNS)
    first_name_concepts = ",\n            ".join(
        f"'{triple[0]}'" for triple in _SIGNATURE_CONCEPT_TRIPLES
    )
    last_name_concepts = ",\n            ".join(
        f"'{triple[1]}'" for triple in _SIGNATURE_CONCEPT_TRIPLES
    )
    role_concepts = ",\n            ".join(
        f"'{triple[2]}'" for triple in _SIGNATURE_CONCEPT_TRIPLES
    )
    all_concepts = ",".join(
        f"'{concept}'" for triple in _SIGNATURE_CONCEPT_TRIPLES for concept in triple
    )
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
WITH sig AS (
    SELECT
        statement_key,
        company_id,
        toInt32(coalesce(toYear(report_period_end), 0)) AS fiscal_year,
        fact_ordinal,
        concept_local_name,
        trim(coalesce(text_value, raw_value)) AS v,
        multiIf(
            concept_local_name LIKE 'UnderskriftRevisionsberattelseRevisor%%', 'auditor',
            concept_local_name LIKE 'UnderskriftFaststallelseintygForetradare%%', 'certification',
            'board_signature'
        ) AS signatory_kind,
        concept_local_name IN (
            {first_name_concepts}
        ) AS is_first_name,
        concept_local_name IN (
            {last_name_concepts}
        ) AS is_last_name,
        concept_local_name IN (
            {role_concepts}
        ) AS is_role
    FROM corpscout.se_financial_facts
    WHERE concept_local_name IN ({all_concepts})
),
grouped AS (
    SELECT *,
        sum(is_first_name) OVER (
            PARTITION BY statement_key, signatory_kind
            ORDER BY fact_ordinal
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS person_seq
    FROM sig
)
SELECT
    company_id,
    fiscal_year,
    statement_key,
    signatory_kind,
    toUInt16(person_seq) AS person_seq,
    anyIf(v, is_first_name = 1) AS first_name,
    anyIf(v, is_last_name = 1) AS last_name,
    coalesce(anyIf(v, is_role = 1), '') AS role_original,
    multiIf(
        role_original ILIKE '%%ordförande%%', 'chairman',
        role_original ILIKE '%%verkställande direktör%%' OR role_original ILIKE 'VD%%', 'ceo',
        role_original ILIKE '%%suppleant%%', 'deputy_board_member',
        role_original ILIKE '%%styrelseledamot%%' OR role_original ILIKE '%%ledamot%%', 'board_member',
        role_original ILIKE '%%likvidator%%', 'liquidator',
        role_original ILIKE '%%revisor%%', 'auditor',
        signatory_kind = 'auditor', 'auditor',
        role_original = '', 'unknown',
        'other'
    ) AS role_kind,
    %(resolved_at)s AS resolved_at
FROM grouped
WHERE person_seq > 0
GROUP BY company_id, fiscal_year, statement_key, signatory_kind, person_seq
HAVING first_name != '' OR last_name != ''"""


def _signatories_quality_sql(qualified_stage_table: str) -> str:
    """Cheap post-INSERT quality SELECT against the stage table itself
    (mirrors ``history.py``'s ``build_history_quality_sql``): total rows,
    distinct companies/statements, a per-signatory-kind breakdown, the count
    of rows that fell back to the ``'unknown'`` role, and the count of rows
    carrying the placeholder ``fiscal_year = 0`` (see the module docstring's
    "Null fiscal year handling" section for why those rows are kept rather
    than filtered).
    """
    return f"""SELECT
    count() AS row_count,
    uniqExact(company_id) AS company_count,
    uniqExact(statement_key) AS statement_count,
    countIf(signatory_kind = 'board_signature') AS board_signature_count,
    countIf(signatory_kind = 'certification') AS certification_count,
    countIf(signatory_kind = 'auditor') AS auditor_signatory_count,
    countIf(role_kind = 'unknown') AS unknown_role_count,
    countIf(fiscal_year = 0) AS null_fiscal_year_count
FROM {qualified_stage_table}"""


def _quality_metadata(row: tuple[Any, ...]) -> dict[str, int]:
    return {
        column: int(value) for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
    }


def _validate_quality(quality: dict[str, int]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "Sweden financial-report signatory extraction produced no rows; "
            "refusing to replace "
            f"{QUALIFIED_SE_FINANCIAL_REPORT_SIGNATORIES_TABLE}"
        )


def replace_se_financial_report_signatories_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> dict[str, int | str | None]:
    """Atomically rebuild ``se_financial_report_signatories`` in ClickHouse.

    Mirrors ``replace_se_bolagsverket_financial_metrics_clickhouse``'s stage +
    ``EXCHANGE TABLES`` pattern: a uuid-suffixed stage table is created as a
    schema-only copy of the live target, the built signatory rows are
    INSERTed into it, a non-empty guard runs (refusing to ever replace a
    populated table with an empty one), the shared shrink guard
    (``guard_against_clickhouse_table_shrink``) refuses a replace that would
    leave the table with less than half its current row count (unless
    ``allow_shrink=True``), then ``EXCHANGE TABLES`` swaps the stage and
    target atomically. The stage table is dropped in a ``finally`` block so
    a mid-run failure never leaves an orphaned stage table behind.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=SWEDEN_FINANCIAL_DATABASE,
        tables=(SE_FINANCIAL_REPORT_SIGNATORIES_TABLE, SE_FINANCIAL_FACTS_TABLE),
    )
    stage_table = f"_tmp_{SE_FINANCIAL_REPORT_SIGNATORIES_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{SWEDEN_FINANCIAL_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{SWEDEN_FINANCIAL_DATABASE}`.`{SE_FINANCIAL_REPORT_SIGNATORIES_TABLE}`"
    )
    if log is not None:
        log(
            "Building Sweden financial-report signatories in ClickHouse: "
            "target=%s source_run_id=%s",
            QUALIFIED_SE_FINANCIAL_REPORT_SIGNATORIES_TABLE,
            source_run_id,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(
                build_signatories_insert_sql(qualified_stage_table),
                {"resolved_at": resolved_at, "source_run_id": source_run_id},
            )
            quality_row = client.execute(
                _signatories_quality_sql(qualified_stage_table)
            )[0]
            quality: dict[str, int | str | None] = dict(_quality_metadata(quality_row))
            _validate_quality(quality)
            existing_row_count = clickhouse_table_row_count(
                client, qualified_target_table
            )
            guard_against_clickhouse_table_shrink(
                qualified_table=QUALIFIED_SE_FINANCIAL_REPORT_SIGNATORIES_TABLE,
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

    quality["table"] = QUALIFIED_SE_FINANCIAL_REPORT_SIGNATORIES_TABLE
    quality["source_run_id"] = source_run_id
    if log is not None:
        log(
            "Finished Sweden financial-report signatories: rows=%s companies=%s "
            "statements=%s "
            "unknown_role=%s null_fiscal_year=%s",
            quality["row_count"],
            quality["company_count"],
            quality["statement_count"],
            quality["unknown_role_count"],
            quality["null_fiscal_year_count"],
        )
    return quality
