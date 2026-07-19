"""company_people_all build asset: per-source SELECT union + stage/EXCHANGE.

Mirrors ``sweden_financial/officers.py``'s (Task 1) stage-then-``EXCHANGE
TABLES`` rebuild -- a uuid-suffixed stage table is created as a schema-only
copy of the live target, every ``PEOPLE_SOURCES`` entry is unioned into one
``INSERT`` against the stage table, a non-empty guard refuses to ever
replace a populated table with an empty one, the shared shrink guard
(``guard_against_clickhouse_table_shrink``, reused from
``sweden_financial.clickhouse`` -- it is a generic row-count guard, not
Sweden-specific logic, and Task 1 is this table's only other caller so far)
refuses a replace that would leave the table with less than half its
current row count (unless ``allow_shrink=True``), then ``EXCHANGE TABLES``
swaps the stage and target atomically. The stage table is dropped in a
``finally`` block so a mid-run failure never leaves an orphaned stage table
behind.

Unlike ``companies_all`` (which INSERTs one leg per country and validates
each leg's row count against its source table's row count 1:1), this table
has no such per-source invariant to check -- a future source could
legitimately contribute fewer people-rows than its source table has company
rows (e.g. a source with sparse role data), so the only correctness gate is
the overall refuse-empty + shrink-guard pair, exactly like Task 1.
"""

import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.company_people.tables import (
    COMPANY_PEOPLE_ALL_COLUMNS,
    COMPANY_PEOPLE_ALL_TABLE,
    PEOPLE_SOURCES,
    QUALIFIED_COMPANY_PEOPLE_ALL_TABLE,
)
from dagster_v3.defs.sweden_financial.clickhouse import (
    clickhouse_table_row_count,
    guard_against_clickhouse_table_shrink,
)

COMPANY_PEOPLE_DATABASE = "corpscout"
GROUP_NAME = "company_people_all"

# Source tables this asset reads from, asserted to exist before the build
# starts (mirrors officers.py's assert_clickhouse_tables_exist call). Kept
# as a tuple rather than derived from PEOPLE_SOURCES because a source's
# underlying table names aren't parseable out of its SELECT text -- add the
# new source's table(s) here alongside its PEOPLE_SOURCES entry. Includes
# both tables the se_xbrl_signatures SELECT reads from -- se_company_officers
# (the FROM) and se_companies (the LEFT JOIN for company_name) -- so a
# missing join table fails this module's own clear pre-flight error instead
# of a raw ClickHouse "table doesn't exist" error surfacing mid-INSERT.
_REQUIRED_SOURCE_TABLES = ("se_company_officers", "se_companies")

_QUALITY_COLUMNS = (
    "row_count",
    "country_count",
    "company_count",
    "source_count",
)


def build_company_people_all_insert_sql(qualified_stage_table: str) -> str:
    """Return the full ``INSERT INTO <stage> (<columns>) ...`` people SQL.

    Concatenates every ``PEOPLE_SOURCES`` SELECT with ``UNION ALL`` -- each
    entry already produces exactly ``COMPANY_PEOPLE_ALL_COLUMNS``, in order,
    under those aliases (see ``tables.py``'s module docstring), so this
    function stays a pure string join with no per-source knowledge.
    """
    columns = ",\n    ".join(COMPANY_PEOPLE_ALL_COLUMNS)
    unioned_select = "\nUNION ALL\n".join(PEOPLE_SOURCES.values())
    return f"""INSERT INTO {qualified_stage_table} (
    {columns}
)
{unioned_select}"""


def _company_people_all_quality_sql(qualified_stage_table: str) -> str:
    """Cheap post-INSERT quality SELECT against the stage table itself
    (mirrors ``officers.py``'s ``_officers_quality_sql``): total rows,
    distinct countries, distinct companies, and distinct sources -- a
    second source appearing (or an existing one silently vanishing) shows
    up in ``source_count`` without needing a dedicated per-source check.
    """
    return f"""SELECT
    count() AS row_count,
    uniqExact(country_iso2) AS country_count,
    uniqExact(company_id) AS company_count,
    uniqExact(source) AS source_count
FROM {qualified_stage_table}"""


def _quality_metadata(row: tuple[Any, ...]) -> dict[str, int]:
    return {
        column: int(value)
        for column, value in zip(_QUALITY_COLUMNS, row, strict=True)
    }


def _validate_quality(quality: dict[str, int]) -> None:
    if quality["row_count"] == 0:
        raise ValueError(
            "company_people_all build produced no rows; refusing to "
            f"replace {QUALIFIED_COMPANY_PEOPLE_ALL_TABLE}"
        )


def replace_company_people_all_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    resolved_at: datetime,
    log: Callable[..., object] | None = None,
    allow_shrink: bool = False,
) -> dict[str, int | str | None]:
    """Atomically rebuild ``company_people_all`` in ClickHouse.

    See the module docstring for the stage + ``EXCHANGE TABLES`` + guards
    shape, identical to ``replace_se_company_officers_clickhouse`` (Task 1).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=COMPANY_PEOPLE_DATABASE,
        tables=(COMPANY_PEOPLE_ALL_TABLE, *_REQUIRED_SOURCE_TABLES),
    )
    stage_table = f"_tmp_{COMPANY_PEOPLE_ALL_TABLE}_{uuid.uuid4().hex}"
    qualified_stage_table = f"`{COMPANY_PEOPLE_DATABASE}`.`{stage_table}`"
    qualified_target_table = (
        f"`{COMPANY_PEOPLE_DATABASE}`.`{COMPANY_PEOPLE_ALL_TABLE}`"
    )
    if log is not None:
        log(
            "Building company_people_all in ClickHouse: target=%s source_run_id=%s",
            QUALIFIED_COMPANY_PEOPLE_ALL_TABLE,
            source_run_id,
        )

    with clickhouse.get_connection() as client:
        client.execute(
            f"CREATE TABLE {qualified_stage_table} AS {qualified_target_table}"
        )
        primary_error: Exception | None = None
        try:
            client.execute(
                build_company_people_all_insert_sql(qualified_stage_table),
                {"resolved_at": resolved_at, "source_run_id": source_run_id},
            )
            quality_row = client.execute(
                _company_people_all_quality_sql(qualified_stage_table)
            )[0]
            quality: dict[str, int | str | None] = dict(
                _quality_metadata(quality_row)
            )
            _validate_quality(quality)
            existing_row_count = clickhouse_table_row_count(
                client, qualified_target_table
            )
            guard_against_clickhouse_table_shrink(
                qualified_table=QUALIFIED_COMPANY_PEOPLE_ALL_TABLE,
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

    quality["table"] = QUALIFIED_COMPANY_PEOPLE_ALL_TABLE
    quality["source_run_id"] = source_run_id
    if log is not None:
        log(
            "Finished company_people_all: rows=%s countries=%s companies=%s sources=%s",
            quality["row_count"],
            quality["country_count"],
            quality["company_count"],
            quality["source_count"],
        )
    return quality


class CompanyPeopleClickhouseExportConfig(dg.Config):
    # Shrink-guard override (see sweden_financial/clickhouse.py's
    # guard_against_clickhouse_table_shrink) -- MUST stay False by default.
    # Only set True via explicit run config for a confirmed-intentional
    # shrink of a populated company_people_all table, never as a standing
    # default.
    allow_shrink: bool = False


@dg.asset(
    name="company_people_all_clickhouse",
    deps=["se_company_officers_clickhouse"],
    group_name=GROUP_NAME,
    kinds={"clickhouse"},
    metadata={"table": QUALIFIED_COMPANY_PEOPLE_ALL_TABLE},
    description=(
        "Cross-country people-search table built from the per-source "
        "PEOPLE_SOURCES registry (currently se_xbrl_signatures only), "
        "deduped to one row per (company, fiscal_year, person, "
        "signatory_kind), into corpscout.company_people_all (stage + "
        "EXCHANGE TABLES)."
    ),
)
def company_people_all_clickhouse(
    context: dg.AssetExecutionContext,
    config: CompanyPeopleClickhouseExportConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    metadata = replace_company_people_all_clickhouse(
        clickhouse=clickhouse,
        source_run_id=context.run_id,
        resolved_at=datetime.now(UTC),
        log=context.log.info,
        allow_shrink=config.allow_shrink,
    )
    return dg.MaterializeResult(metadata=metadata)


defs = dg.Definitions(assets=[company_people_all_clickhouse])
