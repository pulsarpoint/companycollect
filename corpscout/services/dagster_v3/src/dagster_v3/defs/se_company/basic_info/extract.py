"""The change scan, page loop and publish every SQL suggestion extractor shares (spec 3.2).

A source contributes two SQL texts: `current_sql` returns `(company_id, observed_at)` for
its current record per company, `select_sql` returns one wide suggestion row per company
for the ids bound as `%(company_ids)s`, in SUGGESTION_SELECT_COLUMNS order. This module
decides which companies to visit (never suggested by this source, or whose source record is
newer than the current suggestion row), pages them by keyset, counts, and in execute mode
inserts each page straight into the suggestion table with the publisher's stamps.
"""

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.assets import GROUP_NAME
from dagster_v3.defs.se_company.basic_info.batch import ID_BOUND_QUERY_SETTINGS
from dagster_v3.defs.se_company.common import normalized_se_company_ids

SUGGESTION_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id",
    "source",
    "source_record_uid",
    "observed_at",
    *tables.VALUE_COLUMNS,
)
_ISO_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d{1,3})?Z$")


class ExtractConfig(dg.Config):
    # False = preview: scan, count what would be written, write nothing.
    execute: bool = False
    # Explicit scope: skip the change scan and extract exactly these companies.
    company_ids: list[str] = Field(default_factory=list)
    max_companies: int = Field(default=1_000_000, ge=1, le=1_000_000)
    # ISO-8601 UTC ("2026-09-01T00:00:00Z"): visit every company whose source record is
    # newer than this instant instead of comparing with the current suggestion row.
    since: str = ""
    # Ids bound per statement; 5,000 twelve-digit ids render to about 80 KB.
    page_size: int = Field(default=5_000, ge=1, le=20_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))

    @field_validator("since")
    @classmethod
    def _valid_since(cls, value: str) -> str:
        if value and not _ISO_UTC.match(value):
            raise ValueError("since must be an ISO-8601 UTC instant like 2026-09-01T00:00:00Z")
        return value


@dataclass(frozen=True, slots=True)
class ExtractCounts:
    companies: int
    pages: int
    candidates: int
    inserted: int
    execute: bool
    stopped_at_cap: bool

    def as_metadata(self) -> dict[str, int | bool]:
        return {
            "companies": self.companies,
            "pages": self.pages,
            "candidates": self.candidates,
            "inserted": self.inserted,
            "execute": self.execute,
            "stopped_at_cap": self.stopped_at_cap,
        }


_SCOPE_TAIL = "WHERE company_id > %(after_company_id)s\nORDER BY company_id\nLIMIT %(page_size)s"


def changed_scope_sql(*, current_sql: str) -> str:
    """Companies the source has never suggested, plus those whose current source record is
    newer than the current suggestion row. Two branches rather than one LEFT JOIN so the
    text means the same under join_use_nulls 0 and 1."""
    return (
        "SELECT company_id FROM (\n"
        "    SELECT candidate.company_id AS company_id\n"
        f"    FROM ({current_sql}) AS candidate\n"
        "    LEFT ANTI JOIN (\n"
        f"        SELECT company_id FROM {tables.QUALIFIED_SUGGESTION_TABLE} WHERE source = %(source)s\n"
        "    ) AS existing ON existing.company_id = candidate.company_id\n"
        "    UNION ALL\n"
        "    SELECT candidate.company_id AS company_id\n"
        f"    FROM ({current_sql}) AS candidate\n"
        "    INNER JOIN (\n"
        "        SELECT company_id, argMax(observed_at, suggested_at) AS observed_at\n"
        f"        FROM {tables.QUALIFIED_SUGGESTION_TABLE} WHERE source = %(source)s\n"
        "        GROUP BY company_id\n"
        "    ) AS current ON current.company_id = candidate.company_id\n"
        "    WHERE candidate.observed_at > current.observed_at\n"
        ")\n"
        f"{_SCOPE_TAIL}"
    )


def since_scope_sql(*, current_sql: str) -> str:
    return (
        "SELECT company_id FROM (\n"
        "    SELECT candidate.company_id AS company_id\n"
        f"    FROM ({current_sql}) AS candidate\n"
        "    WHERE candidate.observed_at > parseDateTime64BestEffort(%(since)s, 3, 'UTC')\n"
        ")\n"
        f"{_SCOPE_TAIL}"
    )


def count_page_sql(*, select_sql: str) -> str:
    return f"SELECT count() AS candidates FROM ({select_sql}) AS candidate"


def insert_page_sql(*, select_sql: str) -> str:
    selected = ", ".join(f"candidate.{column}" for column in SUGGESTION_SELECT_COLUMNS)
    return (
        f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} ({', '.join(tables.SUGGESTION_INSERT_COLUMNS)})\n"
        f"SELECT {selected}, CAST(NULL AS Nullable(String)) AS decided_by, "
        "CAST(NULL AS Nullable(String)) AS note, now64(3, 'UTC') AS suggested_at, "
        "%(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version\n"
        f"FROM ({select_sql}) AS candidate"
    )


def _scan_pages(client: Any, *, source: str, current_sql: str, config: ExtractConfig, select_params: dict[str, Any]):
    scope_sql = since_scope_sql(current_sql=current_sql) if config.since else changed_scope_sql(current_sql=current_sql)
    after = ""
    while True:
        params = {**select_params, "source": source, "after_company_id": after, "page_size": config.page_size}
        if config.since:
            params["since"] = config.since
        page = [row[0] for row in client.execute(scope_sql, params)]
        if not page:
            return
        yield page
        if len(page) < config.page_size:
            return
        after = page[-1]


def run_extractor(
    client: Any,
    *,
    source: str,
    extractor_version: str,
    current_sql: str,
    select_sql: str,
    select_params: dict[str, Any] | None,
    source_run_id: str,
    config: ExtractConfig,
    log: Callable[..., object] | None = None,
) -> ExtractCounts:
    """Visit the companies in scope page by page; count the rows the source would write
    and, in execute mode, insert them with this run's stamps."""
    extra = dict(select_params or {})
    if config.company_ids:
        pages = (config.company_ids[i : i + config.page_size] for i in range(0, len(config.company_ids), config.page_size))
    else:
        pages = _scan_pages(client, source=source, current_sql=current_sql, config=config, select_params=extra)
    companies = page_count = candidates = inserted = 0
    stopped = False
    for page in pages:
        remaining = config.max_companies - companies
        if remaining <= 0:
            stopped = True
            break
        if len(page) > remaining:
            page = page[:remaining]
            stopped = True
        page_count += 1
        companies += len(page)
        params = {**extra, "company_ids": page, "source_run_id": source_run_id, "extractor_version": extractor_version}
        page_candidates = int(client.execute(count_page_sql(select_sql=select_sql), params, settings=ID_BOUND_QUERY_SETTINGS)[0][0])
        candidates += page_candidates
        if config.execute and page_candidates:
            client.execute(insert_page_sql(select_sql=select_sql), params, settings=ID_BOUND_QUERY_SETTINGS)
            inserted += page_candidates
        if log is not None:
            log("Suggestion page: source=%s companies=%d candidates=%d execute=%s", source, len(page), page_candidates, config.execute)
        if stopped:
            break
    return ExtractCounts(
        companies=companies, pages=page_count, candidates=candidates, inserted=inserted,
        execute=config.execute, stopped_at_cap=stopped,
    )


def define_suggestion_asset(
    *,
    source: str,
    extractor_version: str,
    current_sql: str,
    select_sql: str,
    select_params: dict[str, Any] | None = None,
    deps: Sequence[dg.AssetKey] = (),
    description: str,
) -> dg.AssetsDefinition:
    """One asset per SQL source, all writing the suggestion table; `source` in the metadata
    tells them apart."""

    @dg.asset(
        name=f"se_basic_info_suggestions_{source}",
        group_name=GROUP_NAME,
        deps=list(deps),
        kinds={"clickhouse", "sql"},
        metadata={"table": tables.QUALIFIED_SUGGESTION_TABLE, "source": source},
        description=description,
    )
    def _suggestions(context: dg.AssetExecutionContext, config: ExtractConfig, clickhouse: ClickhouseResource) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.SUGGESTION_TABLE,))
        with clickhouse.get_connection() as client:
            counts = run_extractor(
                client, source=source, extractor_version=extractor_version, current_sql=current_sql,
                select_sql=select_sql, select_params=select_params, source_run_id=context.run_id,
                config=config, log=context.log.info,
            )
        return dg.MaterializeResult(
            metadata={**counts.as_metadata(), "source": source, "table": tables.QUALIFIED_SUGGESTION_TABLE}
        )

    return _suggestions


__all__ = [
    "SUGGESTION_SELECT_COLUMNS", "ExtractConfig", "ExtractCounts", "changed_scope_sql", "since_scope_sql",
    "count_page_sql", "insert_page_sql", "run_extractor", "define_suggestion_asset",
]
