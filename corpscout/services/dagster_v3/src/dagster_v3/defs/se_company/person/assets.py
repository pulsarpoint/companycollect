"""People normalization, matching and publication, plus company correction and precedence maintenance."""

from collections import Counter
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.info import build_llm_client
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.batch import (
    BUCKET_COUNT,
    PAGE_SIZE as FOLD_PAGE_SIZE,
    FoldCounts,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.person.match import MatchCounts, PersonMatchProfile, run_match
from dagster_v3.defs.se_company.person.match_input import refresh_all_inputs, refresh_inputs
from dagster_v3.defs.se_company.person.normalize import (
    PAGE_SIZE,
    NormalizeCounts,
    normalize_all,
    normalize_companies,
)
from dagster_v3.defs.se_company.person.precedence import precedence_rows

GROUP_NAME = "se_company_person"
NORMALIZE_POOL = "se_company_person_normalize"
# Publishing scans the normalized table per bucket, so it visits buckets sequentially
# and uses a limit-1 pool to serialize full publication runs. The targeted company fold
# shares NORMALIZE_POOL because it also writes normalized rows and input snapshots.
FOLD_POOL = "se_company_person_fold"
# One pool of limit 1 (the instance default), so two match runs can never race on the same
# companies and double-spend their calls (spec section 8).
MATCH_POOL = "se_company_person_match"
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata", "ratsit")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(
    f"se_company_person_suggestions_{source}" for source in EXTRACTOR_SOURCES
)


class PersonNormalizeConfig(dg.Config):
    changed_only: bool = True
    company_ids: list[str] = Field(default_factory=list)
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


@dg.asset(
    name="se_company_person_normalize",
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES],
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_NORMALIZED_TABLE, "reads": tables.QUALIFIED_SUGGESTION_TABLE,
              "input_table": tables.QUALIFIED_MATCH_INPUT_TABLE},
    description=(
        "Normalizes raw person suggestions into se_company_person_normalized: rows never "
        "normalized, computed from an older raw version, or computed by an older normalizer "
        "version. Refreshes complete per-company match-input hashes after each page and repairs "
        "missing or stale snapshots. changed_only=false re-normalizes every raw row; company_ids targets companies."
    ),
)
def se_company_person_normalize(
    context: dg.AssetExecutionContext, config: PersonNormalizeConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.SUGGESTION_TABLE, tables.NORMALIZED_TABLE, tables.MATCH_INPUT_TABLE)
    )
    normalized_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        if config.company_ids:
            counts = normalize_companies(
                client, config.company_ids, changed_only=config.changed_only,
                normalized_at=normalized_at, page_size=config.page_size, log=context.log,
            )
        else:
            counts = normalize_all(
                client, changed_only=config.changed_only, normalized_at=normalized_at,
                page_size=config.page_size, log=context.log,
            )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "table": tables.QUALIFIED_NORMALIZED_TABLE}
    )


@dg.asset(
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    deps=[se_company_person_normalize],
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MATCH_INPUT_TABLE},
    description=(
        "Repairs or backfills per-company People input snapshots without LLM calls. "
        "Normalization already maintains snapshots after each written page; this asset "
        "also supports rebuilding them after a hash algorithm change."
    ),
)
def se_company_person_match_input(
    context: dg.AssetExecutionContext, config: PersonNormalizeConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE,
        tables=(tables.NORMALIZED_TABLE, tables.MATCH_INPUT_TABLE),
    )
    with clickhouse.get_connection() as client:
        if config.company_ids:
            count = 0
            for start in range(0, len(config.company_ids), config.page_size):
                count += refresh_inputs(client, config.company_ids[start:start + config.page_size])
        else:
            count = refresh_all_inputs(
                client, changed_only=config.changed_only, page_size=config.page_size, log=context.log,
            )
    return dg.MaterializeResult(metadata={"companies": count, "table": tables.QUALIFIED_MATCH_INPUT_TABLE})


@dg.asset(
    name="se_company_person_match",
    group_name=GROUP_NAME,
    pool=MATCH_POOL,
    deps=[se_company_person_match_input],
    kinds={"clickhouse", "python", "llm"},
    retry_policy=dg.RetryPolicy(max_retries=3, delay=60, backoff=dg.Backoff.EXPONENTIAL),
    metadata={
        "table": tables.QUALIFIED_MATCH_TABLE,
        "state_table": tables.QUALIFIED_MATCH_STATE_TABLE,
        "reads": tables.QUALIFIED_MATCH_INPUT_TABLE,
    },
    description=(
        "Asks an LLM, once per company whose normalized people come from two or more "
        "machine sources, which of them are the same physical person, and stores the scored "
        "pairs in se_company_person_match with one state row per company in "
        "se_company_person_match_state. The fold unions the pairs at or above "
        "MATCH_THRESHOLD. changed_only=true sends a company with no state row, one whose "
        "model-visible data or effective prompt/model changed (a different input_hash), and one whose last attempt failed "
        "TRANSIENTLY (rate_limited:, http_error:, unexpected:); a malformed, truncated or "
        "empty answer and a company over the candidate cap are STICKY for the same input -- "
        "skipped without a new state row and reported as skipped_sticky -- until its "
        "data or configuration changes. Binding-only changes replay the saved answer against "
        "current normalized IDs without an LLM call. Prompt names and revisions do not "
        "invalidate identical content. company_ids targets companies. provider and model have no "
        "defaults -- a bare Materialize fails validation rather than spending on one -- and "
        "the provider's API key is read from the host environment at call time. A page in "
        "which most calls fail raises after its rows are written, so a provider outage "
        "retries with backoff instead of finishing green."
    ),
)
def se_company_person_match(
    context: dg.AssetExecutionContext,
    config: PersonMatchProfile,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE,
        tables=(tables.MATCH_INPUT_TABLE, tables.MATCH_TABLE, tables.MATCH_STATE_TABLE),
    )
    # Built before any page is touched, so a run configured for a provider whose key this
    # host does not carry fails without having written a row or spent a call.
    llm_client = build_llm_client(
        config, timeout_seconds=config.timeout_seconds,
        api_key_environment_variable=config.api_key_environment_variable,
    )
    with clickhouse.get_connection() as client:
        counts: MatchCounts = run_match(
            client, llm_client=llm_client, config=config, source_run_id=context.run_id,
            log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "prompt_version": config.prompt_version,
                  "table": tables.QUALIFIED_MATCH_TABLE}
    )


def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string for ``toDateTime64(..., 3,
    'UTC')``: a bare tz-aware datetime parameter would let the stale-pairs comparison depend
    on the server's default timezone and drop sub-second precision (basic info and the
    address entity do the same)."""
    return exported_at.strftime("%Y-%m-%d %H:%M:%S.") + f"{exported_at.microsecond // 1000:03d}"


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    decided_by 'code') and count global rules exported before this run that the dictionary
    no longer names. Returns (pairs inserted, stale pairs remaining). Never touches a
    company-scoped row.

    `decided_at` is one of the fold's selection watermarks (batch.py's
    `global_precedence_watermark_sql`), so writing a fresh one when nothing actually changed
    would re-fold every company on an idle re-materialisation. Read the stored global rows
    first: when they already equal `precedence_rows()`, insert nothing and report 0 pairs
    (the caller reads that as `unchanged`); otherwise (a first export against an empty table,
    or a changed dictionary) insert the five rows as before."""
    wanted = precedence_rows()
    stored = {
        tuple(row)
        for row in client.execute(
            f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
            "FINAL WHERE company_id = '' AND removed = 0"
        )
    }
    pairs = 0
    if stored != set(wanted):
        rows = [
            ("", field, source, precedence, 0, "code", "", exported_at)
            for field, source, precedence in wanted
        ]
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
            f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        pairs = len(rows)
    stale = int(
        client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
            "WHERE company_id = '' AND removed = 0 "
            "AND decided_at < toDateTime64(%(exported_at)s, 3, 'UTC')",
            {"exported_at": _precedence_export_timestamp(exported_at)},
        )[0][0]
    )
    return pairs, stale


@dg.asset(
    name="se_company_person_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports PERSON_PRECEDENCE to se_company_person_precedence as global rules "
        "(company_id '', field 'name') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, which "
        "re-folds every company (the export's stamp is newer than their last fold). Idle "
        "re-materialisation is a no-op: when the stored rows already match the dictionary, "
        "nothing is written and the watermark does not move. Never touches a company-scoped "
        "row."
    ),
)
def se_company_person_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,)
    )
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(
        metadata={
            "pairs": pairs, "stale_pairs": stale, "unchanged": pairs == 0,
            "table": tables.QUALIFIED_PRECEDENCE_TABLE,
        }
    )


_FOLD_TABLES = (
    tables.NORMALIZED_TABLE, tables.MATCH_TABLE, tables.MATCH_STATE_TABLE,
    tables.MAIN_TABLE, tables.HISTORY_TABLE, tables.RULE_TABLE, tables.PRECEDENCE_TABLE,
)


class PersonFoldConfig(dg.Config):
    # True: only companies whose newest normalized row, rule version or precedence row (their
    # own, or the global export) is newer than their last fold, plus companies never folded
    # that have a foldable row. False re-folds all companies; history rows are written
    # either way only where a compared column changed.
    changed_only: bool = True
    # Companies per page. Lower it if a page's rows press the host's memory.
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)


class PersonFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


def targeted_fold(
    client: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int,
    log: Callable[..., object] | None,
    logger: Any = None,
) -> tuple[NormalizeCounts, FoldCounts]:
    """The targeted fold normalizes the companies' raw rows first (spec section 7: a
    reviewer's brand-new suggestion has to parse before Fold now can publish it), always
    changed_only so only rows never normalized, on another suggestion_id or on an older
    normalizer version are touched; then folds the same ids with the caller's changed_only.
    `log` (a plain callable) goes to fold_companies, which calls it directly; `logger` (an
    object with `.info`) goes to normalize_companies, which calls `log.info(...)` -- the two
    want different shapes, so the caller passes both."""
    normalized = normalize_companies(
        client, company_ids, changed_only=True, normalized_at=folded_at,
        page_size=page_size, log=logger,
    )
    folded = fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
    return normalized, folded


@dg.asset(
    group_name=GROUP_NAME,
    deps=[se_company_person_normalize, se_company_person_match],
    pool=FOLD_POOL,
    kinds={"clickhouse", "python"},
    description=(
        "Publishes People across all companies, visiting the 64 fold buckets sequentially. "
        "Used by the backoffice Full processing action. By default only changed companies "
        "are folded. Runs after normalization and, when selected, LLM matching."
    ),
)
def se_company_person_publish(
    context: dg.AssetExecutionContext, config: PersonFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    totals: Counter[str] = Counter()
    with clickhouse.get_connection() as client:
        for bucket in range(BUCKET_COUNT):
            counts = fold_bucket(
                client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
                folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
            )
            totals.update({key: value for key, value in counts.as_metadata().items() if isinstance(value, int)})
            context.log.info("People publish: completed bucket %d of %d", bucket + 1, BUCKET_COUNT)
    return dg.MaterializeResult(metadata={
        **totals, "buckets": BUCKET_COUNT, "changed_only": config.changed_only,
        "table": tables.QUALIFIED_MAIN_TABLE,
    })


@dg.asset(
    name="se_company_person_fold_companies",
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    kinds={"clickhouse", "python"},
    metadata={
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
    },
    description=(
        "The targeted person fold: the companies named in config.company_ids, whatever their "
        "bucket. The backoffice's Fold now button launches this asset for one company. "
        "Normalizes the companies' raw rows first, so a reviewer's draft parses on Fold now."
    ),
)
def se_company_person_fold_companies(
    context: dg.AssetExecutionContext,
    config: PersonFoldCompaniesConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE,
        tables=(tables.SUGGESTION_TABLE, tables.MATCH_INPUT_TABLE, *_FOLD_TABLES),
    )
    folded_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        normalized, counts = targeted_fold(
            client, config.company_ids, changed_only=config.changed_only,
            source_run_id=context.run_id, folded_at=folded_at, page_size=config.page_size,
            log=context.log.info, logger=context.log,
        )
    return dg.MaterializeResult(
        metadata={
            **counts.as_metadata(),
            "changed_only": config.changed_only,
            "page_size": config.page_size,
            "table": tables.QUALIFIED_MAIN_TABLE,
            "history_table": tables.QUALIFIED_HISTORY_TABLE,
            **{f"normalize_{key}": value for key, value in normalized.as_metadata().items()},
        }
    )
