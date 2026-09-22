"""Global domain source sync and publication, with optional evidence verification."""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.batch import insert_rows, publish_domains, read_rows, verify_domains
from dagster_v3.defs.se_company.domain.precedence import DOMAIN_PRECEDENCE
from dagster_v3.defs.se_company.domain.verification import DomainVerificationProfile


class DomainPublishConfig(dg.Config):
    changed_only: bool = True
    page_size: int = Field(default=1_000, ge=1, le=5_000)
    # Publication only hashes this profile to select existing answers. Supplying the
    # same profile to both steps prevents reuse of an old prompt after a capped run.
    # None permits reuse of the latest answer for the current evidence.
    verification: DomainVerificationProfile | None = None


class DomainVerificationConfig(DomainPublishConfig):
    retry_failed_only: bool = Field(default=False, description="Retry only existing failed responses for this evidence, prompt and model. Reuse successes and skip never-processed associations.")
    max_llm_calls: int | None = Field(default=None, ge=1, description="Optional limit for a manually bounded run. Omit to verify every eligible association.")


@dg.asset(group_name=tables.GROUP_NAME, kinds={"clickhouse", "python"}, pool="se_company_domain_fold")
def se_company_domain_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,))
    with clickhouse.get_connection() as client:
        stored = read_rows(client, tables.PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS, [""])
        existing = {(r["field"], r["source"]): r for r in stored}
        wanted = {(field, source): rank for field, sources in DOMAIN_PRECEDENCE.items() for source, rank in sources.items()}
        stamp = datetime.now(UTC)
        rows = []
        for key in sorted(set(existing) | set(wanted)):
            field, source = key
            rank = wanted.get(key, 0)
            removed = int(key not in wanted)
            before = existing.get(key)
            if before is not None and before["precedence"] == rank and before["removed"] == removed:
                continue
            rows.append(dict(zip(tables.PRECEDENCE_COLUMNS,
                                 ("", "", field, source, rank, removed, "code", "", stamp), strict=True)))
        insert_rows(client, tables.PRECEDENCE_TABLE, tables.PRECEDENCE_COLUMNS, rows)
    return dg.MaterializeResult(metadata={"rules_written": len(rows), "unchanged": not rows})


@dg.asset(
    group_name=tables.GROUP_NAME, kinds={"clickhouse", "python", "llm"}, pool="se_company_domain_fold",
    deps=[*tables.EXTRACTOR_ASSETS, "se_company_domain_precedence_clickhouse"],
    description="Send all eligible uncertain or conflicting company-domain suggestions to the configured LLM. Store the verdict, confidence score (0–1), explanation, evidence citations and every attempt in se_company_domain_verification. Reuse successful unchanged inputs. No profile means no LLM calls.",
)
def se_company_domain_verification(
    context: dg.AssetExecutionContext, config: DomainVerificationConfig, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=tables.TABLES)
    with clickhouse.get_connection() as client:
        counts = verify_domains(client, page_size=config.page_size, changed_only=config.changed_only,
                                profile=config.verification, max_llm_calls=config.max_llm_calls,
                                retry_failed_only=config.retry_failed_only,
                                run_id=context.run_id, log=context.log.info)
    metadata = {**counts, "changed_only": config.changed_only,
                "verification_enabled": config.verification is not None,
                "verification_scope": "uncertain_or_conflicting", "retry_failed_only": config.retry_failed_only,
                "table": tables.VERIFICATION_TABLE,
                "score_column": "confidence", "score_range": "0–1 (confidence in the verdict)"}
    if counts["http_error"] or counts["invalid_response"]:
        raise dg.Failure(
            f"Domain verification finished with {counts['invalid_response']} invalid model responses and "
            f"{counts['http_error']} HTTP errors from {counts['llm_calls']} calls; "
            f"{counts['success']} new answers succeeded and {counts['verification_reused']} were reused. "
            "Successful answers are saved. Retry with changed_only=true and retry_failed_only=true "
            "to preserve successes and skip new associations. See per-domain failure logs for the cause.",
            metadata=metadata,
        )
    return dg.MaterializeResult(metadata=metadata)


@dg.asset(
    group_name=tables.GROUP_NAME, kinds={"clickhouse", "python"}, pool="se_company_domain_fold",
    deps=["se_company_domain_verification"],
    description="Fold Swedish company domains from source suggestions, precedence, reviewer decisions and stored current-input LLM verdicts. Append change history before publication. This step never calls an LLM.",
)
def se_company_domain_publish(
    context: dg.AssetExecutionContext, config: DomainPublishConfig, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    context.log.info("Domain publication: checking required ClickHouse tables")
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=tables.TABLES)
    context.log.info("Domain publication: tables available; opening ClickHouse connection")
    with clickhouse.get_connection() as client:
        counts = publish_domains(client, page_size=config.page_size, changed_only=config.changed_only,
                                 profile=config.verification, run_id=context.run_id, log=context.log.info)
    return dg.MaterializeResult(metadata={**counts, "changed_only": config.changed_only,
                                         "table": tables.MAIN_TABLE, "history_table": tables.HISTORY_TABLE})
