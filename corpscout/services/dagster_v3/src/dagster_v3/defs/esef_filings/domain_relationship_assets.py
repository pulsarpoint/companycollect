"""Bounded, independently retryable ESEF relationship analysis from stored context."""

from collections import Counter

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.domain_relationships import digest, model_settings
from dagster_v3.defs.esef_filings.domain_context import CONTEXT_VERSION
from dagster_v3.defs.esef_filings.domain_relationships import (
    ANALYSIS_COLUMNS, ANALYSIS_TABLE, SYSTEM_PROMPT, RelationshipProfile,
    analyze_domain,
)
from dagster_v3.defs.se_company.info import build_llm_client


class EsefDomainRelationshipConfig(dg.Config):
    execute: bool = False
    source_document_ids: list[str] = Field(default_factory=list)
    max_domains: int = Field(default=25, ge=1, le=1_000)
    max_input_chars: int = Field(default=80_000, ge=4_000, le=250_000)
    profile: RelationshipProfile = Field(default_factory=RelationshipProfile)


def pending_relationships_sql(*, scoped: bool) -> str:
    scope = "AND d.source_document_id IN %(source_document_ids)s" if scoped else ""
    return f"""SELECT d.source_document_id AS source_document_id, d.package_sha256 AS package_sha256, d.lei AS lei,
        f.entity_name AS reporting_entity_name, d.period_end AS period_end, d.registrable_domain AS registrable_domain,
        coalesce(nullIf(f.viewer_url, ''), nullIf(f.report_url, ''), f.package_url) AS source_url,
        d.evidence_json AS evidence_json
    FROM corpscout.esef_domains AS d FINAL
    INNER JOIN corpscout.esef_filings AS f FINAL ON f.fxo_id = d.source_document_id
        AND f.package_sha256 = d.package_sha256 AND f.lei = d.lei
    LEFT ANTI JOIN (
        SELECT source_document_id, registrable_domain, package_sha256, lei,
            reporting_entity_name, period_end, source_evidence_hash
        FROM corpscout.{ANALYSIS_TABLE}
        WHERE status = 'success' AND analysis_version = %(analysis_version)s
            AND prompt_hash = %(prompt_hash)s AND model_config_json = %(model_config_json)s
    ) AS done ON done.source_document_id = d.source_document_id
        AND done.registrable_domain = d.registrable_domain
        AND done.package_sha256 = d.package_sha256 AND done.lei = d.lei
        AND done.reporting_entity_name = f.entity_name AND done.period_end = d.period_end
        AND done.source_evidence_hash = lower(hex(SHA256(d.evidence_json)))
    WHERE d.extraction_status = 'ok' AND d.registrable_domain != ''
        AND arrayExists(e -> JSONExtractString(e, 'source_context', 'version') = %(context_version)s,
            JSONExtractArrayRaw(d.evidence_json)) {scope}
    ORDER BY d.period_end DESC, d.source_document_id, d.registrable_domain
    LIMIT %(max_domains)s"""


@dg.asset(
    group_name="esef", kinds={"python", "llm", "clickhouse"},
    deps=["esef_domains_clickhouse"], pool="esef_domain_relationships",
    description="Explain domain mentions from persisted report sections using free text and exact source quotations. All roles are retained. Preview by default; never changes company websites.",
)
def esef_domain_relationships(
    context: dg.AssetExecutionContext, config: EsefDomainRelationshipConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    counts: Counter[str] = Counter()
    with clickhouse.get_connection() as database:
        assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=(ANALYSIS_TABLE,))
        rows, columns = database.execute(
            pending_relationships_sql(scoped=bool(config.source_document_ids)),
            {"source_document_ids": tuple(config.source_document_ids), "max_domains": config.max_domains,
             "analysis_version": config.profile.prompt_version, "prompt_hash": digest(SYSTEM_PROMPT),
             "model_config_json": model_settings(config.profile), "context_version": CONTEXT_VERSION},
            with_column_types=True,
        )
        inputs = [dict(zip((name for name, _ in columns), row, strict=True)) for row in rows]
        counts["selected_domains"] = len(inputs)
        if config.execute and inputs:
            with build_llm_client(config.profile, timeout_seconds=120) as llm:
                for row in inputs:
                    result = analyze_domain(row, profile=config.profile, client=llm,
                                            run_id=context.run_id, max_input_chars=config.max_input_chars)
                    # Persist every attempt before proceeding. Database failure must
                    # stop the run, not silently lose a paid response or evidence.
                    database.execute(
                        f"INSERT INTO corpscout.{ANALYSIS_TABLE} ({', '.join(ANALYSIS_COLUMNS)}) VALUES",
                        [tuple(result[column] for column in ANALYSIS_COLUMNS)],
                        settings={"async_insert": 1, "wait_for_async_insert": 1},
                    )
                    counts[result["status"]] += 1
                    context.log.info("Domain context analysis: document=%s domain=%s status=%s",
                                     row["source_document_id"], row["registrable_domain"], result["status"])
    metadata = {"execute": config.execute, **dict(counts)}
    if any(counts[status] for status in ("http_error", "invalid_response", "context_limit", "needs_context")):
        raise dg.Failure("Some domain interpretations need attention; successful answers and all attempts are saved.", metadata=metadata)
    return dg.MaterializeResult(metadata=metadata)


esef_domain_relationships_job = dg.define_asset_job(
    "esef_domain_relationships_job", selection=dg.AssetSelection.assets("esef_domain_relationships"),
    description="Analyze a bounded set of stored ESEF domain contexts; reuse unchanged successful answers.",
)
