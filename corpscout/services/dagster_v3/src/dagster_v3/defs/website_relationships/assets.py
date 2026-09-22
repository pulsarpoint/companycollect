"""Bounded relationship analysis after the crawler's saved-result import."""

from collections import Counter

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.domain_relationships import digest, model_settings
from dagster_v3.defs.se_company.info import build_llm_client
from dagster_v3.defs.website_relationships.relationships import (
    ANALYSIS_COLUMNS, ANALYSIS_TABLE, SYSTEM_PROMPT, WebsiteRelationshipProfile, analyze_domain,
)


class WebsiteRelationshipConfig(dg.Config):
    execute: bool = False
    result_ids: list[str] = Field(default_factory=list)
    max_domains: int = Field(default=25, ge=1, le=1_000)
    max_input_chars: int = Field(default=80_000, ge=4_000, le=250_000)
    profile: WebsiteRelationshipProfile = Field(default_factory=WebsiteRelationshipProfile)


def pending_relationships_sql(*, scoped: bool) -> str:
    scope = "AND i.result_id IN %(result_ids)s" if scoped else ""
    return f"""SELECT i.result_id AS result_id, i.crawl_domain AS crawl_domain,
        i.result_kind AS result_kind, i.source_path AS source_path, i.source_host AS source_host,
        i.country_code AS country_code, i.company_id AS company_id,
        i.reporting_entity_name AS reporting_entity_name, i.registrable_domain AS registrable_domain,
        i.captured_at AS captured_at, i.source_url AS source_url, i.evidence_json AS evidence_json
    FROM corpscout.website_domain_relationship_inputs AS i
    LEFT ANTI JOIN corpscout.{ANALYSIS_TABLE} AS done
        ON done.result_id = i.result_id AND done.source_host = i.source_host
        AND done.country_code = i.country_code AND done.company_id = i.company_id
        AND done.reporting_entity_name = i.reporting_entity_name
        AND done.registrable_domain = i.registrable_domain
        AND done.source_evidence_hash = lower(hex(SHA256(i.evidence_json)))
        AND done.status = 'success' AND done.analysis_version = %(analysis_version)s
        AND done.prompt_hash = %(prompt_hash)s AND done.model_config_json = %(model_config_json)s
    WHERE 1 {scope}
    ORDER BY i.captured_at DESC, i.result_id, i.source_host, i.registrable_domain
    LIMIT %(max_domains)s"""


@dg.asset(
    group_name="website_relationships", kinds={"python", "llm", "clickhouse"},
    pool="website_domain_relationships",
    description="Explain saved external links from company websites with free text and exact citations. Preview by default; requires imported crawler results and an unambiguous active source-host association.",
)
def website_domain_relationships(
    context: dg.AssetExecutionContext, config: WebsiteRelationshipConfig, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    counts: Counter[str] = Counter()
    with clickhouse.get_connection() as database:
        assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=(ANALYSIS_TABLE, "website_domain_relationship_inputs"))
        rows, columns = database.execute(
            pending_relationships_sql(scoped=bool(config.result_ids)),
            {"result_ids": tuple(config.result_ids), "max_domains": config.max_domains,
             "analysis_version": config.profile.prompt_version, "prompt_hash": digest(SYSTEM_PROMPT),
             "model_config_json": model_settings(config.profile)}, with_column_types=True,
        )
        inputs = [dict(zip((name for name, _ in columns), row, strict=True)) for row in rows]
        counts["selected_domains"] = len(inputs)
        if config.execute and inputs:
            with build_llm_client(config.profile, timeout_seconds=120) as llm:
                for row in inputs:
                    result = analyze_domain(row, profile=config.profile, client=llm,
                                            run_id=context.run_id, max_input_chars=config.max_input_chars)
                    database.execute(
                        f"INSERT INTO corpscout.{ANALYSIS_TABLE} ({', '.join(ANALYSIS_COLUMNS)}) VALUES",
                        [tuple(result[column] for column in ANALYSIS_COLUMNS)],
                        settings={"async_insert": 1, "wait_for_async_insert": 1},
                    )
                    counts[result["status"]] += 1
                    context.log.info("Website context: company=%s:%s source=%s domain=%s status=%s",
                                     row["country_code"], row["company_id"], row["source_host"],
                                     row["registrable_domain"], result["status"])
    metadata = {"execute": config.execute, **dict(counts)}
    if any(counts[status] for status in ("http_error", "invalid_response", "context_limit", "needs_context")):
        raise dg.Failure("Some website interpretations need attention; all attempts are saved.", metadata=metadata)
    return dg.MaterializeResult(metadata=metadata)


website_domain_relationships_job = dg.define_asset_job(
    "website_domain_relationships_job", selection=dg.AssetSelection.assets("website_domain_relationships"),
    description="Analyze a bounded set of saved website domain contexts, reusing unchanged successful answers.",
)
