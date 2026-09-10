"""ESEF people-per-filing extraction: one paid LLM pass per document per LEI.

Unlike ``esef_document_company_information_clickhouse`` (the latest report per
company), this pass selects every eligible filing for every admitted LEI,
newest first, and extracts only the people named with an explicit role. It
shares its preparation, request, artifact-reuse, and row-building loops with
the company-information pass through the ``_PassProfile``-parameterised
helpers in ``llm_enrichment_assets``; it never writes a canonical person or
company row.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from openai import OpenAI
from pydantic import ConfigDict, Field

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.llm_enrichment import (
    PEOPLE_EVIDENCE_SEGMENTS,
    PEOPLE_PROMPT_VERSION,
    PEOPLE_VISIBLE_SECTION_TYPES,
    build_people_extraction_request,
    people_extraction_artifact_json_bytes,
    people_extraction_object_key,
    people_request_object_key,
    request_people_extraction,
)
from dagster_v3.defs.esef_filings.llm_enrichment_assets import (
    _LINK_STATUSES,
    _CompletedEnrichment,
    _PassArtifactParams,
    _PassProfile,
    _PassRunParams,
    _build_pass_rows,
    _information_identity,
    _json_text,
    _load_disclosure_artifacts,
    _load_latest_source_documents,
    _mapping,
    _openai_client,
    _people_with_explicit_roles,
    _prepare_pass_documents,
    _process_pass_outcomes,
    _replace_information_rows_clickhouse,
    _request_enrichments,
    _validate_prompt_version,
)
from dagster_v3.defs.esef_filings.publish import LINK_STATUS_REGISTER_VERIFIED
from dagster_v3.defs.esef_filings.segment_assets import ESEF_DOCUMENT_BUCKET

GROUP_NAME = "esef"


class EsefPeopleExtractionConfig(dg.Config):
    model_config = ConfigDict(str_strip_whitespace=True)

    # Deliberately no defaults: a bare "Materialize" from the Dagster UI must
    # fail run-config validation rather than silently spend on the default
    # provider, mirroring EsefLlmEnrichmentConfig.
    provider: str = Field(min_length=1, max_length=120)
    model: str = Field(min_length=1, max_length=200)
    base_url: str = Field(
        default="https://api.deepseek.com",
        min_length=1,
        max_length=2_048,
    )
    api_key_environment_variable: str = Field(
        default="DEEPSEEK_API_KEY",
        min_length=1,
        max_length=128,
        pattern=r"^[A-Za-z_][A-Za-z0-9_]*$",
    )
    temperature: float = Field(default=0, ge=0, le=2)
    prompt_version: str = Field(
        default=PEOPLE_PROMPT_VERSION,
        min_length=1,
        max_length=120,
    )
    concurrency: int = Field(default=1, ge=1, le=8)
    country_iso2s: list[str] = Field(default_factory=list)
    link_statuses: list[str] = Field(
        default_factory=lambda: [LINK_STATUS_REGISTER_VERIFIED]
    )
    company_ids: list[str] = Field(default_factory=list)
    source_document_ids: list[str] = Field(default_factory=list)
    max_documents: int | None = Field(default=None, ge=1, le=100_000)
    refresh_existing: bool = False
    max_evidence_chars: int = Field(default=64_000, ge=500, le=250_000)
    timeout_seconds: int = Field(default=180, ge=1, le=600)


def run_esef_people_extraction(
    *,
    clickhouse: ClickhouseResource,
    object_store: Any,
    client: OpenAI,
    model: str,
    source_run_id: str,
    country_iso2s: Sequence[str],
    link_statuses: Sequence[str],
    company_ids: Sequence[str],
    source_document_ids: Sequence[str],
    max_documents: int | None,
    refresh_existing: bool,
    max_evidence_chars: int,
    log_info: Callable[..., object],
    provider: str = "deepseek",
    base_url: str = "https://api.deepseek.com",
    temperature: float = 0,
    prompt_version: str = PEOPLE_PROMPT_VERSION,
    concurrency: int = 1,
) -> dict[str, object]:
    """Extract people from every eligible filing of every admitted LEI.

    Follows ``run_esef_llm_enrichment`` step by step, without its
    ``reprocess_existing_without_model`` reprocessing mode: selection takes
    every filing per admitted LEI (newest first) instead of only the latest,
    and the paid pass extracts people only.
    """
    _validate_prompt_version(prompt_version, expected=PEOPLE_PROMPT_VERSION)
    clean_provider = provider.strip().casefold()
    model = model.strip()
    base_url = base_url.strip().rstrip("/")
    if clean_provider == "":
        raise ValueError("ESEF LLM provider must not be empty")
    if model == "":
        raise ValueError("ESEF LLM model must not be empty")
    if base_url == "":
        raise ValueError("ESEF LLM base_url must not be empty")
    if concurrency < 1 or concurrency > 8:
        raise ValueError("ESEF LLM concurrency must be between 1 and 8")
    selected_country_iso2s = {
        value.strip().upper() for value in country_iso2s if value.strip()
    }
    if any(
        len(country_iso2) != 2 or not country_iso2.isalpha()
        for country_iso2 in selected_country_iso2s
    ):
        raise ValueError("ESEF LLM country_iso2s must contain two-letter country codes")
    selected_company_ids = {value.strip() for value in company_ids if value.strip()}
    if selected_company_ids and len(selected_country_iso2s) != 1:
        raise ValueError(
            "ESEF LLM company_ids require exactly one country_iso2 because "
            "company identity is country-scoped"
        )
    selected_link_statuses = {value.strip() for value in link_statuses if value.strip()}
    if not selected_link_statuses:
        raise ValueError("ESEF LLM link_statuses must not be empty")
    if not selected_link_statuses <= _LINK_STATUSES:
        raise ValueError(
            f"ESEF LLM link_statuses must be a subset of {sorted(_LINK_STATUSES)}"
        )
    selected_ids = {value.strip() for value in source_document_ids if value.strip()}
    documents = _load_latest_source_documents(
        clickhouse,
        provider=clean_provider,
        model=model,
        prompt_version=prompt_version,
        link_statuses=selected_link_statuses,
        country_iso2s=selected_country_iso2s,
        company_ids=selected_company_ids,
        source_document_ids=selected_ids,
        max_documents=max_documents,
        latest_per_lei=False,
        existing_table=tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
        evidence_segments=PEOPLE_EVIDENCE_SEGMENTS,
        visible_section_types=PEOPLE_VISIBLE_SECTION_TYPES,
    )
    artifacts = _load_disclosure_artifacts(clickhouse, documents=documents)
    object_store.ensure_bucket(ESEF_DOCUMENT_BUCKET)
    extracted_at = datetime.now(UTC)
    generated_at = extracted_at.isoformat().replace("+00:00", "Z")

    log_info(
        "ESEF LLM people-extraction selector: %s source documents considered",
        len(documents),
    )

    profile = _PassProfile(
        prompt_version=prompt_version,
        build_request=build_people_extraction_request,
        request=request_people_extraction,
        request_key=people_request_object_key,
        output_key=people_extraction_object_key,
        artifact_bytes=people_extraction_artifact_json_bytes,
        extracted_status="extracted",
        table=tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
        columns=tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_EXPORT_COLUMNS,
    )
    run_params = _PassRunParams(
        provider=clean_provider,
        model=model,
        temperature=temperature,
        refresh_existing=refresh_existing,
        reprocess_existing_without_model=False,
        max_evidence_chars=max_evidence_chars,
        evidence_segments=PEOPLE_EVIDENCE_SEGMENTS,
        visible_section_types=PEOPLE_VISIBLE_SECTION_TYPES,
    )
    batch = _prepare_pass_documents(
        documents,
        artifacts=artifacts,
        object_store=object_store,
        profile=profile,
        run=run_params,
        log_info=log_info,
    )

    outcomes = _request_enrichments(
        client=client,
        work=batch.pending,
        concurrency=concurrency,
        log_info=log_info,
        request=profile.request,
    )
    outcome_counts = _process_pass_outcomes(
        outcomes,
        attempted_count=len(batch.pending),
        profile=profile,
        object_store=object_store,
        artifact_params=_PassArtifactParams(
            provider=clean_provider,
            model=model,
            base_url=base_url,
            temperature=temperature,
            source_run_id=source_run_id,
            generated_at=generated_at,
        ),
        log_info=log_info,
    )
    completed = {**batch.completed, **outcome_counts.completed}

    no_evidence_rows_by_document = {
        str(document["source_document_id"]): _people_no_evidence_row(
            document,
            provider=clean_provider,
            model=model,
            prompt_version=prompt_version,
            source_run_id=source_run_id,
            extracted_at=extracted_at,
        )
        for document in batch.no_evidence_documents
    }

    def _build_people_row(
        document: Mapping[str, object],
        entry: _CompletedEnrichment,
    ) -> dict[str, object]:
        return _people_row(
            document,
            artifact=entry.artifact,
            artifact_object_key=entry.work.output_key,
            input_artifact_object_key=entry.work.input_key,
            llm_request_object_key=entry.work.request_key,
            llm_request_sha256=entry.work.request_sha256,
            extraction_status=entry.extraction_status,
            provider=clean_provider,
            model=model,
            prompt_version=prompt_version,
            source_run_id=source_run_id,
            extracted_at=extracted_at,
        )

    row_build = _build_pass_rows(
        documents,
        no_evidence_rows_by_document=no_evidence_rows_by_document,
        completed=completed,
        result_row=_build_people_row,
        artifact_people=_people_artifact_candidates,
    )

    _replace_information_rows_clickhouse(
        clickhouse,
        source_document_ids=[
            str(document["source_document_id"])
            for document in row_build.processed_documents
        ],
        provider=clean_provider,
        model=model,
        prompt_version=prompt_version,
        rows=row_build.rows,
        table=profile.table,
        columns=profile.columns,
    )
    return {
        "selection_method": "every_filing_per_lei",
        "llm_provider": clean_provider,
        "llm_model": model,
        "llm_base_url": base_url,
        "llm_temperature": temperature,
        "llm_prompt_version": prompt_version,
        "llm_concurrency": concurrency,
        "candidate_document_count": len(documents),
        "attempted_document_count": len(batch.pending),
        "processed_document_count": len(row_build.processed_documents),
        "failed_document_count": outcome_counts.failed_document_count,
        "rate_limited_document_count": outcome_counts.rate_limited_document_count,
        "invalid_response_artifact_count": (
            outcome_counts.invalid_response_artifact_count
        ),
        "selected_document_count": len(row_build.processed_documents),
        "unchanged_document_count": (
            len(documents)
            - len(row_build.processed_documents)
            - outcome_counts.failed_document_count
        ),
        "selected_lei_count": len(
            {str(document["lei"]) for document in row_build.processed_documents}
        ),
        "extraction_row_count": len(row_build.rows),
        "extracted_document_count": outcome_counts.enriched_count,
        "reused_extraction_count": batch.reused_count,
        "no_evidence_count": batch.no_evidence_count,
        "prompt_token_count": row_build.prompt_token_count,
        "completion_token_count": row_build.completion_token_count,
        "request_artifact_written_count": batch.request_artifact_written_count,
        "request_artifact_reused_count": batch.request_artifact_reused_count,
        "person_candidate_count": sum(
            len(json.loads(str(row["people_json"]))) for row in row_build.rows
        ),
        "raw_person_candidate_count": row_build.raw_person_candidate_count,
        "dropped_non_specific_person_candidate_count": (
            row_build.dropped_non_specific_person_candidate_count
        ),
        "citation_adjustment_count": row_build.citation_adjustment_count,
        "dropped_invalid_citation_candidate_count": (
            row_build.dropped_invalid_citation_candidate_count
        ),
        "table": tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
    }


def _people_artifact_candidates(artifact: Mapping[str, Any]) -> list[object]:
    extraction = _mapping(artifact.get("extraction"), name="extraction")
    people = extraction.get("people", [])
    return people if isinstance(people, list) else []


def _people_row(
    document: Mapping[str, object],
    *,
    artifact: Mapping[str, Any],
    artifact_object_key: str,
    input_artifact_object_key: str,
    llm_request_object_key: str,
    llm_request_sha256: str,
    extraction_status: str,
    provider: str,
    model: str,
    prompt_version: str,
    source_run_id: str,
    extracted_at: datetime,
) -> dict[str, object]:
    extraction = _mapping(artifact.get("extraction"), name="extraction")
    model_metadata = _mapping(artifact.get("model"), name="model")
    llm_response_text = str(model_metadata.get("raw_response", ""))
    return {
        **_information_identity(document),
        "extraction_status": extraction_status,
        "people_json": _json_text(
            _people_with_explicit_roles(extraction.get("people", []))
        ),
        "extraction_artifact_object_key": artifact_object_key,
        "input_artifact_object_key": input_artifact_object_key,
        "llm_request_object_key": llm_request_object_key,
        "llm_request_sha256": llm_request_sha256,
        "llm_response_text": llm_response_text,
        "llm_response_sha256": str(model_metadata.get("raw_response_sha256", "")),
        "model_provider": str(model_metadata.get("provider", provider)),
        "model_name": str(model_metadata.get("name", model)),
        "prompt_version": str(artifact.get("prompt_version", prompt_version)),
        "prompt_tokens": int(model_metadata.get("prompt_tokens") or 0),
        "completion_tokens": int(model_metadata.get("completion_tokens") or 0),
        "input_character_count": int(artifact.get("input_character_count") or 0),
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }


def _people_no_evidence_row(
    document: Mapping[str, object],
    *,
    provider: str,
    model: str,
    prompt_version: str,
    source_run_id: str,
    extracted_at: datetime,
) -> dict[str, object]:
    return {
        **_information_identity(document),
        "extraction_status": "no_evidence",
        "people_json": "[]",
        "extraction_artifact_object_key": "",
        "input_artifact_object_key": str(document["input_artifact_object_key"]),
        "llm_request_object_key": "",
        "llm_request_sha256": "",
        "llm_response_text": "",
        "llm_response_sha256": "",
        "model_provider": provider,
        "model_name": model,
        "prompt_version": prompt_version,
        "prompt_tokens": 0,
        "completion_tokens": 0,
        "input_character_count": 0,
        "source_run_id": source_run_id,
        "extracted_at": extracted_at,
    }


@dg.asset(
    name="esef_document_people_extraction_clickhouse",
    deps=[
        dg.AssetDep(
            dg.AssetKey("esef_disclosures_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetDep(
            dg.AssetKey("esef_document_concept_labels_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        ),
        dg.AssetKey("esef_filings_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "clickhouse", "llm", "xbrl"},
    pool="esef_document_people_extraction_clickhouse",
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE},
    description=(
        "Extracts the people named with an explicit role from every eligible "
        "ESEF filing of every admitted LEI (not only the latest), archives the "
        "exact content-addressed OpenAI-compatible request in S3, and atomically "
        "writes per-document observations to ClickHouse."
    ),
)
def esef_document_people_extraction_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefPeopleExtractionConfig,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    _validate_prompt_version(config.prompt_version, expected=PEOPLE_PROMPT_VERSION)
    client = _openai_client(
        base_url=config.base_url,
        api_key_environment_variable=config.api_key_environment_variable,
        timeout_seconds=config.timeout_seconds,
    )
    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=client,
        provider=config.provider,
        model=config.model,
        base_url=config.base_url,
        temperature=config.temperature,
        prompt_version=config.prompt_version,
        concurrency=config.concurrency,
        source_run_id=context.run_id,
        country_iso2s=config.country_iso2s,
        link_statuses=config.link_statuses,
        company_ids=config.company_ids,
        source_document_ids=config.source_document_ids,
        max_documents=config.max_documents,
        refresh_existing=config.refresh_existing,
        max_evidence_chars=config.max_evidence_chars,
        log_info=context.log.info,
    )
    return dg.MaterializeResult(metadata=metadata)


defs = dg.Definitions(assets=[esef_document_people_extraction_clickhouse])
