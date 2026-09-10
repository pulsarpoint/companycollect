"""Explicit document-oriented ESEF company-information extraction.

The asset selects each linked company's latest parsed report from final
ClickHouse state. Its exact model request is content-addressed in S3, and the
source-document result is written atomically back to ClickHouse. It never
writes a canonical company description, person, contact, or company row.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
import os
import uuid
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from hashlib import sha256
from typing import Any
from urllib.parse import quote

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from openai import OpenAI, OpenAIError, RateLimitError
from pydantic import ConfigDict, Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.llm_enrichment import (
    ENRICHMENT_EVIDENCE_SEGMENTS,
    ENRICHMENT_VISIBLE_SECTION_TYPES,
    PROMPT_VERSION,
    EsefEnrichmentInput,
    EsefLlmResponseError,
    build_company_enrichment_request,
    build_enrichment_evidence,
    enrichment_artifact_json_bytes,
    enrichment_object_key,
    enrichment_request_json_bytes,
    enrichment_request_object_key,
    request_company_enrichment,
)
from dagster_v3.defs.esef_filings.publish import (
    LINK_STATUS_GLEIF,
    LINK_STATUS_REGISTER_VERIFIED,
    LINK_STATUS_UNVERIFIED,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
)

_LINK_STATUSES = frozenset(
    {LINK_STATUS_REGISTER_VERIFIED, LINK_STATUS_UNVERIFIED, LINK_STATUS_GLEIF}
)

GROUP_NAME = "esef"
_PROGRESS_INTERVAL = 25
_NON_SPECIFIC_PERSON_ROLES = frozenset(
    {
        "key management personnel",
        "other key management personnel",
        "senior executives",
        "other senior executives",
        "other executives",
    }
)


class EsefLlmEnrichmentConfig(dg.Config):
    model_config = ConfigDict(str_strip_whitespace=True)

    # Deliberately no defaults: a bare "Materialize" from the Dagster UI must
    # fail run-config validation rather than silently spend on the default
    # provider. Every launcher (UI launchpad, backoffice, GraphQL) has to name
    # the provider and model explicitly.
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
        default=PROMPT_VERSION,
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
    reprocess_existing_without_model: bool = False
    max_evidence_chars: int = Field(default=64_000, ge=500, le=250_000)
    timeout_seconds: int = Field(default=180, ge=1, le=600)


def build_esef_llm_client(config: EsefLlmEnrichmentConfig) -> OpenAI:
    """Build the configured client while keeping credentials out of run config."""
    _validate_prompt_version(config.prompt_version)
    return _openai_client(
        base_url=config.base_url,
        api_key_environment_variable=config.api_key_environment_variable,
        timeout_seconds=config.timeout_seconds,
    )


def _openai_client(
    *,
    base_url: str,
    api_key_environment_variable: str,
    timeout_seconds: int,
) -> OpenAI:
    """Build an OpenAI-compatible client while keeping credentials out of run config.

    Shared by every ESEF LLM pass's asset function (company enrichment, people
    extraction): the credential lookup and client construction do not depend on
    which pass is calling.
    """
    variable = api_key_environment_variable
    api_key = os.getenv(variable, "").strip()
    if api_key == "":
        raise ValueError(f"No ESEF LLM API key: set {variable} on the Dagster host")
    return OpenAI(
        base_url=base_url.rstrip("/"),
        api_key=api_key,
        timeout=float(timeout_seconds),
        max_retries=2,
    )


def _validate_prompt_version(
    prompt_version: str, *, expected: str = PROMPT_VERSION
) -> None:
    if prompt_version != expected:
        raise ValueError(
            "Unsupported ESEF LLM prompt_version "
            f"{prompt_version!r}; this deployment provides {expected!r}"
        )


@dataclass(frozen=True)
class _PreparedEnrichment:
    document: Mapping[str, object]
    input_key: str
    evidence_input: EsefEnrichmentInput
    request_payload: Mapping[str, Any]
    request_key: str
    request_sha256: str
    output_key: str


@dataclass(frozen=True)
class _CompletedEnrichment:
    artifact: Mapping[str, Any]
    work: _PreparedEnrichment
    extraction_status: str


@dataclass(frozen=True)
class _EnrichmentRequestOutcome:
    work: _PreparedEnrichment
    # The enrichment pass reads .enrichment on this; the people pass reads
    # .extraction. Only the transport-level shape (RateLimitError/OpenAIError/
    # EsefLlmResponseError handling in _request_prepared_enrichment) is shared.
    result: object | None
    failure_kind: str | None = None
    # The caught exception itself, so the failure log line can show the actual
    # cause (a bad key, an exhausted balance, ...) beside the failure kind.
    failure_exception: BaseException | None = None


@dataclass(frozen=True)
class _PassProfile:
    """Fixes one LLM pass's functions and identifiers for the shared helpers below.

    ``run_esef_llm_enrichment`` and ``run_esef_people_extraction`` each build one
    of these and pass it to ``_prepare_pass_documents``/``_process_pass_outcomes``/
    ``_build_pass_rows`` so those helpers hold one implementation of the
    preparation, request-artifact, artifact-reuse, and row-building loops instead
    of each pass copying them.
    """

    prompt_version: str
    build_request: Callable[..., Mapping[str, Any]]
    request: Callable[..., object]
    request_key: Callable[..., str]
    output_key: Callable[..., str]
    artifact_bytes: Callable[..., bytes]
    extracted_status: str
    table: str
    columns: Sequence[str]


@dataclass(frozen=True)
class _PassRunParams:
    """Scalars the preparation loop needs that vary per run, not per document."""

    provider: str
    model: str
    temperature: float
    refresh_existing: bool
    reprocess_existing_without_model: bool
    max_evidence_chars: int
    evidence_segments: Sequence[str]
    visible_section_types: Sequence[str]


@dataclass(frozen=True)
class _PassArtifactParams:
    """Scalars the outcome loop needs to serialize a completed pass's artifact."""

    provider: str
    model: str
    base_url: str
    temperature: float
    source_run_id: str
    generated_at: str


@dataclass(frozen=True)
class _PreparedPassBatch:
    no_evidence_documents: list[dict[str, object]]
    completed: dict[str, _CompletedEnrichment]
    pending: list[_PreparedEnrichment]
    no_evidence_count: int
    reused_count: int
    request_artifact_written_count: int
    request_artifact_reused_count: int


@dataclass(frozen=True)
class _PassOutcomeCounts:
    completed: dict[str, _CompletedEnrichment]
    enriched_count: int
    failed_document_count: int
    rate_limited_document_count: int
    invalid_response_artifact_count: int


@dataclass(frozen=True)
class _PassRowBuildResult:
    rows: list[dict[str, object]]
    processed_documents: list[Mapping[str, object]]
    raw_person_candidate_count: int
    dropped_non_specific_person_candidate_count: int
    citation_adjustment_count: int
    dropped_invalid_citation_candidate_count: int
    prompt_token_count: int
    completion_token_count: int


def run_esef_llm_enrichment(
    *,
    clickhouse: ClickhouseResource,
    object_store: Any,
    client: OpenAI,
    model: str,
    source_run_id: str,
    country_iso2s: Sequence[str],
    company_ids: Sequence[str],
    source_document_ids: Sequence[str],
    max_documents: int | None,
    refresh_existing: bool,
    max_evidence_chars: int,
    log_info: Callable[..., object],
    reprocess_existing_without_model: bool = False,
    provider: str = "deepseek",
    base_url: str = "https://api.deepseek.com",
    temperature: float = 0,
    prompt_version: str = PROMPT_VERSION,
    concurrency: int = 1,
    link_statuses: Sequence[str] = (LINK_STATUS_REGISTER_VERIFIED,),
) -> dict[str, object]:
    """Extract company information from each company's latest final CH document."""
    _validate_prompt_version(prompt_version)
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
    if refresh_existing and reprocess_existing_without_model:
        raise ValueError(
            "ESEF LLM refresh_existing and reprocess_existing_without_model "
            "cannot both be enabled"
        )
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
    )
    artifacts = _load_disclosure_artifacts(clickhouse, documents=documents)
    object_store.ensure_bucket(ESEF_DOCUMENT_BUCKET)
    extracted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    log_info(
        "ESEF LLM latest-company selector: %s source documents considered",
        len(documents),
    )

    profile = _PassProfile(
        prompt_version=prompt_version,
        build_request=build_company_enrichment_request,
        request=request_company_enrichment,
        request_key=enrichment_request_object_key,
        output_key=enrichment_object_key,
        artifact_bytes=enrichment_artifact_json_bytes,
        extracted_status="enriched",
        table=tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
        columns=tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS,
    )
    run_params = _PassRunParams(
        provider=clean_provider,
        model=model,
        temperature=temperature,
        refresh_existing=refresh_existing,
        reprocess_existing_without_model=reprocess_existing_without_model,
        max_evidence_chars=max_evidence_chars,
        evidence_segments=ENRICHMENT_EVIDENCE_SEGMENTS,
        visible_section_types=ENRICHMENT_VISIBLE_SECTION_TYPES,
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
            generated_at=extracted_at,
        ),
        log_info=log_info,
    )
    completed = {**batch.completed, **outcome_counts.completed}

    no_evidence_rows_by_document = {
        str(document["source_document_id"]): _no_evidence_row(
            document,
            provider=clean_provider,
            model=model,
            prompt_version=prompt_version,
            source_run_id=source_run_id,
            extracted_at=extracted_at,
        )
        for document in batch.no_evidence_documents
    }

    def _build_information_row(
        document: Mapping[str, object],
        entry: _CompletedEnrichment,
    ) -> dict[str, object]:
        return _information_row(
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
        result_row=_build_information_row,
        artifact_people=_enrichment_artifact_people,
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
        "selection_method": "latest_xbrl_per_lei",
        "llm_provider": clean_provider,
        "llm_model": model,
        "llm_base_url": base_url.rstrip("/"),
        "llm_temperature": temperature,
        "llm_prompt_version": prompt_version,
        "llm_concurrency": concurrency,
        "reprocess_existing_without_model": reprocess_existing_without_model,
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
        "information_row_count": len(row_build.rows),
        "enriched_document_count": outcome_counts.enriched_count,
        "reused_enrichment_count": batch.reused_count,
        "no_evidence_count": batch.no_evidence_count,
        "prompt_token_count": row_build.prompt_token_count,
        "completion_token_count": row_build.completion_token_count,
        "request_artifact_written_count": batch.request_artifact_written_count,
        "request_artifact_reused_count": batch.request_artifact_reused_count,
        "description_candidate_count": sum(
            str(row["company_description"]) != "" for row in row_build.rows
        ),
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
        "table": tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
    }


def _enrichment_artifact_people(artifact: Mapping[str, Any]) -> list[object]:
    enrichment = _mapping(artifact.get("enrichment"), name="enrichment")
    people = enrichment.get("people", [])
    return people if isinstance(people, list) else []


def _request_enrichments(
    *,
    client: OpenAI,
    work: Sequence[_PreparedEnrichment],
    concurrency: int,
    log_info: Callable[..., object] | None = None,
    request: Callable[..., object] = request_company_enrichment,
) -> Iterator[_EnrichmentRequestOutcome]:
    """Call the HTTP client with bounded parallelism and retain document order."""
    call = partial(_request_prepared_enrichment, client=client, request=request)
    if concurrency == 1 or len(work) <= 1:
        for index, item in enumerate(work, start=1):
            if log_info is not None:
                log_info(
                    "ESEF LLM request starting: %s/%s",
                    index,
                    len(work),
                )
            yield call(item)
        return
    if log_info is not None:
        log_info(
            "ESEF LLM dispatching %s requests with concurrency %s",
            len(work),
            concurrency,
        )
    with ThreadPoolExecutor(
        max_workers=concurrency,
        thread_name_prefix="esef_llm_pass",
    ) as executor:
        yield from executor.map(call, work)


def _request_prepared_enrichment(
    work: _PreparedEnrichment,
    *,
    client: OpenAI,
    request: Callable[..., object] = request_company_enrichment,
) -> _EnrichmentRequestOutcome:
    try:
        result = request(
            client,
            evidence_input=work.evidence_input,
            request_payload=work.request_payload,
        )
    except RateLimitError as exc:
        return _EnrichmentRequestOutcome(
            work=work,
            result=None,
            failure_kind="rate_limited",
            failure_exception=exc,
        )
    except OpenAIError as exc:
        return _EnrichmentRequestOutcome(
            work=work,
            result=None,
            failure_kind="http_error",
            failure_exception=exc,
        )
    except EsefLlmResponseError as exc:
        return _EnrichmentRequestOutcome(
            work=work,
            result=None,
            failure_kind="invalid_response",
            failure_exception=exc,
        )
    return _EnrichmentRequestOutcome(work=work, result=result)


def _prepare_pass_documents(
    documents: Sequence[Mapping[str, object]],
    *,
    artifacts: Mapping[str, Mapping[str, Any]],
    object_store: Any,
    profile: _PassProfile,
    run: _PassRunParams,
    log_info: Callable[..., object],
) -> _PreparedPassBatch:
    """Prepare every selected document's evidence, request, and reuse decision.

    Shared by every ESEF LLM pass: for each document this builds the evidence,
    records a "no evidence" document when none is found, builds and
    content-addresses the exact request (writing or reusing its S3 artifact),
    and either marks the document already complete from a reused output
    artifact or queues it for a model call.
    """
    no_evidence_documents: list[dict[str, object]] = []
    completed: dict[str, _CompletedEnrichment] = {}
    pending: list[_PreparedEnrichment] = []
    no_evidence_count = 0
    reused_count = 0
    request_artifact_written_count = 0
    request_artifact_reused_count = 0

    for index, document in enumerate(documents, start=1):
        source_document_id = str(document["source_document_id"])
        input_key = _disclosure_input_key(source_document_id)
        segment_artifact = artifacts[source_document_id]
        try:
            evidence_input = build_enrichment_evidence(
                segment_artifact,
                max_evidence_chars=run.max_evidence_chars,
                evidence_segments=run.evidence_segments,
                visible_section_types=run.visible_section_types,
            )
        except ValueError as exc:
            if str(exc) != "ESEF segment artifact contains no LLM enrichment evidence":
                raise
            if (
                not run.refresh_existing
                and not run.reprocess_existing_without_model
                and str(document["existing_extraction_status"]) == "no_evidence"
            ):
                continue
            no_evidence_documents.append(
                {**document, "input_artifact_object_key": input_key}
            )
            no_evidence_count += 1
            continue

        package_sha256 = str(document["package_sha256"])
        request_payload = profile.build_request(
            evidence_input,
            model=run.model,
            provider=run.provider,
            temperature=run.temperature,
            prompt_version=profile.prompt_version,
        )
        request_bytes = enrichment_request_json_bytes(request_payload)
        request_sha256 = sha256(request_bytes).hexdigest()
        if (
            not run.refresh_existing
            and not run.reprocess_existing_without_model
            and str(document["existing_request_sha256"]) == request_sha256
        ):
            continue

        request_key = profile.request_key(
            request_sha256,
            provider=run.provider,
            model=run.model,
            prompt_version=profile.prompt_version,
        )
        if object_store.exists(request_key, bucket=ESEF_DOCUMENT_BUCKET):
            request_artifact_reused_count += 1
        else:
            object_store.write_bytes(
                request_key,
                request_bytes,
                bucket=ESEF_DOCUMENT_BUCKET,
            )
            request_artifact_written_count += 1
        output_key = profile.output_key(
            package_sha256,
            provider=run.provider,
            model=run.model,
            request_sha256=request_sha256,
            prompt_version=profile.prompt_version,
        )
        work = _PreparedEnrichment(
            document=document,
            input_key=input_key,
            evidence_input=evidence_input,
            request_payload=request_payload,
            request_key=request_key,
            request_sha256=request_sha256,
            output_key=output_key,
        )
        if not run.refresh_existing and object_store.exists(
            output_key,
            bucket=ESEF_DOCUMENT_BUCKET,
        ):
            artifact = _mapping(
                json.loads(
                    object_store.read_bytes(
                        output_key,
                        bucket=ESEF_DOCUMENT_BUCKET,
                    )
                ),
                name="LLM artifact",
            )
            completed[source_document_id] = _CompletedEnrichment(
                artifact=artifact,
                work=work,
                extraction_status="reused",
            )
            reused_count += 1
        else:
            pending.append(work)
        if index == 1 or index % _PROGRESS_INTERVAL == 0 or index == len(documents):
            log_info(
                "ESEF LLM selector: %s/%s documents prepared",
                index,
                len(documents),
            )
    return _PreparedPassBatch(
        no_evidence_documents=no_evidence_documents,
        completed=completed,
        pending=pending,
        no_evidence_count=no_evidence_count,
        reused_count=reused_count,
        request_artifact_written_count=request_artifact_written_count,
        request_artifact_reused_count=request_artifact_reused_count,
    )


def _finish_reason_from_message(message: str) -> str | None:
    """Pull ``finish_reason=...`` out of an EsefLlmResponseError message.

    The exception carries no structured finish_reason of its own -- only the
    validation and truncation messages embed it as text -- so the invalid-
    response artifact recovers it this way, falling back to ``None`` when it
    is absent or the placeholder value ``"unknown"``.
    """
    marker = "finish_reason="
    start = message.find(marker)
    if start < 0:
        return None
    start += len(marker)
    end = start
    while end < len(message) and message[end] not in ",) \n":
        end += 1
    value = message[start:end]
    return None if value in ("", "unknown") else value


def _write_invalid_response_artifact(
    *,
    object_store: Any,
    work: _PreparedEnrichment,
    source_document_id: str,
    raw_response: str,
    validation_summary: str,
    exc_text: str,
    log_info: Callable[..., object],
) -> str | None:
    """Archive an invalid model response beside its (never-written) artifact.

    The output key always ends in ``artifact.json``; this keeps everything
    ahead of that -- schema/prompt/model/package/request path segments -- and
    swaps in ``invalid_response.json`` so the raw text sits right next to
    where the validated artifact would have landed.

    This archive is best-effort: it is already handling a failed document, so
    an object-store error here must not propagate and stop the rest of the
    batch. The write is caught, logged, and swallowed -- returning ``None``
    tells the caller to fall back to the ordinary failure log line and leave
    ``invalid_response_artifact_count`` unchanged.
    """
    invalid_response_key = (
        work.output_key.removesuffix("artifact.json") + "invalid_response.json"
    )
    invalid_response_document = {
        "schema_version": 1,
        "source_document_id": source_document_id,
        "request_sha256": work.request_sha256,
        "finish_reason": _finish_reason_from_message(exc_text),
        "validation_summary": validation_summary,
        "raw_response": raw_response,
    }
    body = json.dumps(
        invalid_response_document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    try:
        object_store.write_bytes(
            invalid_response_key,
            body,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort archive, must not stop the batch
        log_info(
            "ESEF LLM could not keep the invalid response for document %s at %s: "
            "%s: %s",
            source_document_id,
            invalid_response_key,
            type(exc).__name__,
            str(exc)[:300],
        )
        return None
    return invalid_response_key


def _process_pass_outcomes(
    outcomes: Iterable[_EnrichmentRequestOutcome],
    *,
    attempted_count: int,
    profile: _PassProfile,
    object_store: Any,
    artifact_params: _PassArtifactParams,
    log_info: Callable[..., object],
) -> _PassOutcomeCounts:
    """Serialize every successful model outcome's artifact and record failures.

    Shared by every ESEF LLM pass: the artifact serializer and completed
    extraction status are supplied by ``profile``, everything else about
    walking the outcomes, writing the output artifact, and progress logging is
    identical between passes. An ``invalid_response`` failure whose exception
    carries a raw response additionally has that response archived beside the
    (unwritten) artifact -- the document itself still gets no row and is
    retried on the next run.
    """
    completed: dict[str, _CompletedEnrichment] = {}
    enriched_count = 0
    failed_document_count = 0
    rate_limited_document_count = 0
    invalid_response_artifact_count = 0
    for attempt_index, outcome in enumerate(outcomes, start=1):
        work = outcome.work
        result = outcome.result
        if result is None:
            failed_document_count += 1
            if outcome.failure_kind == "rate_limited":
                rate_limited_document_count += 1
            exc = outcome.failure_exception
            exc_type = type(exc).__name__ if exc is not None else "unknown"
            exc_text = str(exc)[:300] if exc is not None else ""
            source_document_id = str(work.document["source_document_id"])
            raw_response = getattr(exc, "raw_response", None)
            invalid_response_key = None
            if outcome.failure_kind == "invalid_response" and raw_response:
                validation_summary = (
                    getattr(exc, "validation_summary", None) or exc_text
                )
                invalid_response_key = _write_invalid_response_artifact(
                    object_store=object_store,
                    work=work,
                    source_document_id=source_document_id,
                    raw_response=raw_response,
                    validation_summary=validation_summary,
                    exc_text=exc_text,
                    log_info=log_info,
                )
                if invalid_response_key is not None:
                    invalid_response_artifact_count += 1
            if invalid_response_key is not None:
                log_info(
                    "ESEF LLM request failed for document %s "
                    "(invalid_response: %s); response kept at %s; "
                    "continuing batch: %s/%s attempted, %s failed",
                    source_document_id,
                    validation_summary,
                    invalid_response_key,
                    attempt_index,
                    attempted_count,
                    failed_document_count,
                )
            else:
                log_info(
                    "ESEF LLM request failed for document %s (%s: %s %s); "
                    "continuing batch: %s/%s attempted, %s failed",
                    source_document_id,
                    outcome.failure_kind,
                    exc_type,
                    exc_text,
                    attempt_index,
                    attempted_count,
                    failed_document_count,
                )
            continue
        serialized_artifact = profile.artifact_bytes(
            evidence_input=work.evidence_input,
            result=result,
            model=artifact_params.model,
            input_artifact_key=work.input_key,
            llm_request_object_key=work.request_key,
            llm_request_sha256=work.request_sha256,
            generated_at=artifact_params.generated_at,
            source_run_id=artifact_params.source_run_id,
            provider=artifact_params.provider,
            base_url=artifact_params.base_url.rstrip("/"),
            temperature=artifact_params.temperature,
            prompt_version=profile.prompt_version,
        )
        object_store.write_bytes(
            work.output_key,
            serialized_artifact,
            bucket=ESEF_DOCUMENT_BUCKET,
        )
        source_document_id = str(work.document["source_document_id"])
        completed[source_document_id] = _CompletedEnrichment(
            artifact=_mapping(json.loads(serialized_artifact), name="LLM artifact"),
            work=work,
            extraction_status=profile.extracted_status,
        )
        enriched_count += 1
        log_info(
            "ESEF LLM request progress: %s/%s attempted, %s processed, %s failed",
            attempt_index,
            attempted_count,
            enriched_count,
            failed_document_count,
        )
    return _PassOutcomeCounts(
        completed=completed,
        enriched_count=enriched_count,
        failed_document_count=failed_document_count,
        rate_limited_document_count=rate_limited_document_count,
        invalid_response_artifact_count=invalid_response_artifact_count,
    )


def _build_pass_rows(
    documents: Sequence[Mapping[str, object]],
    *,
    no_evidence_rows_by_document: Mapping[str, dict[str, object]],
    completed: Mapping[str, _CompletedEnrichment],
    result_row: Callable[
        [Mapping[str, object], _CompletedEnrichment], dict[str, object]
    ],
    artifact_people: Callable[[Mapping[str, Any]], list[object]],
) -> _PassRowBuildResult:
    """Build every processed document's row and its candidate-tracking counters.

    Shared by every ESEF LLM pass: ``result_row`` builds the pass-specific row
    (already bound to its provider/model/prompt_version/source_run_id/
    extracted_at by the caller) and ``artifact_people`` reads the raw,
    pre-publication-filter people list out of the pass's artifact shape;
    everything else about walking documents, counting candidates, and tallying
    tokens is identical between passes.
    """
    rows: list[dict[str, object]] = []
    processed_documents: list[Mapping[str, object]] = []
    raw_person_candidate_count = 0
    dropped_non_specific_person_candidate_count = 0
    citation_adjustment_count = 0
    dropped_invalid_citation_candidate_count = 0
    prompt_token_count = 0
    completion_token_count = 0
    for document in documents:
        source_document_id = str(document["source_document_id"])
        if source_document_id in no_evidence_rows_by_document:
            rows.append(no_evidence_rows_by_document[source_document_id])
            processed_documents.append(document)
            continue
        entry = completed.get(source_document_id)
        if entry is None:
            continue
        row = result_row(document, entry)
        artifact_person_list = artifact_people(entry.artifact)
        artifact_person_count = len(artifact_person_list)
        published_person_count = len(json.loads(str(row["people_json"])))
        raw_person_candidate_count += artifact_person_count
        dropped_non_specific_person_candidate_count += (
            artifact_person_count - published_person_count
        )
        validation = entry.artifact.get("validation", {})
        if isinstance(validation, Mapping):
            adjustments = validation.get("citation_adjustments", [])
            if isinstance(adjustments, list):
                citation_adjustment_count += len(adjustments)
                dropped_invalid_citation_candidate_count += sum(
                    1
                    for adjustment in adjustments
                    if isinstance(adjustment, Mapping)
                    and adjustment.get("action") == "candidate_dropped"
                )
        rows.append(row)
        processed_documents.append(document)
        prompt_token_count += int(row["prompt_tokens"])
        completion_token_count += int(row["completion_tokens"])
    return _PassRowBuildResult(
        rows=rows,
        processed_documents=processed_documents,
        raw_person_candidate_count=raw_person_candidate_count,
        dropped_non_specific_person_candidate_count=(
            dropped_non_specific_person_candidate_count
        ),
        citation_adjustment_count=citation_adjustment_count,
        dropped_invalid_citation_candidate_count=(
            dropped_invalid_citation_candidate_count
        ),
        prompt_token_count=prompt_token_count,
        completion_token_count=completion_token_count,
    )


_LATEST_DOCUMENT_SELECTION_COLUMNS = (
    "source_document_id",
    "package_sha256",
    "lei",
    "period_end",
    "fiscal_year",
    "package_url",
    "artifact_schema_version",
)


def _selection_query(
    *,
    model: str,
    provider: str = "deepseek",
    prompt_version: str = PROMPT_VERSION,
    link_statuses: set[str],
    country_iso2s: set[str],
    company_ids: set[str],
    source_document_ids: set[str],
    latest_per_lei: bool = True,
    existing_table: str = tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
    evidence_segments: Sequence[str] = ENRICHMENT_EVIDENCE_SEGMENTS,
    visible_section_types: Sequence[str] = ENRICHMENT_VISIBLE_SECTION_TYPES,
) -> tuple[str, dict[str, object]]:
    """Build the (pure) SQL and parameters selecting eligible documents.

    Only LEIs admitted through ``esef_entity_registry_map`` are eligible: the
    membership filter joins on ``link_status`` (and, when given, the map's
    ``country_iso2``/``registry_id``) rather than any stamp on the disclosure
    itself.

    With ``latest_per_lei=True`` (the company-information pass) only the latest
    report per LEI is selected. With ``latest_per_lei=False`` (the people pass,
    which extracts every filing) every eligible document is selected, newest
    first, so a ``max_documents`` cap keeps the newest filings. ``existing_table``
    is the prior-result table the reuse decision is read from, so each pass
    compares against its own results.
    """
    columns = _LATEST_DOCUMENT_SELECTION_COLUMNS
    document_filters = [
        "disclosures.package_sha256 != ''",
        "((disclosures.disclosure_kind = 'tagged_fact' AND disclosures.segment IN "
        "%(evidence_segments)s) OR "
        "(disclosures.disclosure_kind = 'visible_section' AND "
        "disclosures.section_type IN %(visible_section_types)s))",
    ]
    parameters: dict[str, object] = {
        "model_provider": provider,
        "model_name": model,
        "prompt_version": prompt_version,
        "evidence_segments": tuple(evidence_segments),
        "visible_section_types": tuple(visible_section_types),
    }
    link_filters = ["link_status IN %(link_statuses)s"]
    parameters["link_statuses"] = tuple(sorted(link_statuses))
    if country_iso2s:
        link_filters.append("country_iso2 IN %(country_iso2s)s")
        parameters["country_iso2s"] = tuple(sorted(country_iso2s))
    if company_ids:
        link_filters.append("registry_id IN %(company_ids)s")
        parameters["company_ids"] = tuple(sorted(company_ids))
    document_filters.append(
        "disclosures.lei IN (SELECT lei FROM "
        f"{tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE} FINAL "
        f"WHERE {' AND '.join(link_filters)})"
    )

    outer_filters = ["documents.latest_lei_report_rank = 1"] if latest_per_lei else []
    if source_document_ids:
        outer_filters.append("documents.source_document_id IN %(source_document_ids)s")
        parameters["source_document_ids"] = tuple(sorted(source_document_ids))
    where_clause = f"WHERE {' AND '.join(outer_filters)}\n" if outer_filters else ""
    order_by = (
        "ORDER BY documents.lei, documents.source_document_id"
        if latest_per_lei
        else (
            "ORDER BY documents.period_end DESC, documents.fiscal_year DESC, "
            "documents.lei, documents.source_document_id"
        )
    )
    select_columns = ", ".join(f"documents.{column}" for column in columns)
    query = f"""
SELECT
    {select_columns},
    ifNull(existing.llm_request_sha256, ''),
    ifNull(existing.extraction_status, '')
FROM
(
    SELECT
        {", ".join(columns)},
        row_number() OVER (
            PARTITION BY lei
            ORDER BY period_end DESC, fiscal_year DESC,
                source_processed_at DESC, source_document_id DESC
        ) AS latest_lei_report_rank
    FROM (
        SELECT
            disclosures.source_document_id AS source_document_id,
            argMax(disclosures.package_sha256, disclosures.resolved_at)
                AS package_sha256,
            argMax(disclosures.lei, disclosures.resolved_at) AS lei,
            argMax(toString(disclosures.period_end), disclosures.resolved_at)
                AS period_end,
            argMax(disclosures.fiscal_year, disclosures.resolved_at)
                AS fiscal_year,
            argMax(filings.package_url, filings.processed_at) AS package_url,
            max(disclosures.artifact_schema_version) AS artifact_schema_version,
            toString(max(filings.processed_at)) AS source_processed_at
        FROM {tables.QUALIFIED_ESEF_DISCLOSURES_TABLE} AS disclosures
        INNER JOIN {tables.QUALIFIED_ESEF_FILINGS_TABLE} AS filings FINAL
            ON filings.fxo_id = disclosures.source_document_id
        WHERE {" AND ".join(document_filters)}
        GROUP BY disclosures.source_document_id
    ) AS parsed_documents
) AS documents
LEFT JOIN
(
    SELECT
        source_document_id,
        argMax(llm_request_sha256, resolved_at) AS llm_request_sha256,
        argMax(extraction_status, resolved_at) AS extraction_status
    FROM {existing_table}
    WHERE model_provider = %(model_provider)s
      AND model_name = %(model_name)s
      AND prompt_version = %(prompt_version)s
    GROUP BY source_document_id
) AS existing USING (source_document_id)
{where_clause}{order_by}
"""
    return query, parameters


def _load_latest_source_documents(
    clickhouse: ClickhouseResource,
    *,
    model: str,
    provider: str = "deepseek",
    prompt_version: str = PROMPT_VERSION,
    link_statuses: set[str],
    country_iso2s: set[str],
    company_ids: set[str],
    source_document_ids: set[str],
    max_documents: int | None,
    latest_per_lei: bool = True,
    existing_table: str = tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
    evidence_segments: Sequence[str] = ENRICHMENT_EVIDENCE_SEGMENTS,
    visible_section_types: Sequence[str] = ENRICHMENT_VISIBLE_SECTION_TYPES,
) -> list[dict[str, object]]:
    query, parameters = _selection_query(
        model=model,
        provider=provider,
        prompt_version=prompt_version,
        link_statuses=link_statuses,
        country_iso2s=country_iso2s,
        company_ids=company_ids,
        source_document_ids=source_document_ids,
        latest_per_lei=latest_per_lei,
        existing_table=existing_table,
        evidence_segments=evidence_segments,
        visible_section_types=visible_section_types,
    )
    columns = _LATEST_DOCUMENT_SELECTION_COLUMNS
    with clickhouse.get_connection() as client:
        rows = client.execute(query, parameters)
    documents = [
        {
            **dict(zip(columns, row[:-2], strict=True)),
            "existing_request_sha256": str(row[-2]),
            "existing_extraction_status": str(row[-1]),
        }
        for row in rows
    ]
    if max_documents is not None:
        return documents[:max_documents]
    return documents


def _load_disclosure_artifacts(
    clickhouse: ClickhouseResource,
    *,
    documents: Sequence[Mapping[str, object]],
) -> dict[str, dict[str, Any]]:
    if not documents:
        return {}
    source_document_ids = tuple(
        sorted(str(document["source_document_id"]) for document in documents)
    )
    disclosure_columns = tables.ESEF_DISCLOSURES_EXPORT_COLUMNS
    disclosure_query = f"""
SELECT {", ".join(disclosure_columns)}
FROM {tables.QUALIFIED_ESEF_DISCLOSURES_TABLE}
WHERE source_document_id IN %(source_document_ids)s
ORDER BY source_document_id, disclosure_kind, report_member,
    anchor_visual_order, source_fact_key, segment, disclosure_id
"""
    label_query = f"""
SELECT
    concept_labels.source_document_id,
    concept_labels.concept_qname,
    concept_labels.language,
    argMax(
        concept_labels.label,
        tuple(
            concept_labels.is_report_language,
            concept_labels.resolved_at,
            concept_labels.label_id
        )
    ) AS resolved_label
FROM {tables.QUALIFIED_ESEF_DOCUMENT_CONCEPT_LABELS_TABLE} AS concept_labels
WHERE concept_labels.source_document_id IN %(source_document_ids)s
  AND concept_labels.label != ''
GROUP BY
    concept_labels.source_document_id,
    concept_labels.concept_qname,
    concept_labels.language
ORDER BY
    concept_labels.source_document_id,
    concept_labels.concept_qname,
    concept_labels.language
"""
    parameters = {"source_document_ids": source_document_ids}
    with clickhouse.get_connection() as client:
        disclosure_values = client.execute(disclosure_query, parameters)
        label_values = client.execute(label_query, parameters)

    labels: dict[tuple[str, str], dict[str, str]] = {}
    for source_document_id, concept_qname, language, label in label_values:
        labels.setdefault(
            (str(source_document_id), str(concept_qname)),
            {},
        )[str(language)] = str(label)

    artifacts = {
        str(document["source_document_id"]): _empty_disclosure_artifact(document)
        for document in documents
    }
    for values in disclosure_values:
        row = dict(zip(disclosure_columns, values, strict=True))
        source_document_id = str(row["source_document_id"])
        if source_document_id not in artifacts:
            raise ValueError(
                "ESEF disclosure query returned an unselected source document: "
                f"{source_document_id}"
            )
        artifact = artifacts[source_document_id]
        if str(row["disclosure_kind"]) == "tagged_fact":
            _add_tagged_fact_to_artifact(
                artifact,
                row=row,
                labels=labels.get(
                    (source_document_id, str(row["concept_qname"])),
                    {},
                ),
            )
        elif str(row["disclosure_kind"]) == "visible_section":
            _add_visible_section_to_artifact(artifact, row=row)
        else:
            raise ValueError(
                f"Unsupported ESEF disclosure_kind: {row['disclosure_kind']}"
            )
    return artifacts


def _empty_disclosure_artifact(
    document: Mapping[str, object],
) -> dict[str, Any]:
    source_document_id = str(document["source_document_id"])
    return {
        "schema_version": int(document["artifact_schema_version"]),
        "package_sha256": str(document["package_sha256"]),
        "source": {
            "fxo_id": source_document_id,
            "source_url": str(document["package_url"]),
            "object_key": _disclosure_input_key(source_document_id),
        },
        "concepts": {},
        "facts": {},
        "segments": {},
        "visible_sections": [],
    }


def _add_tagged_fact_to_artifact(
    artifact: dict[str, Any],
    *,
    row: Mapping[str, object],
    labels: Mapping[str, str],
) -> None:
    fact_key = str(row["source_fact_key"] or row["source_fact_id"])
    if fact_key == "":
        raise ValueError("ESEF tagged disclosure has no source fact key")
    concept_qname = str(row["concept_qname"])
    if concept_qname == "":
        raise ValueError("ESEF tagged disclosure has no concept qname")
    concepts = artifact["concepts"]
    concepts.setdefault(
        concept_qname,
        {
            "local_name": str(row["concept_local_name"]),
            "labels": dict(labels),
        },
    )
    facts = artifact["facts"]
    facts.setdefault(
        fact_key,
        {
            "fact_key": fact_key,
            "source_fact_id": str(row["source_fact_id"]),
            "report_member": str(row["report_member"]),
            "concept_qname": concept_qname,
            "canonical_value": str(row["plain_text"]),
            "language": str(row["language"]),
            "is_nil": False,
            "is_numeric": False,
            "period": _json_mapping(row["period_json"], name="period_json"),
        },
    )
    segment = str(row["segment"])
    if segment == "":
        return
    reference = {
        "fact_key": fact_key,
        "selection_reason": str(row["selection_reason"]),
    }
    segment_references = artifact["segments"].setdefault(segment, [])
    if reference not in segment_references:
        segment_references.append(reference)


def _add_visible_section_to_artifact(
    artifact: dict[str, Any],
    *,
    row: Mapping[str, object],
) -> None:
    text = str(row["plain_text"])
    original_character_count = int(row["original_character_count"])
    artifact["visible_sections"].append(
        {
            "section_type": str(row["section_type"]),
            "report_member": str(row["report_member"]),
            "heading": "",
            "text": text,
            "page_id": str(row["page_id"]),
            "printed_page_number": str(row["printed_page_number"]),
            "anchor_xpath": str(row["anchor_xpath"]),
            "anchor_visual_order": int(row["anchor_visual_order"]),
            "extraction_method": str(row["extraction_method"]),
            "language": str(row["language"]),
            "original_character_count": original_character_count,
            "included_character_count": len(text),
            "truncated": original_character_count > len(text),
            "text_sha256": str(row["text_sha256"]),
        }
    )


def _disclosure_input_key(source_document_id: str) -> str:
    return (
        f"clickhouse://{tables.QUALIFIED_ESEF_DISCLOSURES_TABLE}/"
        f"source_document_id={quote(source_document_id, safe='')}"
    )


def _information_row(
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
    extracted_at: str,
) -> dict[str, object]:
    enrichment = _mapping(artifact.get("enrichment"), name="enrichment")
    model_metadata = _mapping(artifact.get("model"), name="model")
    description_value = enrichment.get("company_description")
    description = (
        _mapping(description_value, name="company description")
        if description_value is not None
        else None
    )
    llm_response_text = str(model_metadata.get("raw_response", ""))
    return {
        **_information_identity(document),
        "extraction_status": extraction_status,
        "company_description": (
            str(description.get("description", "")) if description is not None else ""
        ),
        "description_language": (
            str(description.get("language", "")) if description is not None else ""
        ),
        "description_confidence": (
            float(description.get("confidence", 0.0))
            if description is not None
            else 0.0
        ),
        "description_evidence_ids_json": _json_text(
            description.get("evidence_ids", []) if description is not None else []
        ),
        "people_json": _json_text(
            _people_with_explicit_roles(enrichment.get("people", []))
        ),
        "products_and_services_json": _json_text(
            enrichment.get("products_and_services", [])
        ),
        "customer_markets_json": _json_text(enrichment.get("customer_markets", [])),
        "operating_geographies_json": _json_text(
            enrichment.get("operating_geographies", [])
        ),
        "business_segments_json": _json_text(enrichment.get("business_segments", [])),
        "material_group_relationships_json": _json_text(
            enrichment.get("material_group_relationships", [])
        ),
        "enrichment_artifact_object_key": artifact_object_key,
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


def _people_with_explicit_roles(value: object) -> list[object]:
    """Exclude non-person names and groups without a specific person's role.

    The complete model response remains preserved in the content-addressed LLM
    artifact and ``llm_response_text``. This filter only controls normalized
    people observations published for company display.
    """
    if not isinstance(value, list):
        return []
    people: list[object] = []
    for item in value:
        if not isinstance(item, Mapping):
            people.append(item)
            continue
        normalized_name = " ".join(str(item.get("name", "")).casefold().split()).strip(
            " ."
        )
        normalized_role = " ".join(str(item.get("role", "")).casefold().split()).strip(
            " ."
        )
        if normalized_role in _NON_SPECIFIC_PERSON_ROLES:
            continue
        if normalized_name == normalized_role:
            continue
        people.append(item)
    return people


def _no_evidence_row(
    document: Mapping[str, object],
    *,
    provider: str,
    model: str,
    prompt_version: str,
    source_run_id: str,
    extracted_at: str,
) -> dict[str, object]:
    return {
        **_information_identity(document),
        "extraction_status": "no_evidence",
        "company_description": "",
        "description_language": "",
        "description_confidence": 0.0,
        "description_evidence_ids_json": "[]",
        "people_json": "[]",
        "products_and_services_json": "[]",
        "customer_markets_json": "[]",
        "operating_geographies_json": "[]",
        "business_segments_json": "[]",
        "material_group_relationships_json": "[]",
        "enrichment_artifact_object_key": "",
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


def _information_identity(document: Mapping[str, object]) -> dict[str, object]:
    return {
        "source_document_id": str(document["source_document_id"]),
        "package_sha256": str(document["package_sha256"]),
        "lei": str(document["lei"]),
        "period_end": str(document["period_end"]),
        "fiscal_year": int(document["fiscal_year"]),
    }


def _replace_information_rows_clickhouse(
    clickhouse: ClickhouseResource,
    *,
    source_document_ids: Sequence[str],
    provider: str,
    model: str,
    prompt_version: str,
    rows: Sequence[Mapping[str, object]],
    table: str = tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
    columns: Sequence[str] = tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS,
) -> None:
    if not source_document_ids:
        return
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(table,),
    )
    target = f"`{tables.ESEF_DATABASE}`.`{table}`"
    stage_name = f"_tmp_{table}_{uuid.uuid4().hex}"
    stage = f"`{tables.ESEF_DATABASE}`.`{stage_name}`"
    parameters = {
        "source_document_ids": tuple(sorted(set(source_document_ids))),
        "model_provider": provider,
        "model_name": model,
        "prompt_version": prompt_version,
    }
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(
                f"INSERT INTO {stage} SELECT * FROM {target} WHERE NOT ("
                "source_document_id IN %(source_document_ids)s "
                "AND model_provider = %(model_provider)s "
                "AND model_name = %(model_name)s "
                "AND prompt_version = %(prompt_version)s)",
                parameters,
            )
            if rows:
                client.execute(
                    f"INSERT INTO {stage} ({', '.join(columns)}) VALUES",
                    [tuple(row[column] for column in columns) for row in rows],
                )
            client.execute(f"EXCHANGE TABLES {stage} AND {target}")
        finally:
            client.execute(f"DROP TABLE IF EXISTS {stage}")


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"ESEF {name} must be an object")
    return value


def _json_mapping(value: object, *, name: str) -> dict[str, str]:
    parsed = json.loads(str(value))
    mapping = _mapping(parsed, name=name)
    return {str(key): str(item) for key, item in mapping.items()}


def _json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dg.asset(
    name="esef_document_company_information_clickhouse",
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
    pool="esef_document_company_information_clickhouse",
    retry_policy=dg.RetryPolicy(
        max_retries=3,
        delay=60,
        backoff=dg.Backoff.EXPONENTIAL,
    ),
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE},
    description=(
        "Selects the latest canonical ESEF disclosures per company, archives "
        "the exact content-addressed OpenAI-compatible request in S3, and atomically "
        "writes source-document observations to ClickHouse."
    ),
)
def esef_document_company_information_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefLlmEnrichmentConfig,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    metadata = run_esef_llm_enrichment(
        clickhouse=clickhouse,
        object_store=object_store,
        client=build_esef_llm_client(config),
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
        reprocess_existing_without_model=config.reprocess_existing_without_model,
    )
    return dg.MaterializeResult(metadata=metadata)


defs = dg.Definitions(assets=[esef_document_company_information_clickhouse])
