"""Explicit document-oriented ESEF company-information extraction.

The asset selects each linked company's latest parsed report from final
ClickHouse state. Its exact model request is content-addressed in S3, and the
model output is stored against the source document in DuckDB before ClickHouse
publication. It never writes a canonical company description, person, contact,
or company row.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from openai import OpenAI
from pydantic import Field

from dagster_v3.defs.common.duckdb_resources import safe_duckdb_connection
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.llm_enrichment import (
    PROMPT_VERSION,
    SUPPORTED_ENRICHMENT_ARTIFACT_SCHEMA_VERSIONS,
    build_company_enrichment_request,
    build_enrichment_evidence,
    deepseek_settings,
    enrichment_artifact_json_bytes,
    enrichment_object_key,
    enrichment_request_json_bytes,
    enrichment_request_object_key,
    request_company_enrichment,
)
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_DOCUMENT_BUCKET,
    ensure_document_company_information_table,
)

GROUP_NAME = "esef_filings"
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
    country_iso2: str = ""
    company_ids: list[str] = Field(default_factory=list)
    source_document_ids: list[str] = Field(default_factory=list)
    max_documents: int | None = Field(default=None, ge=1, le=100_000)
    refresh_existing: bool = False
    reprocess_existing_without_model: bool = False
    max_evidence_chars: int = Field(default=64_000, ge=500, le=250_000)
    timeout_seconds: int = Field(default=180, ge=1, le=600)


def run_esef_llm_enrichment(
    *,
    esef_filings_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
    object_store: Any,
    client: OpenAI,
    model: str,
    source_run_id: str,
    country_iso2: str,
    company_ids: Sequence[str],
    source_document_ids: Sequence[str],
    max_documents: int | None,
    refresh_existing: bool,
    max_evidence_chars: int,
    log_info: Callable[..., object],
    reprocess_existing_without_model: bool = False,
) -> dict[str, object]:
    """Extract company information from each company's latest final CH document."""
    if refresh_existing and reprocess_existing_without_model:
        raise ValueError(
            "ESEF LLM refresh_existing and reprocess_existing_without_model "
            "cannot both be enabled"
        )
    clean_country_iso2 = country_iso2.strip().upper()
    if clean_country_iso2 != "" and (
        len(clean_country_iso2) != 2 or not clean_country_iso2.isalpha()
    ):
        raise ValueError("ESEF LLM country_iso2 must be a two-letter country code")
    selected_company_ids = {value.strip() for value in company_ids if value.strip()}
    if selected_company_ids and clean_country_iso2 == "":
        raise ValueError(
            "ESEF LLM company_ids require country_iso2 because company identity "
            "is country-scoped"
        )
    selected_ids = {value.strip() for value in source_document_ids if value.strip()}
    documents = _load_latest_source_documents(
        clickhouse,
        model=model,
        country_iso2=clean_country_iso2,
        company_ids=selected_company_ids,
        source_document_ids=selected_ids,
        max_documents=max_documents,
        refresh_existing=(refresh_existing or reprocess_existing_without_model),
    )
    object_store.ensure_bucket(ESEF_DOCUMENT_BUCKET)
    extracted_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    information_rows: list[dict[str, object]] = []
    enriched_count = 0
    reused_count = 0
    no_evidence_count = 0
    prompt_token_count = 0
    completion_token_count = 0
    request_artifact_written_count = 0
    request_artifact_reused_count = 0
    raw_person_candidate_count = 0
    dropped_non_specific_person_candidate_count = 0
    citation_adjustment_count = 0
    dropped_invalid_citation_candidate_count = 0

    log_info(
        "ESEF LLM latest-company selector: %s source documents selected",
        len(documents),
    )
    for index, document in enumerate(documents, start=1):
        input_key = str(document["parsed_artifact_object_key"])
        if not object_store.exists(input_key, bucket=ESEF_DOCUMENT_BUCKET):
            raise ValueError(
                "Missing upstream ESEF parsed artifact; materialize "
                f"esef_document_artifacts_s3 first: {input_key}"
            )
        segment_artifact = _source_specific_segment_artifact(
            json.loads(
                object_store.read_bytes(
                    input_key,
                    bucket=ESEF_DOCUMENT_BUCKET,
                )
            ),
            document=document,
        )
        try:
            evidence_input = build_enrichment_evidence(
                segment_artifact,
                max_evidence_chars=max_evidence_chars,
            )
        except ValueError as exc:
            if str(exc) != "ESEF segment artifact contains no LLM enrichment evidence":
                raise
            information_rows.append(
                _no_evidence_row(
                    document,
                    model=model,
                    source_run_id=source_run_id,
                    extracted_at=extracted_at,
                )
            )
            no_evidence_count += 1
            continue

        package_sha256 = str(document["package_sha256"])
        request_payload = build_company_enrichment_request(
            evidence_input,
            model=model,
        )
        request_bytes = enrichment_request_json_bytes(request_payload)
        request_sha256 = sha256(request_bytes).hexdigest()
        request_key = enrichment_request_object_key(
            request_sha256,
            model=model,
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
        output_key = enrichment_object_key(
            package_sha256,
            model=model,
            request_sha256=request_sha256,
        )
        if not refresh_existing and object_store.exists(
            output_key,
            bucket=ESEF_DOCUMENT_BUCKET,
        ):
            enrichment_artifact = _mapping(
                json.loads(
                    object_store.read_bytes(
                        output_key,
                        bucket=ESEF_DOCUMENT_BUCKET,
                    )
                ),
                name="LLM enrichment artifact",
            )
            extraction_status = "reused"
            reused_count += 1
        else:
            result = request_company_enrichment(
                client,
                evidence_input=evidence_input,
                request_payload=request_payload,
            )
            serialized_artifact = enrichment_artifact_json_bytes(
                evidence_input=evidence_input,
                result=result,
                model=model,
                input_artifact_key=f"s3://{ESEF_DOCUMENT_BUCKET}/{input_key}",
                llm_request_object_key=request_key,
                llm_request_sha256=request_sha256,
                generated_at=extracted_at,
                source_run_id=source_run_id,
            )
            object_store.write_bytes(
                output_key,
                serialized_artifact,
                bucket=ESEF_DOCUMENT_BUCKET,
            )
            enrichment_artifact = _mapping(
                json.loads(serialized_artifact),
                name="LLM enrichment artifact",
            )
            extraction_status = "enriched"
            enriched_count += 1

        information_row = _information_row(
            document,
            enrichment_artifact=enrichment_artifact,
            enrichment_artifact_object_key=output_key,
            input_artifact_object_key=input_key,
            llm_request_object_key=request_key,
            llm_request_sha256=request_sha256,
            extraction_status=extraction_status,
            model=model,
            source_run_id=source_run_id,
            extracted_at=extracted_at,
        )
        artifact_enrichment = _mapping(
            enrichment_artifact.get("enrichment"),
            name="enrichment",
        )
        artifact_people = artifact_enrichment.get("people", [])
        artifact_person_count = (
            len(artifact_people) if isinstance(artifact_people, list) else 0
        )
        published_person_count = len(json.loads(str(information_row["people_json"])))
        raw_person_candidate_count += artifact_person_count
        dropped_non_specific_person_candidate_count += (
            artifact_person_count - published_person_count
        )
        validation = enrichment_artifact.get("validation", {})
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
        information_rows.append(information_row)
        prompt_token_count += int(information_row["prompt_tokens"])
        completion_token_count += int(information_row["completion_tokens"])
        if index == 1 or index % _PROGRESS_INTERVAL == 0 or index == len(documents):
            log_info(
                "ESEF LLM latest-company selector: %s/%s documents processed",
                index,
                len(documents),
            )

    _replace_information_rows(
        esef_filings_duckdb,
        source_document_ids=[str(row["source_document_id"]) for row in documents],
        model=model,
        rows=information_rows,
    )
    return {
        "selection_method": "latest_xbrl_per_company",
        "selection_country_iso2": clean_country_iso2,
        "llm_model": model,
        "reprocess_existing_without_model": reprocess_existing_without_model,
        "selected_document_count": len(documents),
        "selected_company_count": len(
            {
                (str(document["country_iso2"]), str(document["company_id"]))
                for document in documents
            }
        ),
        "information_row_count": len(information_rows),
        "enriched_document_count": enriched_count,
        "reused_enrichment_count": reused_count,
        "no_evidence_count": no_evidence_count,
        "prompt_token_count": prompt_token_count,
        "completion_token_count": completion_token_count,
        "request_artifact_written_count": request_artifact_written_count,
        "request_artifact_reused_count": request_artifact_reused_count,
        "description_candidate_count": sum(
            str(row["company_description"]) != "" for row in information_rows
        ),
        "person_candidate_count": sum(
            len(json.loads(str(row["people_json"]))) for row in information_rows
        ),
        "raw_person_candidate_count": raw_person_candidate_count,
        "dropped_non_specific_person_candidate_count": (
            dropped_non_specific_person_candidate_count
        ),
        "citation_adjustment_count": citation_adjustment_count,
        "dropped_invalid_citation_candidate_count": (
            dropped_invalid_citation_candidate_count
        ),
        "table": tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,
    }


def _load_latest_source_documents(
    clickhouse: ClickhouseResource,
    *,
    model: str,
    country_iso2: str,
    company_ids: set[str],
    source_document_ids: set[str],
    max_documents: int | None,
    refresh_existing: bool,
) -> list[dict[str, object]]:
    columns = (
        "source_document_id",
        "package_sha256",
        "lei",
        "country_iso2",
        "company_id",
        "period_end",
        "fiscal_year",
        "package_url",
        "package_object_key",
        "parsed_artifact_object_key",
        "artifact_schema_version",
    )
    compatible_artifact_filters = [
        (
            f"(artifact_schema_version = {schema_version} AND "
            "parsed_artifact_object_key LIKE "
            f"'esef_filings/ixbrl_segments/schema=v{schema_version}/%%')"
        )
        for schema_version in SUPPORTED_ENRICHMENT_ARTIFACT_SCHEMA_VERSIONS
    ]
    document_filters = [
        "extraction_status IN ('parsed', 'reused')",
        "parsed_artifact_object_key != ''",
        f"({' OR '.join(compatible_artifact_filters)})",
        "company_id != ''",
        "country_iso2 != ''",
    ]
    parameters: dict[str, object] = {
        "model_name": model,
        "prompt_version": PROMPT_VERSION,
    }
    if country_iso2 != "":
        document_filters.append("country_iso2 = %(country_iso2)s")
        parameters["country_iso2"] = country_iso2
    if company_ids:
        document_filters.append("company_id IN %(company_ids)s")
        parameters["company_ids"] = tuple(sorted(company_ids))

    outer_filters = ["documents.latest_company_report_rank = 1"]
    if source_document_ids:
        outer_filters.append("documents.source_document_id IN %(source_document_ids)s")
        parameters["source_document_ids"] = tuple(sorted(source_document_ids))
    if not refresh_existing:
        outer_filters.append(
            "(ifNull(existing.source_document_id, '') = '' OR "
            "ifNull(existing.input_artifact_object_key, '') != "
            "documents.parsed_artifact_object_key)"
        )
    limit_sql = ""
    if max_documents is not None:
        limit_sql = "LIMIT %(max_documents)s"
        parameters["max_documents"] = max_documents

    select_columns = ", ".join(f"documents.{column}" for column in columns)
    query = f"""
SELECT {select_columns}
FROM
(
    SELECT
        {", ".join(columns)},
        row_number() OVER (
            PARTITION BY country_iso2, company_id
            ORDER BY period_end DESC, fiscal_year DESC,
                source_processed_at DESC, source_document_id DESC
        ) AS latest_company_report_rank
    FROM
    (
        SELECT
            {", ".join(columns)},
            source_processed_at,
            row_number() OVER (
                PARTITION BY country_iso2, company_id, source_document_id
                ORDER BY artifact_schema_version DESC,
                    source_processed_at DESC, parsed_artifact_object_key DESC
            ) AS preferred_filing_artifact_rank
        FROM {tables.QUALIFIED_ESEF_SOURCE_DOCUMENTS_TABLE}
        WHERE {" AND ".join(document_filters)}
    ) AS compatible_documents
    WHERE preferred_filing_artifact_rank = 1
) AS documents
LEFT JOIN
(
    SELECT source_document_id, input_artifact_object_key
    FROM {tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE}
    WHERE model_name = %(model_name)s
      AND prompt_version = %(prompt_version)s
) AS existing USING (source_document_id)
WHERE {" AND ".join(outer_filters)}
ORDER BY documents.country_iso2, documents.company_id,
    documents.source_document_id
{limit_sql}
"""
    with clickhouse.get_connection() as client:
        rows = client.execute(query, parameters)
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _source_specific_segment_artifact(
    value: Any,
    *,
    document: Mapping[str, object],
) -> dict[str, Any]:
    artifact = dict(_mapping(value, name="segment artifact"))
    artifact_schema_version = artifact.get("schema_version")
    expected_schema_version = document.get("artifact_schema_version")
    if artifact_schema_version != expected_schema_version:
        raise ValueError(
            "ESEF segment artifact schema does not match source-document metadata: "
            f"expected={expected_schema_version} actual={artifact_schema_version}"
        )
    source = dict(_mapping(artifact.get("source"), name="segment artifact source"))
    source.update(
        {
            "fxo_id": str(document["source_document_id"]),
            "country": str(document["country_iso2"]),
            "company_id": str(document["company_id"]),
            "source_url": str(document["package_url"]),
            "object_key": (
                f"s3://{ESEF_DOCUMENT_BUCKET}/{document['package_object_key']}"
            ),
        }
    )
    artifact["source"] = source
    return artifact


def _information_row(
    document: Mapping[str, object],
    *,
    enrichment_artifact: Mapping[str, Any],
    enrichment_artifact_object_key: str,
    input_artifact_object_key: str,
    llm_request_object_key: str,
    llm_request_sha256: str,
    extraction_status: str,
    model: str,
    source_run_id: str,
    extracted_at: str,
) -> dict[str, object]:
    enrichment = _mapping(enrichment_artifact.get("enrichment"), name="enrichment")
    model_metadata = _mapping(enrichment_artifact.get("model"), name="model")
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
        "enrichment_artifact_object_key": enrichment_artifact_object_key,
        "input_artifact_object_key": input_artifact_object_key,
        "llm_request_object_key": llm_request_object_key,
        "llm_request_sha256": llm_request_sha256,
        "llm_response_text": llm_response_text,
        "llm_response_sha256": str(model_metadata.get("raw_response_sha256", "")),
        "model_provider": str(model_metadata.get("provider", "deepseek")),
        "model_name": str(model_metadata.get("name", model)),
        "prompt_version": str(
            enrichment_artifact.get("prompt_version", PROMPT_VERSION)
        ),
        "prompt_tokens": int(model_metadata.get("prompt_tokens") or 0),
        "completion_tokens": int(model_metadata.get("completion_tokens") or 0),
        "input_character_count": int(
            enrichment_artifact.get("input_character_count") or 0
        ),
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
    model: str,
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
        "input_artifact_object_key": str(document["parsed_artifact_object_key"]),
        "llm_request_object_key": "",
        "llm_request_sha256": "",
        "llm_response_text": "",
        "llm_response_sha256": "",
        "model_provider": "deepseek",
        "model_name": model,
        "prompt_version": PROMPT_VERSION,
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
        "country_iso2": str(document["country_iso2"]),
        "company_id": str(document["company_id"]),
        "period_end": str(document["period_end"]),
        "fiscal_year": int(document["fiscal_year"]),
    }


def _replace_information_rows(
    esef_filings_duckdb: DuckDBResource,
    *,
    source_document_ids: Sequence[str],
    model: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    with safe_duckdb_connection(esef_filings_duckdb) as connection:
        ensure_document_company_information_table(connection)
        if not source_document_ids:
            return
        connection.execute("begin transaction")
        try:
            connection.execute(
                "create or replace temp table selected_esef_llm_documents "
                "(source_document_id varchar)"
            )
            connection.executemany(
                "insert into selected_esef_llm_documents values (?)",
                [(source_document_id,) for source_document_id in source_document_ids],
            )
            connection.execute(
                f"delete from {tables.DLT_DATASET_NAME}."
                f"{tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} where "
                "source_document_id in (select source_document_id from "
                "selected_esef_llm_documents) and model_name = ? and prompt_version = ?",
                [model, PROMPT_VERSION],
            )
            if rows:
                columns = tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS
                placeholders = ", ".join("?" for _ in columns)
                connection.executemany(
                    f"insert into {tables.DLT_DATASET_NAME}."
                    f"{tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} "
                    f"({', '.join(columns)}) values "
                    f"({placeholders})",
                    [tuple(row[column] for column in columns) for row in rows],
                )
            connection.execute("commit")
        except Exception:
            connection.execute("rollback")
            raise


def _mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"ESEF {name} must be an object")
    return value


def _json_text(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


@dg.asset(
    name="esef_document_company_information_duckdb",
    deps=[
        dg.AssetDep(
            dg.AssetKey("esef_source_documents_clickhouse"),
            partition_mapping=dg.AllPartitionMapping(),
        )
    ],
    group_name=GROUP_NAME,
    kinds={"python", "s3", "duckdb", "llm", "xbrl", "deepseek"},
    pool="esef_filings_duckdb",
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE},
    description=(
        "Selects the latest final ClickHouse ESEF document per linked company, "
        "archives the exact content-addressed DeepSeek request in S3, and stores "
        "the raw response plus source-document-specific observations in DuckDB. "
        "Canonical company tables are not modified."
    ),
)
def esef_document_company_information_duckdb(
    context: dg.AssetExecutionContext,
    config: EsefLlmEnrichmentConfig,
    esef_filings_duckdb: DuckDBResource,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    settings = deepseek_settings()
    metadata = run_esef_llm_enrichment(
        esef_filings_duckdb=esef_filings_duckdb,
        clickhouse=clickhouse,
        object_store=object_store,
        client=OpenAI(
            base_url=settings.base_url.rstrip("/"),
            api_key=settings.api_key,
            timeout=float(config.timeout_seconds),
            max_retries=2,
        ),
        model=settings.model,
        source_run_id=context.run_id,
        country_iso2=config.country_iso2,
        company_ids=config.company_ids,
        source_document_ids=config.source_document_ids,
        max_documents=config.max_documents,
        refresh_existing=config.refresh_existing,
        max_evidence_chars=config.max_evidence_chars,
        log_info=context.log.info,
        reprocess_existing_without_model=config.reprocess_existing_without_model,
    )
    return dg.MaterializeResult(metadata=metadata)


defs = dg.Definitions(assets=[esef_document_company_information_duckdb])
