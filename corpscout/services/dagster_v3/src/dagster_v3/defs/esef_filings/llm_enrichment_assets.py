"""Explicit document-oriented ESEF company-information extraction.

The asset selects each linked company's latest parsed report from final
ClickHouse state. Its exact model request is content-addressed in S3, and the
source-document result is written atomically back to ClickHouse. It never
writes a canonical company description, person, contact, or company row.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import json
import uuid
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any
from urllib.parse import quote

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from openai import OpenAI
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.llm_enrichment import (
    ENRICHMENT_EVIDENCE_SEGMENTS,
    ENRICHMENT_VISIBLE_SECTION_TYPES,
    PROMPT_VERSION,
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
    )
    artifacts = _load_disclosure_artifacts(clickhouse, documents=documents)
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
    processed_documents: list[dict[str, object]] = []

    log_info(
        "ESEF LLM latest-company selector: %s source documents considered",
        len(documents),
    )
    for index, document in enumerate(documents, start=1):
        source_document_id = str(document["source_document_id"])
        input_key = _disclosure_input_key(source_document_id)
        segment_artifact = artifacts[source_document_id]
        try:
            evidence_input = build_enrichment_evidence(
                segment_artifact,
                max_evidence_chars=max_evidence_chars,
            )
        except ValueError as exc:
            if str(exc) != "ESEF segment artifact contains no LLM enrichment evidence":
                raise
            if (
                not refresh_existing
                and not reprocess_existing_without_model
                and str(document["existing_extraction_status"]) == "no_evidence"
            ):
                continue
            document["input_artifact_object_key"] = input_key
            information_rows.append(
                _no_evidence_row(
                    document,
                    model=model,
                    source_run_id=source_run_id,
                    extracted_at=extracted_at,
                )
            )
            processed_documents.append(document)
            no_evidence_count += 1
            continue

        package_sha256 = str(document["package_sha256"])
        request_payload = build_company_enrichment_request(
            evidence_input,
            model=model,
        )
        request_bytes = enrichment_request_json_bytes(request_payload)
        request_sha256 = sha256(request_bytes).hexdigest()
        if (
            not refresh_existing
            and not reprocess_existing_without_model
            and str(document["existing_request_sha256"]) == request_sha256
        ):
            continue

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
                input_artifact_key=input_key,
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
        processed_documents.append(document)
        prompt_token_count += int(information_row["prompt_tokens"])
        completion_token_count += int(information_row["completion_tokens"])
        if index == 1 or index % _PROGRESS_INTERVAL == 0 or index == len(documents):
            log_info(
                "ESEF LLM latest-company selector: %s/%s documents processed",
                index,
                len(documents),
            )

    _replace_information_rows_clickhouse(
        clickhouse,
        source_document_ids=[
            str(document["source_document_id"])
            for document in processed_documents
        ],
        model=model,
        rows=information_rows,
    )
    return {
        "selection_method": "latest_xbrl_per_company",
        "selection_country_iso2": clean_country_iso2,
        "llm_model": model,
        "reprocess_existing_without_model": reprocess_existing_without_model,
        "candidate_document_count": len(documents),
        "selected_document_count": len(processed_documents),
        "unchanged_document_count": len(documents) - len(processed_documents),
        "selected_company_count": len(
            {
                (str(document["country_iso2"]), str(document["company_id"]))
                for document in processed_documents
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
        "artifact_schema_version",
    )
    document_filters = [
        "disclosures.package_sha256 != ''",
        "disclosures.company_id != ''",
        "disclosures.country_iso2 != ''",
        "((disclosures.disclosure_kind = 'tagged_fact' AND disclosures.segment IN "
        "%(evidence_segments)s) OR "
        "(disclosures.disclosure_kind = 'visible_section' AND "
        "disclosures.section_type IN %(visible_section_types)s))",
    ]
    parameters: dict[str, object] = {
        "model_name": model,
        "prompt_version": PROMPT_VERSION,
        "evidence_segments": ENRICHMENT_EVIDENCE_SEGMENTS,
        "visible_section_types": ENRICHMENT_VISIBLE_SECTION_TYPES,
    }
    if country_iso2 != "":
        document_filters.append("disclosures.country_iso2 = %(country_iso2)s")
        parameters["country_iso2"] = country_iso2
    if company_ids:
        document_filters.append("disclosures.company_id IN %(company_ids)s")
        parameters["company_ids"] = tuple(sorted(company_ids))

    outer_filters = ["documents.latest_company_report_rank = 1"]
    if source_document_ids:
        outer_filters.append("documents.source_document_id IN %(source_document_ids)s")
        parameters["source_document_ids"] = tuple(sorted(source_document_ids))
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
            PARTITION BY country_iso2, company_id
            ORDER BY period_end DESC, fiscal_year DESC,
                source_processed_at DESC, source_document_id DESC
        ) AS latest_company_report_rank
    FROM (
        SELECT
            disclosures.source_document_id AS source_document_id,
            argMax(disclosures.package_sha256, disclosures.resolved_at)
                AS package_sha256,
            argMax(disclosures.lei, disclosures.resolved_at) AS lei,
            argMax(disclosures.country_iso2, disclosures.resolved_at)
                AS country_iso2,
            argMax(disclosures.company_id, disclosures.resolved_at) AS company_id,
            argMax(toString(disclosures.period_end), disclosures.resolved_at)
                AS period_end,
            argMax(disclosures.fiscal_year, disclosures.resolved_at)
                AS fiscal_year,
            argMax(filings.package_url, filings.processed_at) AS package_url,
            max(disclosures.artifact_schema_version) AS artifact_schema_version,
            toString(max(filings.processed_at)) AS source_processed_at
        FROM {tables.QUALIFIED_ESEF_DISCLOSURES_TABLE} AS disclosures FINAL
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
    FROM {tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE} FINAL
    WHERE model_name = %(model_name)s
      AND prompt_version = %(prompt_version)s
    GROUP BY source_document_id
) AS existing USING (source_document_id)
WHERE {" AND ".join(outer_filters)}
ORDER BY documents.country_iso2, documents.company_id,
    documents.source_document_id
"""
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
FROM {tables.QUALIFIED_ESEF_DISCLOSURES_TABLE} FINAL
WHERE source_document_id IN %(source_document_ids)s
ORDER BY source_document_id, disclosure_kind, report_member,
    anchor_visual_order, source_fact_key, segment, disclosure_id
"""
    label_query = f"""
SELECT
    source_document_id,
    concept_qname,
    language,
    argMax(label, tuple(is_report_language, resolved_at, label_id)) AS label
FROM {tables.QUALIFIED_ESEF_DOCUMENT_CONCEPT_LABELS_TABLE} FINAL
WHERE source_document_id IN %(source_document_ids)s
  AND label != ''
GROUP BY source_document_id, concept_qname, language
ORDER BY source_document_id, concept_qname, language
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
                "Unsupported ESEF disclosure_kind: "
                f"{row['disclosure_kind']}"
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
            "country": str(document["country_iso2"]),
            "company_id": str(document["company_id"]),
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
        "input_artifact_object_key": str(document["input_artifact_object_key"]),
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


def _replace_information_rows_clickhouse(
    clickhouse: ClickhouseResource,
    *,
    source_document_ids: Sequence[str],
    model: str,
    rows: Sequence[Mapping[str, object]],
) -> None:
    if not source_document_ids:
        return
    table = tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE
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
        "model_name": model,
        "prompt_version": PROMPT_VERSION,
    }
    with clickhouse.get_connection() as client:
        client.execute(f"CREATE TABLE {stage} AS {target}")
        try:
            client.execute(
                f"INSERT INTO {stage} SELECT * FROM {target} WHERE NOT ("
                "source_document_id IN %(source_document_ids)s "
                "AND model_name = %(model_name)s "
                "AND prompt_version = %(prompt_version)s)",
                parameters,
            )
            if rows:
                columns = tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS
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
    kinds={"python", "s3", "clickhouse", "llm", "xbrl", "deepseek"},
    pool="esef_document_company_information_clickhouse",
    metadata={"table": tables.QUALIFIED_ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE},
    description=(
        "Selects the latest canonical ESEF disclosures per company, archives "
        "the exact content-addressed DeepSeek request in S3, and atomically "
        "writes source-document observations to ClickHouse."
    ),
)
def esef_document_company_information_clickhouse(
    context: dg.AssetExecutionContext,
    config: EsefLlmEnrichmentConfig,
    clickhouse: ClickhouseResource,
    object_store: ObjectStoreResource,
) -> dg.MaterializeResult:
    settings = deepseek_settings()
    metadata = run_esef_llm_enrichment(
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


defs = dg.Definitions(assets=[esef_document_company_information_clickhouse])
