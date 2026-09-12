import json
from hashlib import sha256
from types import SimpleNamespace
from typing import Any

import httpx
from openai import RateLimitError
from pydantic import ValidationError

from dagster_v3.defs.esef_filings import llm_enrichment, tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.llm_enrichment import (
    EsefLlmResponseError,
    EsefPeopleExtraction,
    PEOPLE_EVIDENCE_SEGMENTS,
    PEOPLE_PROMPT_VERSION,
    PEOPLE_VISIBLE_SECTION_TYPES,
    build_enrichment_evidence,
    build_people_extraction_request,
    enrichment_request_json_bytes,
    people_extraction_artifact_json_bytes,
    people_extraction_object_key,
    people_request_object_key,
    request_people_extraction,
)
from dagster_v3.defs.esef_filings.llm_enrichment_assets import (
    _load_disclosure_artifacts,
    _selection_query,
)
from dagster_v3.defs.esef_filings.people_extraction_assets import (
    EsefPeopleExtractionConfig,
    run_esef_people_extraction,
)
from dagster_v3.defs.esef_filings.segment_assets import ESEF_DOCUMENT_BUCKET
from tests.test_esef_llm_enrichment import (
    _client_returning,
    _FakeClickHouse,
    _FakeObjectStore,
    _segment_artifact,
    _segment_artifact_clickhouse_rows,
    _source_document_clickhouse_row,
    _source_document_mapping,
)


def _people_evidence():
    return build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=64_000,
        evidence_segments=PEOPLE_EVIDENCE_SEGMENTS,
        visible_section_types=PEOPLE_VISIBLE_SECTION_TYPES,
    )


def test_people_evidence_carries_only_the_people_segment() -> None:
    evidence = _people_evidence()
    assert {item.segment for item in evidence.evidence} == {"people_and_audit"}
    full = build_enrichment_evidence(_segment_artifact(), max_evidence_chars=64_000)
    assert len(evidence.evidence) < len(full.evidence)
    assert PEOPLE_VISIBLE_SECTION_TYPES == (
        "board_composition",
        "executive_management",
        "board_committees",
        "auditor_appointment",
        "annual_report_signatures",
        "person_profiles",
    )


def test_people_request_is_json_mode_with_the_people_schema_only() -> None:
    request = build_people_extraction_request(
        _people_evidence(), model="deepseek-v4-flash"
    )
    assert request["response_format"] == {"type": "json_object"}
    assert request["extra_body"] == {"thinking": {"type": "disabled"}}
    system = request["messages"][0]["content"]
    assert (
        "Extract people only when both a person's name and role are explicit." in system
    )
    assert "company description" not in system and "customer markets" not in system
    assert '"people"' in system and '"products_and_services"' not in system
    assert PEOPLE_PROMPT_VERSION == "esef-people-v1"
    try:
        build_people_extraction_request(
            _people_evidence(), model="m", prompt_version="esef-company-enrichment-v2"
        )
    except ValueError as error:
        assert "esef-people-v1" in str(error)
    else:
        raise AssertionError("a foreign prompt version must be refused")


def test_people_response_is_validated_and_citations_normalised() -> None:
    evidence = _people_evidence()
    person_id = evidence.evidence[0].evidence_id
    response = {
        "people": [
            {
                "name": "Anna Svensson",
                "role": "Chief Executive Officer",
                "role_category": "chief_executive",
                "organization": "Example AB",
                "status": "current",
                "effective_from": None,
                "effective_to": None,
                "evidence_ids": [person_id, "E9999"],
                "confidence": 0.9,
            },
            {
                "name": "Nobody",
                "role": "Board member",
                "role_category": "board_member",
                "organization": "Example AB",
                "status": "current",
                "effective_from": None,
                "effective_to": None,
                "evidence_ids": ["E9999"],
                "confidence": 0.5,
            },
        ]
    }
    request = build_people_extraction_request(evidence, model="deepseek-v4-flash")
    result = request_people_extraction(
        _client_returning(response), evidence_input=evidence, request_payload=request
    )
    assert [p.name for p in result.extraction.people] == ["Anna Svensson"]
    assert result.extraction.people[0].evidence_ids == [person_id]
    assert [a.action for a in result.citation_adjustments] == [
        "invalid_evidence_ids_removed",
        "candidate_dropped",
    ]
    assert json.loads(result.raw_response) == response


def test_people_response_without_people_key_is_an_error() -> None:
    evidence = _people_evidence()
    request = build_people_extraction_request(evidence, model="m")
    canned_response = {"company_description": None}
    # Pydantic orders its errors differently for a JSON string than for a dict
    # (extra-field errors sort ahead of missing-field ones under
    # model_validate_json but not under model_validate), so compute the
    # expected summary the same way request_people_extraction does: from the
    # exact JSON text the client returns.
    try:
        EsefPeopleExtraction.model_validate_json(json.dumps(canned_response))
    except ValidationError as validation_error:
        expected_validation_summary = llm_enrichment._validation_summary(
            validation_error
        )
    else:
        raise AssertionError("expected a validation error")

    try:
        request_people_extraction(
            _client_returning(canned_response),
            evidence_input=evidence,
            request_payload=request,
        )
    except EsefLlmResponseError as error:
        assert "people extraction" in str(error)
        assert "company enrichment" not in str(error)
        # The raw model response and a rendered validation summary travel with
        # the error so the caller can archive them for inspection.
        assert error.raw_response == json.dumps(canned_response)
        assert error.validation_summary == expected_validation_summary
    else:
        raise AssertionError("a response without the people list must be refused")


def test_people_response_reports_provider_truncation_with_raw_response() -> None:
    # Mirror test_request_company_enrichment_reports_provider_truncation in
    # test_esef_llm_enrichment.py: a finish_reason="length" completion is a
    # transport-level failure raised by the shared _completion_json, but the
    # truncated content is still available, so it travels as raw_response too
    # -- an invalid_response artifact can archive it just like a
    # schema-validation failure.
    evidence = _people_evidence()
    request = build_people_extraction_request(evidence, model="m")
    truncated_content = '{"people":['
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(
                    id="response-truncated",
                    choices=[
                        SimpleNamespace(
                            finish_reason="length",
                            message=SimpleNamespace(content=truncated_content),
                        )
                    ],
                    usage=SimpleNamespace(
                        prompt_tokens=1_200,
                        completion_tokens=8_000,
                    ),
                )
            )
        )
    )

    try:
        request_people_extraction(
            client,  # type: ignore[arg-type]
            evidence_input=evidence,
            request_payload=request,
        )
    except EsefLlmResponseError as error:
        assert "truncated by the provider" in str(error)
        assert error.raw_response == truncated_content
    else:
        raise AssertionError("a truncated response must be refused")


def test_validation_summary_renders_the_first_error_with_a_remaining_count() -> None:
    try:
        EsefPeopleExtraction.model_validate({"people": [{"name": "Anna"}]})
    except ValidationError as error:
        summary = llm_enrichment._validation_summary(error)
        errors = error.errors()
    else:
        raise AssertionError("expected a validation error")

    assert len(errors) > 1
    first = errors[0]
    expected_loc = ".".join(str(part) for part in first["loc"])
    assert summary.startswith(f"{expected_loc}: {first['msg']}")
    assert summary.endswith(f"(+{len(errors) - 1} more)")
    assert len(summary) <= 300


def test_people_artifact_and_keys_are_versioned() -> None:
    evidence = _people_evidence()
    request = build_people_extraction_request(evidence, model="deepseek-v4-flash")
    result = request_people_extraction(
        _client_returning({"people": []}),
        evidence_input=evidence,
        request_payload=request,
    )
    artifact = json.loads(
        people_extraction_artifact_json_bytes(
            evidence_input=evidence,
            result=result,
            model="deepseek-v4-flash",
            input_artifact_key="clickhouse://x",
            llm_request_object_key="k",
            llm_request_sha256="a" * 64,
            generated_at="2026-09-09T00:00:00Z",
            source_run_id="run",
            provider="deepseek",
            base_url="https://api.deepseek.com",
        )
    )
    assert (
        artifact["schema_version"] == 1
        and artifact["prompt_version"] == "esef-people-v1"
    )
    assert artifact["extraction"] == {"people": []} and "enrichment" not in artifact
    assert artifact["model"]["name"] == "deepseek-v4-flash"
    assert people_request_object_key(
        "b" * 64,
        model="deepseek-v4-flash",
        provider="deepseek",
        prompt_version="esef-people-v1",
    ) == (
        "esef_filings/llm_people_extraction_requests/schema=v1/prompt=esef-people-v1/provider=deepseek/"
        "model=deepseek-v4-flash/request_sha256=" + "b" * 64 + "/request.json"
    )
    assert people_extraction_object_key(
        "c" * 64,
        model="deepseek-v4-flash",
        request_sha256="b" * 64,
        provider="deepseek",
        prompt_version="esef-people-v1",
    ) == (
        "esef_filings/llm_people_extraction/schema=v1/prompt=esef-people-v1/provider=deepseek/"
        "model=deepseek-v4-flash/package_sha256="
        + "c" * 64
        + "/request_sha256="
        + "b" * 64
        + "/artifact.json"
    )


def test_enrichment_defaults_are_untouched() -> None:
    assert llm_enrichment.ENRICHMENT_EVIDENCE_SEGMENTS[0] == "identity"
    assert llm_enrichment.PROMPT_VERSION == "esef-company-enrichment-v2"


def test_people_selection_takes_every_filing_newest_first_and_looks_up_its_own_table() -> (
    None
):
    sql, parameters = _selection_query(
        model="deepseek-v4-flash", provider="deepseek", prompt_version="esef-people-v1",
        link_statuses={"register_verified"}, country_iso2s={"SE"}, company_ids=set(), source_document_ids=set(),
        latest_per_lei=False, existing_table=tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,
        evidence_segments=PEOPLE_EVIDENCE_SEGMENTS, visible_section_types=PEOPLE_VISIBLE_SECTION_TYPES,
    )
    assert "latest_lei_report_rank = 1" not in sql
    assert "FROM corpscout.esef_document_people_extraction" in sql
    assert "ORDER BY documents.period_end DESC, documents.fiscal_year DESC, documents.lei, documents.source_document_id" in sql
    assert parameters["evidence_segments"] == ("people_and_audit",)
    assert parameters["visible_section_types"] == PEOPLE_VISIBLE_SECTION_TYPES
    assert parameters["prompt_version"] == "esef-people-v1"
    # Ruling B (2026-09-12): a filing with a period end after today() must never win a
    # "latest filing" choice -- this pass extracts every filing, so the guard is the only
    # thing keeping the 2029-05-01-dated filing from being processed as if it had happened.
    assert "ifNull(toDate32OrNull(disclosures.period_end) <= today(), false)" in sql
    enrichment_sql, enrichment_parameters = _selection_query(
        model="m", link_statuses={"register_verified"}, country_iso2s=set(), company_ids=set(), source_document_ids=set(),
    )
    assert "latest_lei_report_rank = 1" in enrichment_sql
    assert "FROM corpscout.esef_document_company_information" in enrichment_sql
    assert enrichment_parameters["evidence_segments"][0] == "identity"
    assert "ifNull(toDate32OrNull(disclosures.period_end) <= today(), false)" in enrichment_sql


def test_people_config_requires_provider_and_model() -> None:
    try:
        EsefPeopleExtractionConfig()
    except Exception:
        pass
    else:
        raise AssertionError("a bare config must fail validation")
    config = EsefPeopleExtractionConfig(provider="deepseek", model="deepseek-v4-flash")
    assert config.prompt_version == "esef-people-v1"
    assert config.link_statuses == ["register_verified"]
    assert not hasattr(config, "reprocess_existing_without_model")


def test_people_run_writes_one_extraction_row_per_document() -> None:
    # Mirror test_llm_asset_reads_disclosures_and_writes_clickhouse_directly: one selected
    # document, its disclosure rows and labels, the model answering one person; assert the
    # INSERT into esef_document_people_extraction carries the 21 export columns in order,
    # extraction_status 'extracted', people_json with the person, the people object keys,
    # and that the request and artifact landed under the people prefixes in the fake store.
    package_sha256 = "a" * 64
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    clickhouse = _FakeClickHouse(
        [
            [_source_document_clickhouse_row()],
            disclosure_rows,
            label_rows,
            [(tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,)],
        ]
    )
    object_store = _FakeObjectStore({})
    response = {
        "people": [
            {
                "name": "Anna Andersson",
                "role": "Chief Executive Officer",
                "role_category": "chief_executive",
                "organization": "AAK AB",
                "status": "current",
                "effective_from": None,
                "effective_to": None,
                "evidence_ids": ["E0001"],
                "confidence": 0.98,
            }
        ]
    }

    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=_client_returning(response),
        model="deepseek-v4-flash",
        source_run_id="people-run-1",
        source_document_ids=["AAK-2024"],
        country_iso2s=[],
        link_statuses=["register_verified"],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
    )

    assert metadata["selection_method"] == "every_filing_per_lei"
    assert metadata["candidate_document_count"] == 1
    assert metadata["attempted_document_count"] == 1
    assert metadata["processed_document_count"] == 1
    assert metadata["extracted_document_count"] == 1
    assert metadata["reused_extraction_count"] == 0
    assert metadata["no_evidence_count"] == 0
    assert metadata["failed_document_count"] == 0
    assert metadata["rate_limited_document_count"] == 0
    assert metadata["extraction_row_count"] == 1
    assert metadata["selected_lei_count"] == 1
    assert metadata["person_candidate_count"] == 1
    assert metadata["table"] == tables.QUALIFIED_ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE

    request_keys = [
        key
        for bucket, key in object_store.objects
        if bucket == ESEF_DOCUMENT_BUCKET and key.endswith("/request.json")
    ]
    assert len(request_keys) == 1
    assert request_keys[0].startswith("esef_filings/llm_people_extraction_requests/")
    request_bytes = object_store.objects[(ESEF_DOCUMENT_BUCKET, request_keys[0])]
    request_sha256 = sha256(request_bytes).hexdigest()
    output_key = people_extraction_object_key(
        package_sha256,
        model="deepseek-v4-flash",
        request_sha256=request_sha256,
    )
    assert output_key.startswith("esef_filings/llm_people_extraction/")
    artifact = json.loads(object_store.objects[(ESEF_DOCUMENT_BUCKET, output_key)])
    assert artifact["source_run_id"] == "people-run-1"
    assert len(artifact["extraction"]["people"]) == 1

    people_insert = next(
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_people_extraction" in sql and " VALUES" in sql
    )
    [inserted_values] = people_insert
    inserted = dict(
        zip(
            tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_EXPORT_COLUMNS,
            inserted_values,
            strict=True,
        )
    )
    assert inserted["source_document_id"] == "AAK-2024"
    assert inserted["extraction_status"] == "extracted"
    # The canned response's one person, after the run's own citation
    # normalisation (unchanged here: E0001 is in the people_and_audit
    # segment) -- every field the model returned, not just the name.
    expected_person = {
        "name": "Anna Andersson",
        "role": "Chief Executive Officer",
        "role_category": "chief_executive",
        "organization": "AAK AB",
        "status": "current",
        "effective_from": None,
        "effective_to": None,
        "evidence_ids": ["E0001"],
        "confidence": 0.98,
    }
    assert json.loads(str(inserted["people_json"])) == [expected_person]
    assert inserted["extraction_artifact_object_key"] == output_key
    assert inserted["input_artifact_object_key"].startswith("clickhouse://")
    assert inserted["llm_request_object_key"] == request_keys[0]


def test_people_run_reuses_an_unchanged_request_and_records_no_evidence() -> None:
    # Selection returns a document whose existing_request_sha256 equals the recomputed sha
    # (skipped: no row, no model call) and a document whose artifact has no people evidence
    # (a 'no_evidence' row, no model call). Assert the metadata counts:
    # unchanged_document_count == 1, no_evidence_count == 1, attempted_document_count == 0.
    unchanged_document = _source_document_mapping()
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    lookup_clickhouse = _FakeClickHouse([disclosure_rows, label_rows])
    artifacts = _load_disclosure_artifacts(
        lookup_clickhouse, documents=[unchanged_document]
    )
    evidence = build_enrichment_evidence(
        artifacts["AAK-2024"],
        max_evidence_chars=64_000,
        evidence_segments=PEOPLE_EVIDENCE_SEGMENTS,
        visible_section_types=PEOPLE_VISIBLE_SECTION_TYPES,
    )
    request_payload = build_people_extraction_request(evidence, model="deepseek-v4-flash")
    unchanged_request_sha256 = sha256(
        enrichment_request_json_bytes(request_payload)
    ).hexdigest()

    unchanged_row = (
        "AAK-2024",
        "a" * 64,
        "549300GK4LGIDDWJWL07",
        "2024-12-31",
        2024,
        "https://example.test/aak.zip",
        ARTIFACT_SCHEMA_VERSION,
        unchanged_request_sha256,
        "extracted",
    )
    no_evidence_row = (
        "NOPEOPLE-2024",
        "b" * 64,
        "549300NOPEOPLE000000",
        "2024-12-31",
        2024,
        "https://example.test/nopeople.zip",
        ARTIFACT_SCHEMA_VERSION,
        "",
        "",
    )

    clickhouse = _FakeClickHouse(
        [
            [unchanged_row, no_evidence_row],
            disclosure_rows,
            label_rows,
            [(tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,)],
        ]
    )
    object_store = _FakeObjectStore({})

    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=_client_returning({"people": []}),
        model="deepseek-v4-flash",
        source_run_id="people-run-2",
        source_document_ids=[],
        country_iso2s=[],
        link_statuses=["register_verified"],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
    )

    assert metadata["candidate_document_count"] == 2
    assert metadata["attempted_document_count"] == 0
    assert metadata["unchanged_document_count"] == 1
    assert metadata["no_evidence_count"] == 1
    assert metadata["extracted_document_count"] == 0
    assert metadata["reused_extraction_count"] == 0
    assert metadata["processed_document_count"] == 1
    assert metadata["extraction_row_count"] == 1

    people_insert = next(
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_people_extraction" in sql and " VALUES" in sql
    )
    [inserted_values] = people_insert
    inserted = dict(
        zip(
            tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_EXPORT_COLUMNS,
            inserted_values,
            strict=True,
        )
    )
    assert inserted["source_document_id"] == "NOPEOPLE-2024"
    assert inserted["extraction_status"] == "no_evidence"
    assert inserted["people_json"] == "[]"
    assert inserted["extraction_artifact_object_key"] == ""
    assert inserted["input_artifact_object_key"].startswith("clickhouse://")


def test_people_run_reuses_an_existing_artifact_without_calling_the_model() -> None:
    # Mirror test_people_run_writes_one_extraction_row_per_document, except the exact
    # request already has an output artifact under the people prefix in the fake store:
    # run_esef_people_extraction must mark the row 'reused' and never call the client.
    package_sha256 = "a" * 64
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    lookup_clickhouse = _FakeClickHouse([disclosure_rows, label_rows])
    artifacts = _load_disclosure_artifacts(
        lookup_clickhouse, documents=[_source_document_mapping()]
    )
    evidence = build_enrichment_evidence(
        artifacts["AAK-2024"],
        max_evidence_chars=64_000,
        evidence_segments=PEOPLE_EVIDENCE_SEGMENTS,
        visible_section_types=PEOPLE_VISIBLE_SECTION_TYPES,
    )
    request_payload = build_people_extraction_request(evidence, model="deepseek-v4-flash")
    request_sha256 = sha256(enrichment_request_json_bytes(request_payload)).hexdigest()
    response = {
        "people": [
            {
                "name": "Anna Andersson",
                "role": "Chief Executive Officer",
                "role_category": "chief_executive",
                "organization": "AAK AB",
                "status": "current",
                "effective_from": None,
                "effective_to": None,
                "evidence_ids": ["E0001"],
                "confidence": 0.98,
            }
        ]
    }
    result = request_people_extraction(
        _client_returning(response),
        evidence_input=evidence,
        request_payload=request_payload,
    )
    output_key = people_extraction_object_key(
        package_sha256, model="deepseek-v4-flash", request_sha256=request_sha256
    )
    artifact_bytes = people_extraction_artifact_json_bytes(
        evidence_input=evidence,
        result=result,
        model="deepseek-v4-flash",
        input_artifact_key="clickhouse://prior",
        llm_request_object_key="prior-request-key",
        llm_request_sha256=request_sha256,
        generated_at="2026-09-01T00:00:00Z",
        source_run_id="prior-run",
        provider="deepseek",
        base_url="https://api.deepseek.com",
    )
    object_store = _FakeObjectStore({(ESEF_DOCUMENT_BUCKET, output_key): artifact_bytes})
    clickhouse = _FakeClickHouse(
        [
            [_source_document_clickhouse_row()],
            disclosure_rows,
            label_rows,
            [(tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,)],
        ]
    )

    def _fail(**_kwargs: Any) -> None:
        raise AssertionError("the model must not be called when the artifact is reused")

    client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=_fail))
    )

    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=client,  # type: ignore[arg-type]
        model="deepseek-v4-flash",
        source_run_id="people-run-3",
        source_document_ids=["AAK-2024"],
        country_iso2s=[],
        link_statuses=["register_verified"],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
    )

    assert metadata["attempted_document_count"] == 0
    assert metadata["extracted_document_count"] == 0
    assert metadata["reused_extraction_count"] == 1
    assert metadata["extraction_row_count"] == 1
    assert metadata["failed_document_count"] == 0

    people_insert = next(
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_people_extraction" in sql and " VALUES" in sql
    )
    [inserted_values] = people_insert
    inserted = dict(
        zip(
            tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_EXPORT_COLUMNS,
            inserted_values,
            strict=True,
        )
    )
    assert inserted["source_document_id"] == "AAK-2024"
    assert inserted["extraction_status"] == "reused"
    assert json.loads(str(inserted["people_json"]))[0]["name"] == "Anna Andersson"


def test_people_run_records_a_rate_limited_call_without_inserting_a_row() -> None:
    # Mirror test_people_run_writes_one_extraction_row_per_document: one selected
    # document, but the client raises openai.RateLimitError -- no row is inserted for
    # that document, the run still returns normally, and both failed_document_count
    # and rate_limited_document_count record it.
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    clickhouse = _FakeClickHouse(
        [
            [_source_document_clickhouse_row()],
            disclosure_rows,
            label_rows,
            [(tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,)],
        ]
    )
    object_store = _FakeObjectStore({})
    error = RateLimitError(
        "rate limited: insufficient balance",
        response=httpx.Response(
            429,
            request=httpx.Request(
                "POST", "https://api.deepseek.com/chat/completions"
            ),
        ),
        body=None,
    )

    def _raise(**_kwargs: Any) -> None:
        raise error

    client = SimpleNamespace(
        chat=SimpleNamespace(completions=SimpleNamespace(create=_raise))
    )

    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=client,  # type: ignore[arg-type]
        model="deepseek-v4-flash",
        source_run_id="people-run-4",
        source_document_ids=["AAK-2024"],
        country_iso2s=[],
        link_statuses=["register_verified"],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
    )

    assert metadata["attempted_document_count"] == 1
    assert metadata["failed_document_count"] == 1
    assert metadata["rate_limited_document_count"] == 1
    assert metadata["extracted_document_count"] == 0
    assert metadata["reused_extraction_count"] == 0
    assert metadata["processed_document_count"] == 0
    assert metadata["extraction_row_count"] == 0

    people_inserts = [
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_people_extraction" in sql and " VALUES" in sql
    ]
    assert people_inserts == []


def test_people_run_keeps_the_raw_response_when_validation_fails() -> None:
    # Mirror test_people_run_records_a_rate_limited_call_without_inserting_a_row,
    # except the client returns JSON missing the required "people" key instead of
    # raising: no row is inserted, the failure is counted as both a
    # failed_document and an invalid_response_artifact, and the raw model
    # response is archived in the fake store beside the artifact prefix so it
    # can be inspected -- the failed document is still retried on the next run.
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    clickhouse = _FakeClickHouse(
        [
            [_source_document_clickhouse_row()],
            disclosure_rows,
            label_rows,
            [(tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,)],
        ]
    )
    object_store = _FakeObjectStore({})
    canned_response = {"company_description": None}
    log_lines: list[str] = []

    def _capture_log(message: str, *args: object) -> None:
        log_lines.append(message % args)

    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=_client_returning(canned_response),
        model="deepseek-v4-flash",
        source_run_id="people-run-5",
        source_document_ids=["AAK-2024"],
        country_iso2s=[],
        link_statuses=["register_verified"],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=_capture_log,
    )

    assert metadata["attempted_document_count"] == 1
    assert metadata["failed_document_count"] == 1
    assert metadata["invalid_response_artifact_count"] == 1
    assert metadata["rate_limited_document_count"] == 0
    assert metadata["extracted_document_count"] == 0
    assert metadata["processed_document_count"] == 0
    assert metadata["extraction_row_count"] == 0

    people_inserts = [
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_people_extraction" in sql and " VALUES" in sql
    ]
    assert people_inserts == []

    invalid_response_keys = [
        key
        for bucket, key in object_store.objects
        if bucket == ESEF_DOCUMENT_BUCKET
        and key.startswith("esef_filings/llm_people_extraction/")
        and key.endswith("/invalid_response.json")
    ]
    assert len(invalid_response_keys) == 1
    stored = json.loads(
        object_store.objects[(ESEF_DOCUMENT_BUCKET, invalid_response_keys[0])]
    )
    assert stored["schema_version"] == 1
    assert stored["source_document_id"] == "AAK-2024"
    assert stored["raw_response"] == json.dumps(canned_response)
    assert stored["validation_summary"]
    assert isinstance(stored["request_sha256"], str) and stored["request_sha256"]

    assert any("response kept at" in line for line in log_lines)


class _RaisingOnInvalidResponseObjectStore(_FakeObjectStore):
    """A store whose ``invalid_response.json`` archive write always fails.

    Simulates an object-store outage hitting only the best-effort
    invalid-response archive, so a test can assert that failure never
    propagates out of the outcome loop and stops the rest of the batch.
    """

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        if key.endswith("invalid_response.json"):
            raise RuntimeError("simulated object store outage")
        super().write_bytes(key, body, bucket=bucket)


def test_people_run_survives_a_store_failure_while_archiving_an_invalid_response() -> (
    None
):
    # Two documents in one run: AAK-2024's response fails schema validation
    # and the invalid_response.json archive write itself raises (a simulated
    # object-store outage); AAK-2025's response is valid. The archive write
    # is best-effort and must not propagate out of the outcome loop -- the
    # valid document's row is still inserted, the failed document counts
    # toward failed_document_count but NOT invalid_response_artifact_count
    # (the archive never landed), and the outage is logged.
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    source_document_id_index = tables.ESEF_DISCLOSURES_EXPORT_COLUMNS.index(
        "source_document_id"
    )
    second_disclosure_rows = [
        tuple(
            "AAK-2025" if index == source_document_id_index else value
            for index, value in enumerate(row)
        )
        for row in disclosure_rows
    ]
    second_label_rows = [("AAK-2025", *row[1:]) for row in label_rows]

    first_document_row = _source_document_clickhouse_row()
    second_document_row = (
        "AAK-2025",
        "e" * 64,
        "549300GK4LGIDDWJWL07",
        "2024-12-31",
        2024,
        "https://example.test/aak.zip",
        ARTIFACT_SCHEMA_VERSION,
        "",
        "",
    )
    clickhouse = _FakeClickHouse(
        [
            [first_document_row, second_document_row],
            disclosure_rows + second_disclosure_rows,
            label_rows + second_label_rows,
            [(tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_TABLE,)],
        ]
    )
    object_store = _RaisingOnInvalidResponseObjectStore({})

    invalid_content = json.dumps({"company_description": None})
    valid_content = json.dumps(
        {
            "people": [
                {
                    "name": "Anna Andersson",
                    "role": "Chief Executive Officer",
                    "role_category": "chief_executive",
                    "organization": "AAK AB",
                    "status": "current",
                    "effective_from": None,
                    "effective_to": None,
                    "evidence_ids": ["E0001"],
                    "confidence": 0.98,
                }
            ]
        }
    )
    responses = iter([invalid_content, valid_content])
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(
                    id="response",
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=next(responses)),
                        )
                    ],
                    usage=None,
                )
            )
        )
    )
    log_lines: list[str] = []

    def _capture_log(message: str, *args: object) -> None:
        log_lines.append(message % args)

    metadata = run_esef_people_extraction(
        clickhouse=clickhouse,
        object_store=object_store,
        client=client,  # type: ignore[arg-type]
        model="deepseek-v4-flash",
        source_run_id="people-run-6",
        source_document_ids=["AAK-2024", "AAK-2025"],
        country_iso2s=[],
        link_statuses=["register_verified"],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=_capture_log,
    )

    assert metadata["attempted_document_count"] == 2
    assert metadata["failed_document_count"] == 1
    assert metadata["invalid_response_artifact_count"] == 0
    assert metadata["extracted_document_count"] == 1
    assert metadata["processed_document_count"] == 1
    assert metadata["extraction_row_count"] == 1

    people_insert = next(
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_people_extraction" in sql and " VALUES" in sql
    )
    [inserted_values] = people_insert
    inserted = dict(
        zip(
            tables.ESEF_DOCUMENT_PEOPLE_EXTRACTION_EXPORT_COLUMNS,
            inserted_values,
            strict=True,
        )
    )
    assert inserted["source_document_id"] == "AAK-2025"
    assert inserted["extraction_status"] == "extracted"

    invalid_response_keys = [
        key
        for bucket, key in object_store.objects
        if bucket == ESEF_DOCUMENT_BUCKET and key.endswith("/invalid_response.json")
    ]
    assert invalid_response_keys == []
    assert any(
        "could not keep the invalid response" in line for line in log_lines
    )
