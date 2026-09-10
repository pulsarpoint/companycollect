import json

from dagster_v3.defs.esef_filings import llm_enrichment
from dagster_v3.defs.esef_filings.llm_enrichment import (
    EsefLlmResponseError,
    PEOPLE_EVIDENCE_SEGMENTS,
    PEOPLE_PROMPT_VERSION,
    PEOPLE_VISIBLE_SECTION_TYPES,
    build_enrichment_evidence,
    build_people_extraction_request,
    people_extraction_artifact_json_bytes,
    people_extraction_object_key,
    people_request_object_key,
    request_people_extraction,
)
from tests.test_esef_llm_enrichment import _client_returning, _segment_artifact


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
    try:
        request_people_extraction(
            _client_returning({"company_description": None}),
            evidence_input=evidence,
            request_payload=request,
        )
    except EsefLlmResponseError as error:
        assert "people extraction" in str(error)
        assert "company enrichment" not in str(error)
    else:
        raise AssertionError("a response without the people list must be refused")


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
