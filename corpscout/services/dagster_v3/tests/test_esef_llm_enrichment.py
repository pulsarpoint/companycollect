import json
from contextlib import contextmanager
from hashlib import sha256
from types import SimpleNamespace
from typing import Any

import pytest
from dagster import AssetKey

from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.artifact_contract import ARTIFACT_SCHEMA_VERSION
from dagster_v3.defs.esef_filings.disclosure_parser import parse_esef_disclosure
from dagster_v3.defs.esef_filings.llm_enrichment import (
    EsefCompanyEnrichment,
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
from dagster_v3.defs.esef_filings.llm_enrichment_assets import (
    EsefLlmEnrichmentConfig,
    _PreparedEnrichment,
    _load_disclosure_artifacts,
    _load_latest_source_documents,
    _people_with_explicit_roles,
    _request_enrichments,
    build_esef_llm_client,
    esef_document_company_information_clickhouse,
    run_esef_llm_enrichment,
)
from dagster_v3.defs.esef_filings.segment_assets import ESEF_DOCUMENT_BUCKET


def test_build_enrichment_evidence_decodes_text_blocks_and_excludes_numbers() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )

    assert evidence_input.source.company_id == "5566692850"
    assert evidence_input.source.report_period_end == "2024-12-31"
    assert [item.segment for item in evidence_input.evidence] == [
        "identity",
        "business_profile",
        "people_and_audit",
        "products_markets_and_segments",
        "group_structure",
    ]
    assert evidence_input.evidence[1].text == (
        "AAK makes plant-based oils and fats for food, nutrition, "
        "and personal-care products."
    )
    assert "<span" not in evidence_input.evidence[1].text
    assert "IGNORE THIS" not in evidence_input.evidence[1].text
    assert all(item.concept_local_name != "Revenue" for item in evidence_input.evidence)
    assert evidence_input.input_character_count == sum(
        len(item.text) for item in evidence_input.evidence
    )


def test_build_enrichment_evidence_prioritizes_description_and_people() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(long_group_text="Subsidiary " * 2_000),
        max_evidence_chars=800,
    )

    assert {item.segment for item in evidence_input.evidence} >= {
        "identity",
        "business_profile",
        "people_and_audit",
    }
    group_evidence = [
        item for item in evidence_input.evidence if item.segment == "group_structure"
    ]
    assert group_evidence == [] or group_evidence[0].truncated
    assert evidence_input.input_character_count <= 800


def test_build_enrichment_evidence_includes_bounded_visible_people_sections() -> None:
    artifact = _segment_artifact()
    visible_text = "BOARD OF DIRECTORS\nAnna Andersson — Chair\nBo Berg — Board member"
    artifact["visible_sections"] = [
        {
            "section_type": "board_composition",
            "report_member": "reports/aak.xhtml",
            "heading": "BOARD OF DIRECTORS",
            "text": visible_text,
            "page_id": "pf70",
            "printed_page_number": "70",
            "anchor_xpath": "/html/body/div/div[1]",
            "anchor_visual_order": 1,
            "extraction_method": "positioned_page",
            "language": "en",
            "original_character_count": len(visible_text),
            "included_character_count": len(visible_text),
            "truncated": False,
            "text_sha256": sha256(visible_text.encode()).hexdigest(),
        }
    ]

    evidence_input = build_enrichment_evidence(
        artifact,
        max_evidence_chars=20_000,
    )

    visible = evidence_input.evidence[0]
    assert visible.evidence_kind == "visible_section"
    assert visible.segment == "people_and_audit"
    assert visible.text == visible_text
    assert visible.model_dump()["section_type"] == "board_composition"
    assert visible.model_dump()["printed_page_number"] == "70"
    assert any(item.evidence_kind == "tagged_fact" for item in evidence_input.evidence)
    assert evidence_input.input_character_count <= 20_000


def test_build_enrichment_evidence_validates_visible_section_hash() -> None:
    artifact = _segment_artifact()
    visible_text = "Anna Andersson — Chair"
    artifact["visible_sections"] = [
        {
            "section_type": "board_composition",
            "report_member": "reports/aak.xhtml",
            "heading": "Board",
            "text": visible_text,
            "page_id": "pf70",
            "printed_page_number": "70",
            "anchor_xpath": "/html/body/div/div[1]",
            "anchor_visual_order": 1,
            "extraction_method": "positioned_page",
            "language": "en",
            "original_character_count": len(visible_text),
            "included_character_count": len(visible_text),
            "truncated": False,
            "text_sha256": "0" * 64,
        }
    ]

    with pytest.raises(ValueError, match="text SHA-256 does not match"):
        build_enrichment_evidence(artifact, max_evidence_chars=20_000)


def test_build_enrichment_evidence_accepts_v3_and_preserves_source_version() -> None:
    artifact = _segment_artifact()
    artifact["schema_version"] = 3
    artifact.pop("visible_sections")

    evidence_input = build_enrichment_evidence(
        artifact,
        max_evidence_chars=20_000,
    )

    assert SUPPORTED_ENRICHMENT_ARTIFACT_SCHEMA_VERSIONS == (3, 4, 5)
    assert evidence_input.source.segment_artifact_schema_version == 3


@pytest.mark.parametrize("schema_version", [2, 6, "5", 4.0, True, None])
def test_build_enrichment_evidence_rejects_unsupported_schema_versions(
    schema_version: object,
) -> None:
    artifact = _segment_artifact()
    artifact["schema_version"] = schema_version

    with pytest.raises(
        ValueError,
        match=r"expected one of \[3, 4, 5\]",
    ):
        build_enrichment_evidence(artifact, max_evidence_chars=20_000)


def test_request_company_enrichment_uses_deepseek_json_mode_and_validates_evidence() -> (
    None
):
    captured_request: dict[str, Any] = {}
    response_data = _valid_response()

    def create_completion(**kwargs: Any) -> SimpleNamespace:
        captured_request.update(kwargs)
        return SimpleNamespace(
            id="response-1",
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=json.dumps(response_data)),
                )
            ],
            usage=SimpleNamespace(prompt_tokens=1_200, completion_tokens=450),
        )

    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(create=create_completion),
        )
    )
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )

    request_payload = build_company_enrichment_request(
        evidence_input,
        model="deepseek-v4-flash",
    )
    result = request_company_enrichment(
        client,
        evidence_input=evidence_input,
        request_payload=request_payload,
    )

    assert result.enrichment == EsefCompanyEnrichment.model_validate(response_data)
    assert result.response_id == "response-1"
    assert result.prompt_tokens == 1_200
    assert result.completion_tokens == 450
    assert captured_request["response_format"] == {"type": "json_object"}
    assert captured_request["extra_body"] == {"thinking": {"type": "disabled"}}
    assert captured_request["temperature"] == 0
    assert "Do not infer" in captured_request["messages"][0]["content"]
    assert "never customer markets" in captured_request["messages"][0]["content"]
    assert "is not a role" in captured_request["messages"][0]["content"]
    evidence_payload = json.loads(captured_request["messages"][1]["content"])
    assert evidence_payload["evidence"][1]["evidence_id"] == "E0002"
    assert "Revenue" not in captured_request["messages"][1]["content"]
    assert captured_request == request_payload


def test_request_profile_controls_openai_compatible_request() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )

    request = build_company_enrichment_request(
        evidence_input,
        provider="fireworks",
        model="accounts/example/models/custom",
        temperature=0.35,
    )

    assert request["model"] == "accounts/example/models/custom"
    assert request["temperature"] == 0.35
    assert "max_tokens" not in request
    assert "max_completion_tokens" not in request
    assert "extra_body" not in request
    request_sha256 = sha256(enrichment_request_json_bytes(request)).hexdigest()
    assert "/provider=fireworks/model=" in enrichment_request_object_key(
        request_sha256,
        provider="fireworks",
        model="accounts/example/models/custom",
    )


def test_request_company_enrichment_reports_provider_truncation() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(
                    id="response-truncated",
                    choices=[
                        SimpleNamespace(
                            finish_reason="length",
                            message=SimpleNamespace(content='{"people":['),
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

    with pytest.raises(
        ValueError,
        match=(
            "truncated by the provider .*client_output_token_limit=none, "
            "completion_tokens=8000"
        ),
    ):
        request_company_enrichment(
            client,
            evidence_input=evidence_input,
            request_payload=build_company_enrichment_request(
                evidence_input,
                model="stealth/ox-alpha",
                provider="ox alpha",
            ),
        )


def test_request_company_enrichment_drops_candidate_with_unknown_evidence() -> None:
    response_data = _valid_response()
    response_data["company_description"]["evidence_ids"] = ["E9999"]
    client = _client_returning(response_data)

    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )
    result = request_company_enrichment(
        client,
        evidence_input=evidence_input,
        request_payload=build_company_enrichment_request(
            evidence_input,
            model="deepseek-v4-flash",
        ),
    )

    assert result.enrichment.company_description is None
    assert [item.model_dump() for item in result.citation_adjustments] == [
        {
            "candidate_type": "company description",
            "candidate_index": 0,
            "rejected_evidence_ids": ["E9999"],
            "retained_evidence_ids": [],
            "action": "candidate_dropped",
        }
    ]


def test_request_company_enrichment_removes_unrelated_person_citation() -> None:
    response_data = _valid_response()
    response_data["people"][0]["evidence_ids"] = ["E0002", "E0003"]
    client = _client_returning(response_data)
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )

    result = request_company_enrichment(
        client,
        evidence_input=evidence_input,
        request_payload=build_company_enrichment_request(
            evidence_input,
            model="deepseek-v4-flash",
        ),
    )

    assert result.enrichment.people[0].evidence_ids == ["E0003"]
    assert result.citation_adjustments[0].action == "invalid_evidence_ids_removed"


def test_request_company_enrichment_drops_person_cited_to_description() -> None:
    response_data = _valid_response()
    response_data["people"][0]["evidence_ids"] = ["E0002"]
    client = _client_returning(response_data)
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )

    result = request_company_enrichment(
        client,
        evidence_input=evidence_input,
        request_payload=build_company_enrichment_request(
            evidence_input,
            model="deepseek-v4-flash",
        ),
    )

    assert [person.name for person in result.enrichment.people] == ["Example Executive"]
    assert result.citation_adjustments[0].action == "candidate_dropped"


def test_request_company_enrichment_preserves_exact_model_response_text() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )
    response_text = f"```json\n{json.dumps(_valid_response())}\n```"
    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(
                    id="response-1",
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=response_text),
                        )
                    ],
                    usage=None,
                )
            )
        )
    )

    result = request_company_enrichment(
        client,
        evidence_input=evidence_input,
        request_payload=build_company_enrichment_request(
            evidence_input,
            model="deepseek-v4-flash",
        ),
    )

    assert result.raw_response == response_text
    assert result.enrichment == EsefCompanyEnrichment.model_validate(_valid_response())


def test_enrichment_artifact_is_versioned_and_auditable() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )
    request_payload = build_company_enrichment_request(
        evidence_input,
        model="deepseek-v4-flash",
    )
    result = request_company_enrichment(
        _client_returning(_valid_response()),
        evidence_input=evidence_input,
        request_payload=request_payload,
    )
    request_bytes = enrichment_request_json_bytes(request_payload)
    request_sha256 = sha256(request_bytes).hexdigest()
    request_key = enrichment_request_object_key(
        request_sha256,
        model="deepseek-v4-flash",
    )

    body = enrichment_artifact_json_bytes(
        evidence_input=evidence_input,
        result=result,
        model="deepseek-v4-flash",
        input_artifact_key="s3://source-esef-filings/input/artifact.json",
        llm_request_object_key=request_key,
        llm_request_sha256=request_sha256,
        generated_at="2026-08-01T12:00:00Z",
        source_run_id="run-1",
    )
    artifact = json.loads(body)

    assert artifact["schema_version"] == 2
    assert artifact["prompt_version"] == "esef-company-enrichment-v2"
    assert artifact["source"]["package_sha256"] == "a" * 64
    assert artifact["model"]["name"] == "deepseek-v4-flash"
    assert artifact["model"]["provider"] == "deepseek"
    assert artifact["model"]["temperature"] == 0
    assert "max_tokens" not in artifact["model"]
    assert artifact["model"]["finish_reason"] == ""
    assert artifact["model"]["raw_response_sha256"]
    assert artifact["model"]["raw_response"] == result.raw_response
    assert artifact["validation"] == {
        "citation_policy": "retain_only_directly_supported_candidates",
        "citation_adjustments": [],
    }
    assert artifact["request"]["object_key"] == request_key
    assert artifact["request"]["sha256"] == request_sha256
    assert artifact["enrichment"]["people"][0]["name"] == "Anna Andersson"
    assert artifact["evidence"][2]["fact_key"] == "people-fact"
    assert enrichment_object_key(
        "a" * 64,
        model="deepseek-v4-flash",
        request_sha256=request_sha256,
    ) == (
        "esef_filings/llm_company_enrichment/schema=v2/"
        "prompt=esef-company-enrichment-v2/model=deepseek-v4-flash/"
        f"package_sha256={'a' * 64}/request_sha256={request_sha256}/artifact.json"
    )
    assert json.loads(request_bytes) == request_payload
    assert request_key.endswith(f"request_sha256={request_sha256}/request.json")


def test_deepseek_settings_use_existing_repository_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DEEPSEEK_URL", "https://api.deepseek.example")
    monkeypatch.setenv("DEEPSEEK_MODEL", "deepseek-v4-flash")
    monkeypatch.setenv("DEEPSEEK_API_KEY", "test-key")

    settings = deepseek_settings()

    assert settings.base_url == "https://api.deepseek.example"
    assert settings.model == "deepseek-v4-flash"
    assert settings.api_key == "test-key"
    # The provider label rides with the endpoint so a DeepSeek-compatible host
    # that is not DeepSeek is not recorded as one.
    assert settings.provider == "deepseek"
    monkeypatch.setenv("DEEPSEEK_PROVIDER", "  fireworks  ")
    assert deepseek_settings().provider == "fireworks"
    monkeypatch.setenv("DEEPSEEK_PROVIDER", "   ")
    assert deepseek_settings().provider == "deepseek"
    monkeypatch.delenv("DEEPSEEK_API_KEY")
    with pytest.raises(RuntimeError, match="DEEPSEEK_API_KEY"):
        deepseek_settings()


def test_esef_runtime_profile_uses_only_host_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = EsefLlmEnrichmentConfig(
        provider="ox alpha",
        model="stealth/ox-alpha",
        base_url="https://openrouter.ai/api/v1/",
        api_key_environment_variable="OPENROUTER_API",
        temperature=0.25,
        prompt_version="esef-company-enrichment-v2",
        concurrency=3,
        company_ids=["5566692850"],
        source_document_ids=["AAK-2024"],
        max_documents=1,
        refresh_existing=True,
        max_evidence_chars=10_000,
        timeout_seconds=45,
    )

    assert config.model_dump() == {
        "provider": "ox alpha",
        "model": "stealth/ox-alpha",
        "base_url": "https://openrouter.ai/api/v1/",
        "api_key_environment_variable": "OPENROUTER_API",
        "temperature": 0.25,
        "prompt_version": "esef-company-enrichment-v2",
        "concurrency": 3,
        "country_iso2s": [],
        "company_ids": ["5566692850"],
        "source_document_ids": ["AAK-2024"],
        "max_documents": 1,
        "refresh_existing": True,
        "reprocess_existing_without_model": False,
        "max_evidence_chars": 10_000,
        "timeout_seconds": 45,
    }
    assert "api_key" not in config.model_dump()
    monkeypatch.setenv("OPENROUTER_API", "test-key")
    client = build_esef_llm_client(config)
    assert str(client.base_url).rstrip("/") == "https://openrouter.ai/api/v1"

    monkeypatch.delenv("OPENROUTER_API")
    with pytest.raises(ValueError, match="OPENROUTER_API"):
        build_esef_llm_client(config)


def test_esef_runtime_rejects_prompt_version_not_deployed_here() -> None:
    config = EsefLlmEnrichmentConfig(prompt_version="esef-company-enrichment-v99")

    with pytest.raises(ValueError, match="this deployment provides"):
        build_esef_llm_client(config)


def test_esef_model_calls_honor_bounded_concurrency() -> None:
    import threading

    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )
    request = build_company_enrichment_request(
        evidence_input,
        model="test-model",
    )
    barrier = threading.Barrier(2, timeout=10)
    lock = threading.Lock()
    in_flight = 0
    max_in_flight = 0

    def create_completion(**_kwargs: Any) -> SimpleNamespace:
        nonlocal in_flight, max_in_flight
        with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        barrier.wait()
        with lock:
            in_flight -= 1
        return SimpleNamespace(
            id="response",
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content=json.dumps(_valid_response()))
                )
            ],
            usage=None,
        )

    client = SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(create=create_completion),
        )
    )
    work = [
        _PreparedEnrichment(
            document={"source_document_id": source_document_id},
            input_key=f"input/{source_document_id}",
            evidence_input=evidence_input,
            request_payload=request,
            request_key=f"request/{source_document_id}",
            request_sha256="a" * 64,
            output_key=f"output/{source_document_id}",
        )
        for source_document_id in ("document-1", "document-2")
    ]

    results = list(
        _request_enrichments(
            client=client,  # type: ignore[arg-type]
            work=work,
            concurrency=2,
        )
    )

    assert len(results) == 2
    assert max_in_flight == 2


def test_esef_model_response_failure_does_not_stop_later_documents() -> None:
    evidence_input = build_enrichment_evidence(
        _segment_artifact(),
        max_evidence_chars=20_000,
    )
    request = build_company_enrichment_request(
        evidence_input,
        model="test-model",
    )
    responses = iter([None, json.dumps(_valid_response())])
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
    work = [
        _PreparedEnrichment(
            document={"source_document_id": source_document_id},
            input_key=f"input/{source_document_id}",
            evidence_input=evidence_input,
            request_payload=request,
            request_key=f"request/{source_document_id}",
            request_sha256="a" * 64,
            output_key=f"output/{source_document_id}",
        )
        for source_document_id in ("document-1", "document-2")
    ]
    progress: list[str] = []

    outcomes = list(
        _request_enrichments(
            client=client,  # type: ignore[arg-type]
            work=work,
            concurrency=1,
            log_info=lambda message, *args: progress.append(message % args),
        )
    )

    assert outcomes[0].result is None
    assert outcomes[0].failure_kind == "invalid_response"
    assert outcomes[1].result is not None
    assert outcomes[1].failure_kind is None
    assert progress == [
        "ESEF LLM request starting: 1/2",
        "ESEF LLM request starting: 2/2",
    ]


def test_llm_enrichment_asset_depends_on_final_clickhouse_documents() -> None:
    output_key = AssetKey("esef_document_company_information_clickhouse")

    assert esef_document_company_information_clickhouse.asset_deps[output_key] == {
        AssetKey("esef_disclosures_clickhouse"),
        AssetKey("esef_document_concept_labels_clickhouse"),
        AssetKey("esef_filings_clickhouse"),
    }
    assert (
        esef_document_company_information_clickhouse.group_names_by_key[output_key]
        == "esef"
    )
    retry_policy = esef_document_company_information_clickhouse.op.retry_policy
    assert retry_policy is not None
    assert retry_policy.max_retries == 3


def test_latest_document_selector_uses_one_final_xbrl_per_company() -> None:
    clickhouse = _FakeClickHouse([[_source_document_clickhouse_row()]])

    documents = _load_latest_source_documents(
        clickhouse,
        model="deepseek-v4-flash",
        country_iso2s={"SE", "FI"},
        company_ids={"5566692850"},
        source_document_ids=set(),
        max_documents=10,
    )

    assert [document["source_document_id"] for document in documents] == ["AAK-2024"]
    sql, parameters = clickhouse.client.calls[0]
    assert "row_number() OVER" in sql
    assert "PARTITION BY country_iso2, company_id" in sql
    assert "latest_company_report_rank = 1" in sql
    assert "corpscout.esef_filings AS filings FINAL" in sql
    assert "FROM corpscout.esef_disclosures AS disclosures" in sql
    assert "corpscout.esef_disclosures AS disclosures FINAL" not in sql
    assert "FROM corpscout.esef_document_company_information FINAL" not in sql
    assert "disclosures.segment IN" in sql
    assert "esef_document_company_information" in sql
    assert "esef_source_documents" not in sql
    assert parameters["model_name"] == "deepseek-v4-flash"
    assert parameters["model_provider"] == "deepseek"
    assert parameters["prompt_version"] == "esef-company-enrichment-v2"
    assert "disclosures.country_iso2 IN %(country_iso2s)s" in sql
    assert parameters["country_iso2s"] == ("FI", "SE")
    assert parameters["company_ids"] == ("5566692850",)
    assert documents[0]["artifact_schema_version"] == ARTIFACT_SCHEMA_VERSION


def test_latest_document_selector_requires_resolved_company_links() -> None:
    clickhouse = _FakeClickHouse([[]])

    assert (
        _load_latest_source_documents(
            clickhouse,
            model="deepseek-v4-flash",
            country_iso2s=set(),
            company_ids=set(),
            source_document_ids=set(),
            max_documents=None,
        )
        == []
    )
    sql, _parameters = clickhouse.client.calls[0]
    assert "disclosures.company_id != ''" in sql
    assert "disclosures.country_iso2 != ''" in sql


def test_clickhouse_disclosures_reconstruct_llm_evidence() -> None:
    document = _source_document_mapping()
    clickhouse = _FakeClickHouse(
        [
            [_disclosure_clickhouse_row()],
            [("AAK-2024", "sample:Description", "en", "Company description")],
        ]
    )

    artifacts = _load_disclosure_artifacts(clickhouse, documents=[document])
    evidence_input = build_enrichment_evidence(
        artifacts["AAK-2024"],
        max_evidence_chars=20_000,
    )

    [evidence] = evidence_input.evidence
    assert evidence.segment == "business_profile"
    assert evidence.concept_label == "Company description"
    assert evidence.text == "AAK makes plant-based oils and fats."
    assert evidence_input.source.source_object_key.startswith(
        "clickhouse://corpscout.esef_disclosures/"
    )
    disclosure_sql, _parameters = clickhouse.client.calls[0]
    label_sql, _parameters = clickhouse.client.calls[1]
    assert "FROM corpscout.esef_disclosures FINAL" not in disclosure_sql
    assert "FROM corpscout.esef_document_concept_labels FINAL" not in label_sql
    assert "AND concept_labels.label != ''" in label_sql
    assert ") AS resolved_label" in label_sql


def test_company_id_selector_requires_one_country_identity_boundary() -> None:
    with pytest.raises(ValueError, match="company_ids require exactly one"):
        run_esef_llm_enrichment(
            clickhouse=None,  # type: ignore[arg-type]
            object_store=None,
            client=None,  # type: ignore[arg-type]
            model="deepseek-v4-flash",
            source_run_id="llm-run",
            country_iso2s=[],
            company_ids=["5566692850"],
            source_document_ids=[],
            max_documents=None,
            refresh_existing=False,
            max_evidence_chars=64_000,
            log_info=lambda *_args: None,
        )


def test_llm_reprocessing_modes_are_mutually_exclusive() -> None:
    with pytest.raises(ValueError, match="cannot both be enabled"):
        run_esef_llm_enrichment(
            clickhouse=None,  # type: ignore[arg-type]
            object_store=None,
            client=None,  # type: ignore[arg-type]
            model="deepseek-v4-flash",
            source_run_id="llm-run",
            country_iso2s=[],
            company_ids=[],
            source_document_ids=[],
            max_documents=None,
            refresh_existing=True,
            max_evidence_chars=64_000,
            log_info=lambda *_args: None,
            reprocess_existing_without_model=True,
        )


def test_people_filter_rejects_group_name_used_as_its_own_role() -> None:
    people = [
        {
            "name": "Board of Directors",
            "role": "Board of Directors",
        },
        {
            "name": "Anna Andersson",
            "role": "Board Chair",
        },
    ]

    assert _people_with_explicit_roles(people) == [people[1]]


def test_llm_asset_reads_disclosures_and_writes_clickhouse_directly() -> None:
    package_sha256 = "a" * 64
    disclosure_rows, label_rows = _segment_artifact_clickhouse_rows()
    clickhouse = _FakeClickHouse(
        [
            [_source_document_clickhouse_row()],
            disclosure_rows,
            label_rows,
            [(tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE,)],
        ]
    )
    object_store = _FakeObjectStore({})

    metadata = run_esef_llm_enrichment(
        clickhouse=clickhouse,
        object_store=object_store,
        client=_client_returning(_valid_response()),
        model="deepseek-v4-flash",
        source_run_id="llm-run-1",
        source_document_ids=["AAK-2024"],
        country_iso2s=[],
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
    )

    assert metadata["enriched_document_count"] == 1
    assert metadata["attempted_document_count"] == 1
    assert metadata["processed_document_count"] == 1
    assert metadata["failed_document_count"] == 0
    assert metadata["rate_limited_document_count"] == 0
    assert metadata["information_row_count"] == 1
    assert metadata["selected_company_count"] == 1
    assert metadata["llm_provider"] == "deepseek"
    assert metadata["llm_temperature"] == 0
    assert "llm_max_tokens" not in metadata
    assert metadata["llm_prompt_version"] == "esef-company-enrichment-v2"
    assert metadata["llm_concurrency"] == 1
    request_keys = [
        key
        for bucket, key in object_store.objects
        if bucket == ESEF_DOCUMENT_BUCKET and key.endswith("/request.json")
    ]
    assert len(request_keys) == 1
    request_bytes = object_store.objects[(ESEF_DOCUMENT_BUCKET, request_keys[0])]
    request_sha256 = sha256(request_bytes).hexdigest()
    output_key = enrichment_object_key(
        package_sha256,
        model="deepseek-v4-flash",
        request_sha256=request_sha256,
    )
    artifact = json.loads(object_store.objects[(ESEF_DOCUMENT_BUCKET, output_key)])
    assert artifact["source_run_id"] == "llm-run-1"
    assert artifact["source"]["input_artifact_key"].startswith(
        "clickhouse://corpscout.esef_disclosures/"
    )
    assert artifact["enrichment"]["company_description"]["language"] == "en"
    assert len(artifact["enrichment"]["people"]) == 2
    assert artifact["model"]["base_url"] == "https://api.deepseek.com"
    assert object_store.created_buckets == [ESEF_DOCUMENT_BUCKET]
    information_insert = next(
        parameters
        for sql, parameters in clickhouse.client.calls
        if "esef_document_company_information" in sql and " VALUES" in sql
    )
    [inserted_values] = information_insert
    inserted = dict(
        zip(
            tables.ESEF_DOCUMENT_COMPANY_INFORMATION_EXPORT_COLUMNS,
            inserted_values,
            strict=True,
        )
    )
    assert inserted["source_document_id"] == "AAK-2024"
    assert inserted["input_artifact_object_key"].startswith("clickhouse://")
    assert json.loads(str(inserted["people_json"]))[0]["name"] == "Anna Andersson"


def _source_document_clickhouse_row() -> tuple[object, ...]:
    return (
        "AAK-2024",
        "a" * 64,
        "549300GK4LGIDDWJWL07",
        "SE",
        "5566692850",
        "2024-12-31",
        2024,
        "https://example.test/aak.zip",
        ARTIFACT_SCHEMA_VERSION,
        "",
        "",
    )


def _source_document_mapping() -> dict[str, object]:
    values = _source_document_clickhouse_row()
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
        "existing_request_sha256",
        "existing_extraction_status",
    )
    return dict(zip(columns, values, strict=True))


def _disclosure_clickhouse_row(
    *,
    fact_key: str = "description-fact",
    concept_qname: str = "sample:Description",
    concept_local_name: str = "Description",
    segment: str = "business_profile",
    selection_reason: str = "concept:Description",
    plain_text: str = "AAK makes plant-based oils and fats.",
) -> tuple[object, ...]:
    values: dict[str, object] = {
        "disclosure_id": sha256(fact_key.encode()).hexdigest(),
        "disclosure_kind": "tagged_fact",
        "source_document_id": "AAK-2024",
        "source_record_uid": "b" * 64,
        "source_fact_id": fact_key,
        "source_fact_key": fact_key,
        "package_sha256": "a" * 64,
        "artifact_schema_version": ARTIFACT_SCHEMA_VERSION,
        "lei": "549300GK4LGIDDWJWL07",
        "country_iso2": "SE",
        "company_id": "5566692850",
        "period_end": "2024-12-31",
        "fiscal_year": 2024,
        "concept_qname": concept_qname,
        "concept_local_name": concept_local_name,
        "language": "en",
        "segment": segment,
        "selection_reason": selection_reason,
        "report_member": "reports/aak.xhtml",
        "period_json": '{"end":"2024-12-31","start":"2024-01-01"}',
        "section_type": "",
        "page_id": "",
        "printed_page_number": "",
        "anchor_xpath": "",
        "anchor_visual_order": 0,
        "extraction_method": "tagged_fact",
        "text_sha256": sha256(plain_text.encode()).hexdigest(),
        "parser_name": "lxml_html_disclosure",
        "parser_version": "1",
        "blocks_json": "[]",
        "plain_text": plain_text,
        "original_character_count": len(plain_text),
        "block_count": 1,
        "table_count": 0,
        "source_run_id": "parse-run",
        "extracted_at": "2026-08-01T10:00:00Z",
    }
    return tuple(values[column] for column in tables.ESEF_DISCLOSURES_EXPORT_COLUMNS)


def _segment_artifact_clickhouse_rows() -> tuple[
    list[tuple[object, ...]], list[tuple[object, ...]]
]:
    artifact = _segment_artifact()
    facts = artifact["facts"]
    concepts = artifact["concepts"]
    disclosure_rows: list[tuple[object, ...]] = []
    label_rows: list[tuple[object, ...]] = []
    for segment, references in artifact["segments"].items():
        if segment == "financial_highlights":
            continue
        for reference in references:
            fact_key = reference["fact_key"]
            fact = facts[fact_key]
            concept_qname = fact["concept_qname"]
            concept = concepts[concept_qname]
            plain_text = parse_esef_disclosure(fact["canonical_value"]).plain_text
            disclosure_rows.append(
                _disclosure_clickhouse_row(
                    fact_key=fact_key,
                    concept_qname=concept_qname,
                    concept_local_name=concept["local_name"],
                    segment=segment,
                    selection_reason=reference["selection_reason"],
                    plain_text=plain_text,
                )
            )
            label_rows.append(
                (
                    "AAK-2024",
                    concept_qname,
                    "en",
                    concept["labels"]["en"],
                )
            )
    return disclosure_rows, label_rows


def _client_returning(response_data: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(
        chat=SimpleNamespace(
            completions=SimpleNamespace(
                create=lambda **_kwargs: SimpleNamespace(
                    id="response-1",
                    choices=[
                        SimpleNamespace(
                            message=SimpleNamespace(content=json.dumps(response_data)),
                        )
                    ],
                    usage=None,
                )
            )
        )
    )


def _valid_response() -> dict[str, Any]:
    return {
        "company_description": {
            "description": (
                "AAK develops plant-based oils and fats used in food, nutrition, "
                "and personal-care products."
            ),
            "language": "en",
            "evidence_ids": ["E0002"],
            "confidence": 0.99,
        },
        "people": [
            {
                "name": "Anna Andersson",
                "role": "Chief Executive Officer",
                "role_category": "chief_executive",
                "organization": "AAK AB",
                "status": "current",
                "effective_from": None,
                "effective_to": None,
                "evidence_ids": ["E0003"],
                "confidence": 0.98,
            },
            {
                "name": "Example Executive",
                "role": "Other Key Management Personnel",
                "role_category": "executive",
                "organization": "AAK AB",
                "status": "current",
                "effective_from": None,
                "effective_to": None,
                "evidence_ids": ["E0003"],
                "confidence": 0.9,
            },
        ],
        "products_and_services": [
            {
                "name": "Plant-based oils and fats",
                "evidence_ids": ["E0002", "E0004"],
                "confidence": 0.99,
            }
        ],
        "customer_markets": [
            {
                "name": "Food manufacturing",
                "evidence_ids": ["E0002"],
                "confidence": 0.9,
            }
        ],
        "operating_geographies": [
            {
                "name": "Sweden",
                "geography_type": "country",
                "evidence_ids": ["E0001"],
                "confidence": 0.9,
            }
        ],
        "business_segments": [
            {
                "name": "Food Ingredients",
                "evidence_ids": ["E0004"],
                "confidence": 0.98,
            }
        ],
        "material_group_relationships": [
            {
                "related_company_name": "AAK Sweden AB",
                "relationship_type": "subsidiary",
                "ownership_percentage": 100.0,
                "jurisdiction": "Sweden",
                "evidence_ids": ["E0005"],
                "confidence": 0.96,
            }
        ],
    }


def _segment_artifact(*, long_group_text: str = "") -> dict[str, Any]:
    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "package_sha256": "a" * 64,
        "source": {
            "fxo_id": "AAK-2024",
            "country": "SE",
            "source_url": "https://example.test/aak.zip",
            "object_key": "s3://source/aak.zip",
            "company_id": "5566692850",
            "source_run_id": "parse-run",
            "expected_package_sha256": "a" * 64,
        },
        "concepts": {
            "sample:Name": {
                "local_name": "NameOfReportingEntityOrOtherMeansOfIdentification",
                "labels": {"en": "Name of reporting entity"},
            },
            "sample:Description": {
                "local_name": "DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities",
                "labels": {"en": "Description of operations"},
            },
            "sample:People": {
                "local_name": "DisclosureOfInformationAboutKeyManagementPersonnelExplanatory",
                "labels": {"en": "Key management personnel"},
            },
            "sample:Segments": {
                "local_name": "DisclosureOfEntitysReportableSegmentsExplanatory",
                "labels": {"en": "Reportable segments"},
            },
            "sample:Subsidiaries": {
                "local_name": "DisclosureOfSignificantInvestmentsInSubsidiariesExplanatory",
                "labels": {"en": "Subsidiaries"},
            },
            "sample:Revenue": {
                "local_name": "Revenue",
                "labels": {"en": "Revenue"},
            },
        },
        "facts": {
            "identity-fact": _fact(
                "identity-fact",
                concept_qname="sample:Name",
                value="AAK AB",
            ),
            "description-fact": _fact(
                "description-fact",
                concept_qname="sample:Description",
                value=(
                    "<script>IGNORE THIS</script><p>AAK makes plant-based "
                    "<span>oils and fats</span> for food, "
                    "nutrition, and personal-care products.</p>"
                ),
            ),
            "people-fact": _fact(
                "people-fact",
                concept_qname="sample:People",
                value="Anna Andersson — Chief Executive Officer.",
            ),
            "segments-fact": _fact(
                "segments-fact",
                concept_qname="sample:Segments",
                value="Food Ingredients and Chocolate & Confectionery Fats.",
            ),
            "group-fact": _fact(
                "group-fact",
                concept_qname="sample:Subsidiaries",
                value=long_group_text or "AAK Sweden AB, Sweden, ownership 100%.",
            ),
            "revenue-fact": _fact(
                "revenue-fact",
                concept_qname="sample:Revenue",
                value="5000000",
                is_numeric=True,
            ),
        },
        "segments": {
            "identity": [
                {"fact_key": "identity-fact", "selection_reason": "concept:Name"}
            ],
            "business_profile": [
                {
                    "fact_key": "description-fact",
                    "selection_reason": "concept:Description",
                }
            ],
            "people_and_audit": [
                {"fact_key": "people-fact", "selection_reason": "concept:People"}
            ],
            "products_markets_and_segments": [
                {"fact_key": "segments-fact", "selection_reason": "concept:Segments"}
            ],
            "group_structure": [
                {"fact_key": "group-fact", "selection_reason": "concept:Subsidiaries"}
            ],
            "financial_highlights": [
                {"fact_key": "revenue-fact", "selection_reason": "concept:Revenue"}
            ],
        },
        "visible_sections": [],
    }


def _fact(
    fact_key: str,
    *,
    concept_qname: str,
    value: str,
    is_numeric: bool = False,
) -> dict[str, Any]:
    return {
        "fact_key": fact_key,
        "source_fact_id": fact_key,
        "ordinal": 1,
        "report_member": "reports/aak.xhtml",
        "concept_qname": concept_qname,
        "canonical_value": value,
        "language": "en",
        "decimals": None,
        "unit": None,
        "is_nil": False,
        "is_numeric": is_numeric,
        "entity": {"scheme": "lei", "identifier": "AAK"},
        "period": {"start": "2024-01-01", "end": "2024-12-31"},
        "dimensions": {},
        "oim_dimensions": {},
        "links": [],
    }


class _FakeObjectStore:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects
        self.created_buckets: list[str] = []

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        return sorted(
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        )

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)


class _FakeClickHouseClient:
    def __init__(self, responses: list[list[tuple[object, ...]]]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, object]] = []

    def execute(
        self,
        sql: str,
        parameters: object = None,
    ) -> list[tuple[object, ...]]:
        self.calls.append((sql, parameters))
        return self.responses.pop(0) if self.responses else []


class _FakeClickHouse:
    def __init__(self, responses: list[list[tuple[object, ...]]]) -> None:
        self.client = _FakeClickHouseClient(responses)

    @contextmanager
    def get_connection(self):
        yield self.client
