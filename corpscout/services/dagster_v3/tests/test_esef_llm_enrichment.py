import json
from contextlib import contextmanager
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from dagster import AssetKey
from clickhouse_driver import Client

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.esef_filings import tables
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
    _load_latest_source_documents,
    _people_with_explicit_roles,
    _source_specific_segment_artifact,
    esef_document_company_information_duckdb,
    run_esef_llm_enrichment,
)
from dagster_v3.defs.esef_filings.document_publish import (
    esef_document_company_information_clickhouse,
    esef_document_information_clickhouse,
)
from dagster_v3.defs.esef_filings.segment_assets import ESEF_DOCUMENT_BUCKET
from dagster_v3.defs.esef_filings.segment_parser import (
    ARTIFACT_SCHEMA_VERSION,
    artifact_object_key,
)


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


def test_source_artifact_schema_must_match_selected_document_metadata() -> None:
    with pytest.raises(ValueError, match="expected=3 actual=5"):
        _source_specific_segment_artifact(
            _segment_artifact(),
            document={"artifact_schema_version": 3},
        )


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

    assert artifact["schema_version"] == 1
    assert artifact["prompt_version"] == "esef-company-enrichment-v2"
    assert artifact["source"]["package_sha256"] == "a" * 64
    assert artifact["model"]["name"] == "deepseek-v4-flash"
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
        "esef_filings/llm_company_enrichment/schema=v1/"
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


def test_llm_enrichment_asset_depends_on_final_clickhouse_documents() -> None:
    output_key = AssetKey("esef_document_company_information_duckdb")

    assert esef_document_company_information_duckdb.asset_deps[output_key] == {
        AssetKey("esef_source_documents_clickhouse")
    }


def test_llm_clickhouse_publication_is_separate_from_routine_document_publish() -> None:
    company_information_key = AssetKey("esef_document_company_information_clickhouse")

    assert company_information_key not in esef_document_information_clickhouse.keys
    assert esef_document_company_information_clickhouse.asset_deps[
        company_information_key
    ] == {AssetKey("esef_document_company_information_duckdb")}


def test_latest_document_selector_uses_one_final_xbrl_per_company() -> None:
    clickhouse = _FakeClickHouse(
        [
            (
                "AAK-2024",
                "a" * 64,
                "549300GK4LGIDDWJWL07",
                "SE",
                "5566692850",
                "2024-12-31",
                2024,
                "https://example.test/aak.zip",
                "packages/aak.zip",
                artifact_object_key("a" * 64),
                ARTIFACT_SCHEMA_VERSION,
            )
        ]
    )

    documents = _load_latest_source_documents(
        clickhouse,
        model="deepseek-v4-flash",
        country_iso2="SE",
        company_ids={"5566692850"},
        source_document_ids=set(),
        max_documents=10,
        refresh_existing=False,
    )

    assert [document["source_document_id"] for document in documents] == ["AAK-2024"]
    sql, parameters = clickhouse.client.calls[0]
    assert "row_number() OVER" in sql
    assert "PARTITION BY country_iso2, company_id" in sql
    assert "PARTITION BY country_iso2, company_id, source_document_id" in sql
    assert "preferred_filing_artifact_rank = 1" in sql
    assert "ORDER BY artifact_schema_version DESC" in sql
    assert "latest_company_report_rank = 1" in sql
    assert "artifact_schema_version = 3" in sql
    assert "ixbrl_segments/schema=v3/%%" in sql
    assert "artifact_schema_version = 4" in sql
    assert "ixbrl_segments/schema=v4/%%" in sql
    assert "artifact_schema_version = 5" in sql
    assert "ixbrl_segments/schema=v5/%%" in sql
    assert "esef_document_company_information" in sql
    assert "esef_source_documents FINAL" not in sql
    assert "esef_document_company_information FINAL" not in sql
    assert parameters["model_name"] == "deepseek-v4-flash"
    assert parameters["prompt_version"] == "esef-company-enrichment-v2"
    assert parameters["country_iso2"] == "SE"
    assert parameters["company_ids"] == ("5566692850",)
    clickhouse_client = Client("localhost")
    rendered_sql = clickhouse_client.substitute_params(
        sql,
        parameters,
        clickhouse_client.connection.context,
    )
    assert "ixbrl_segments/schema=v3/%'" in rendered_sql
    assert "ixbrl_segments/schema=v4/%'" in rendered_sql
    assert "ixbrl_segments/schema=v5/%'" in rendered_sql


def test_latest_document_selector_requires_resolved_company_links() -> None:
    clickhouse = _FakeClickHouse([])

    assert (
        _load_latest_source_documents(
            clickhouse,
            model="deepseek-v4-flash",
            country_iso2="",
            company_ids=set(),
            source_document_ids=set(),
            max_documents=None,
            refresh_existing=False,
        )
        == []
    )
    sql, _parameters = clickhouse.client.calls[0]
    assert "company_id != ''" in sql
    assert "country_iso2 != ''" in sql


def test_company_id_selector_requires_country_identity_boundary() -> None:
    with pytest.raises(ValueError, match="company_ids require country_iso2"):
        run_esef_llm_enrichment(
            esef_filings_duckdb=None,  # type: ignore[arg-type]
            clickhouse=None,  # type: ignore[arg-type]
            object_store=None,
            client=None,  # type: ignore[arg-type]
            model="deepseek-v4-flash",
            source_run_id="llm-run",
            country_iso2="",
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
            esef_filings_duckdb=None,  # type: ignore[arg-type]
            clickhouse=None,  # type: ignore[arg-type]
            object_store=None,
            client=None,  # type: ignore[arg-type]
            model="deepseek-v4-flash",
            source_run_id="llm-run",
            country_iso2="",
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


def test_llm_asset_stores_source_document_information_and_reuses_artifact(
    tmp_path: Path,
) -> None:
    package_sha256 = "a" * 64
    input_key = artifact_object_key(package_sha256)
    database = duckdb_resource(tmp_path / "esef.duckdb")
    object_store = _FakeObjectStore(
        {
            (ESEF_DOCUMENT_BUCKET, input_key): json.dumps(_segment_artifact()).encode(),
        }
    )

    first = run_esef_llm_enrichment(
        esef_filings_duckdb=database,
        clickhouse=_FakeClickHouse([_source_document_clickhouse_row(input_key)]),
        object_store=object_store,
        client=_client_returning(_valid_response()),
        model="deepseek-v4-flash",
        source_run_id="llm-run-1",
        source_document_ids=["AAK-2024"],
        country_iso2="",
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
    )
    second = run_esef_llm_enrichment(
        esef_filings_duckdb=database,
        clickhouse=_FakeClickHouse([_source_document_clickhouse_row(input_key)]),
        object_store=object_store,
        client=SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **_kwargs: (_ for _ in ()).throw(
                        AssertionError("model must not be called")
                    )
                )
            )
        ),
        model="deepseek-v4-flash",
        source_run_id="llm-run-2",
        source_document_ids=["AAK-2024"],
        country_iso2="",
        company_ids=[],
        max_documents=None,
        refresh_existing=False,
        max_evidence_chars=64_000,
        log_info=lambda *_args: None,
        reprocess_existing_without_model=True,
    )
    assert first["enriched_document_count"] == 1
    assert first["reused_enrichment_count"] == 0
    assert first["person_candidate_count"] == 1
    assert first["raw_person_candidate_count"] == 2
    assert first["dropped_non_specific_person_candidate_count"] == 1
    assert first["description_candidate_count"] == 1
    assert second["enriched_document_count"] == 0
    assert second["reused_enrichment_count"] == 1
    assert second["reprocess_existing_without_model"] is True
    assert first["selection_method"] == "latest_xbrl_per_company"
    assert first["selected_company_count"] == 1
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
    assert artifact["source"]["input_artifact_key"] == (
        f"s3://{ESEF_DOCUMENT_BUCKET}/{input_key}"
    )
    assert artifact["enrichment"]["company_description"]["language"] == "en"
    assert len(artifact["enrichment"]["people"]) == 2
    assert object_store.created_buckets == [ESEF_DOCUMENT_BUCKET] * 2
    with database.get_connection() as connection:
        row = connection.execute(
            f"select source_document_id, country_iso2, company_id, "
            f"company_description, people_json, extraction_status, "
            f"llm_request_object_key, llm_request_sha256, llm_response_text, "
            f"llm_response_sha256 from "
            f"{tables.DLT_DATASET_NAME}."
            f"{tables.ESEF_DOCUMENT_COMPANY_INFORMATION_TABLE}"
        ).fetchone()
    assert row[:4] == (
        "AAK-2024",
        "SE",
        "5566692850",
        "AAK develops plant-based oils and fats used in food, nutrition, and personal-care products.",
    )
    assert json.loads(row[4])[0]["name"] == "Anna Andersson"
    assert len(json.loads(row[4])) == 1
    assert row[5] == "reused"
    assert row[6] == request_keys[0]
    assert row[7] == request_sha256
    assert json.loads(row[8]) == _valid_response()
    assert row[9] == sha256(row[8].encode()).hexdigest()


def _source_document_clickhouse_row(input_key: str) -> tuple[object, ...]:
    return (
        "AAK-2024",
        "a" * 64,
        "549300GK4LGIDDWJWL07",
        "SE",
        "5566692850",
        "2024-12-31",
        2024,
        "https://example.test/aak.zip",
        "packages/aak.zip",
        input_key,
        ARTIFACT_SCHEMA_VERSION,
    )


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
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, object]]] = []

    def execute(
        self,
        sql: str,
        parameters: dict[str, object],
    ) -> list[tuple[object, ...]]:
        self.calls.append((sql, parameters))
        return self.rows


class _FakeClickHouse:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self.client = _FakeClickHouseClient(rows)

    @contextmanager
    def get_connection(self):
        yield self.client
