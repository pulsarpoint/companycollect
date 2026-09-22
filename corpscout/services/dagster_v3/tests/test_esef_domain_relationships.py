import json
from datetime import date
from types import SimpleNamespace

import pytest

from dagster_v3.defs.common.domain_relationships import parse_relationship_answer

from dagster_v3.defs.esef_filings.domain_relationships import (
    ANALYSIS_COLUMNS, RelationshipProfile, analyze_domain,
    relationship_input,
)


def source_row():
    return {"source_document_id": "doc", "package_sha256": "a" * 64,
            "lei": "issuer-lei", "reporting_entity_name": "Example AB",
            "period_end": date(2024, 12, 31), "registrable_domain": "nova.com",
            "source_url": "https://filing.test/doc",
            "evidence_json": json.dumps([{
                "report_member": "report.xhtml", "xpath": "/html/body/section/p", "page_id": "pf1",
                "source_line": 12, "normalized_url": "https://nova.com",
                "source_context": {"version": "esef-domain-context-v1", "scope": "section",
                    "text": "Suppliers. Nova supplies Example AB with pumps.",
                    "local_text": "Nova supplies Example AB with pumps.", "headings": ["Suppliers"],
                    "table_headers": [], "truncated": False},
            }])}


def answer(**changes):
    return json.dumps({"statements": [{"related_entity_name": "Nova",
        "description": "The 2024 report states that Nova supplies Example AB with pumps.",
        "relationship_supported": True,
        "evidence": [{"evidence_id": "e0", "quote": "Nova supplies Example AB with pumps."}],
        **changes}]})


def test_free_text_statements_retain_quotations_and_unexplained_mentions():
    payload = relationship_input(source_row())
    parsed = parse_relationship_answer(answer(), payload)
    assert parsed.statements[0].related_entity_name == "Nova"
    assert "pumps" in parsed.statements[0].description
    uncertain = parse_relationship_answer(answer(relationship_supported=False,
        description="Nova is mentioned; the connection is unclear."), payload)
    assert not uncertain.statements[0].relationship_supported


@pytest.mark.parametrize("changes", [
    {"evidence": [{"evidence_id": "invented", "quote": "Nova"}]},
    {"evidence": [{"evidence_id": "e0", "quote": "Example AB owns Nova"}]},
    {"related_entity_name": "Nova International Holdings Limited"},
    {"evidence": []},
])
def test_unsupported_citations_and_invented_legal_names_are_not_published(changes):
    with pytest.raises(ValueError):
        parse_relationship_answer(answer(**changes), relationship_input(source_row()))


class Completion:
    def __init__(self, content):
        self.content = content
        self.calls = 0
        self.finish_reason = "stop"
        self.chat = SimpleNamespace(completions=self)

    def create(self, **kwargs):
        self.calls += 1
        return SimpleNamespace(usage=SimpleNamespace(prompt_tokens=10, completion_tokens=20),
                               choices=[SimpleNamespace(message=SimpleNamespace(content=self.content), finish_reason=self.finish_reason)])


def test_attempts_keep_invalid_output_and_inputs_and_version_changes_invalidate_cache():
    client = Completion(answer(evidence=[{"evidence_id": "e0", "quote": "Invented quotation"}]))
    profile = RelationshipProfile()
    result = analyze_domain(source_row(), profile=profile, client=client, run_id="test", max_input_chars=80_000)
    assert set(result) == set(ANALYSIS_COLUMNS)
    assert result["status"] == "invalid_response"
    assert result["raw_response"] == client.content
    assert result["statements_json"] == "[]"
    assert result["completion_tokens"] == 20
    client.content = answer()
    next_result = analyze_domain(source_row(), profile=profile.model_copy(update={"prompt_version": "v2"}),
                                 client=client, run_id="test2", max_input_chars=80_000)
    assert next_result["status"] == "success"
    assert result["input_hash"] != next_result["input_hash"]


def test_missing_or_oversized_context_is_recorded_without_a_model_call():
    client = Completion(answer())
    row = source_row()
    row["evidence_json"] = '[{"surrounding_text":"legacy short snippet"}]'
    result = analyze_domain(row, profile=RelationshipProfile(), client=client, run_id="test", max_input_chars=80_000)
    assert result["status"] == "needs_context"
    result = analyze_domain(source_row(), profile=RelationshipProfile(), client=client, run_id="test", max_input_chars=10)
    assert result["status"] == "context_limit"
    assert client.calls == 0


def test_output_limit_preserves_attempt_and_gives_actionable_retry_reason():
    client = Completion("")
    client.finish_reason = "length"
    result = analyze_domain(source_row(), profile=RelationshipProfile(), client=client,
                            run_id="limited", max_input_chars=80_000)
    assert result["status"] == "invalid_response"
    assert result["error_message"] == "Model reached max_tokens; increase profile.max_tokens and retry"
    assert result["statements_json"] == "[]"
    assert result["completion_tokens"] == 20
