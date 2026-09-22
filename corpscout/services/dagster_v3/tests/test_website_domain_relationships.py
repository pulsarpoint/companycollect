import json
from datetime import UTC, datetime

import pytest

from dagster_v3.defs.common.domain_relationships import parse_relationship_answer
from dagster_v3.defs.website_relationships.relationships import (
    ANALYSIS_COLUMNS, WebsiteRelationshipProfile, analyze_domain, relationship_input,
)
from tests.test_esef_domain_relationships import Completion, answer


def source_row():
    return {"result_id": "result", "crawl_domain": "issuer.com", "result_kind": "crawl",
            "source_path": "crawls/issuer/result.json.gz", "source_host": "issuer.com",
            "country_code": "SE", "company_id": "123", "reporting_entity_name": "Example AB",
            "registrable_domain": "nova.com", "captured_at": datetime(2026, 9, 18, tzinfo=UTC),
            "source_url": "https://issuer.com/partners", "evidence_json": json.dumps([{
                "link_id": "p1:l1", "source_page_id": "p1", "url": "https://nova.com",
                "source_url": "https://issuer.com/partners", "fetched_at": "2026-09-18T10:00:00Z",
                "surrounding_text": "Nova supplies Example AB with pumps.", "anchor_text": "Nova",
                "section_heading": "Suppliers", "context_version": "website-domain-context-v1",
                "context_truncated": True, "html_sha256": "a" * 64,
            }])}


def test_website_input_keeps_capture_provenance_and_citations_without_report_dates():
    payload = relationship_input(source_row())
    assert "period_end" not in payload
    evidence = payload["evidence"][0]
    assert evidence["source_url"] == "https://issuer.com/partners"
    assert evidence["source_context"]["truncated"]
    assert evidence["locations"][0]["link_id"] == "p1:l1"
    assert parse_relationship_answer(answer(), payload).statements[0].related_entity_name == "Nova"
    quoted_url = answer(related_entity_name="", evidence=[{"evidence_id": "e0", "quote": "nova.com"}])
    assert parse_relationship_answer(quoted_url, payload).statements
    with pytest.raises(ValueError):
        parse_relationship_answer(answer(evidence=[{"evidence_id": "e0", "quote": "invented.com"}]), payload)


def test_website_attempt_has_complete_schema_and_cache_includes_company_identity():
    row = source_row()
    client = Completion(answer())
    result = analyze_domain(row, profile=WebsiteRelationshipProfile(), client=client,
                            run_id="test", max_input_chars=80_000)
    assert set(result) == set(ANALYSIS_COLUMNS)
    assert result["status"] == "success"
    other = analyze_domain(row | {"company_id": "other"}, profile=WebsiteRelationshipProfile(),
                           client=client, run_id="test", max_input_chars=80_000)
    assert other["input_hash"] != result["input_hash"]
    row["evidence_json"] = '[{"url":"https://nova.com"}]'
    missing = analyze_domain(row, profile=WebsiteRelationshipProfile(), client=client,
                             run_id="test", max_input_chars=80_000)
    assert missing["status"] == "needs_context"
    assert client.calls == 2
