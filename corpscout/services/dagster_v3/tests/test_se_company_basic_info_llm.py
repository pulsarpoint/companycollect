"""The LLM extractor: gate, request, cache reuse, preview counts, write order."""

import json
import uuid
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.llm import (
    LLM_EXTRACTOR_VERSION,
    SUGGESTION_PROMPT_VERSION,
    TEXT_SOURCE_ORDER,
    CompanyContext,
    LlmCounts,
    LlmExtractConfig,
    LlmSuggestionProfile,
    TextCandidate,
    build_suggestion_request,
    contexts_from_rows,
    llm_context_sql,
    llm_scope_sql,
    llm_sni_sql,
    run_llm_extractor,
)
from dagster_v3.defs.se_company.common import ObservationResult, input_hash_for

T1 = datetime(2026, 9, 1, tzinfo=UTC)
PROFILE = LlmSuggestionProfile(provider="deepseek", model="deepseek-v4-flash")


def row(company_id, source, *, legal_name=None, description=None, language=None, description_sv=None):
    return (company_id, source, legal_name, description, language, description_sv)


def test_scope_sql_gates_on_two_text_sources_newer_than_the_llm_row() -> None:
    sql = llm_scope_sql()
    assert f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in sql
    assert "uniqExactIf(source, source != 'llm' AND description IS NOT NULL) AS text_sources" in sql
    assert "maxIf(observed_at, source != 'llm' AND description IS NOT NULL) AS newest_text" in sql
    assert "HAVING text_sources >= 2 AND (llm_rows = 0 OR newest_text > llm_observed)" in sql
    assert sql.rstrip().endswith("WHERE company_id > %(after_company_id)s\nORDER BY company_id\nLIMIT %(page_size)s")
    assert "source != 'llm'" in llm_context_sql() and "company_id IN %(company_ids)s" in llm_context_sql()
    assert "FROM corpscout.se_scb_companies FINAL WHERE has_company = 1 AND company_id IN %(company_ids)s" in llm_sni_sql()


def test_contexts_pick_the_legal_name_by_precedence_and_order_texts() -> None:
    contexts = contexts_from_rows(
        [
            row("5560000000", "wikidata", legal_name="Wiki AB", description="wiki text", language="en"),
            row("5560000000", "bolagsverket", legal_name="Bolag AB", description="Bolag text en", language="en", description_sv="Bolag text sv"),
            row("5560000000", "scb", legal_name="SCB AB"),
            row("5560000000", "esef", description="esef text", language="en"),
            row("5561111111", "scb", legal_name="Solo AB", description=None),
        ],
        [("5560000000", "62010")],
    )
    context = contexts["5560000000"]
    assert context.legal_name == "SCB AB" and context.sni_code == "62010"
    assert [t.source for t in context.texts] == ["esef", "wikidata", "bolagsverket"]
    assert context.texts[2] == TextCandidate(source="bolagsverket", text="Bolag text en", text_sv="Bolag text sv")
    assert contexts["5561111111"].texts == () and contexts["5561111111"].sni_code is None
    assert TEXT_SOURCE_ORDER == ("esef", "wikidata", "bolagsverket", "ratsit")


def test_request_is_stable_json_and_only_model_and_messages_hash() -> None:
    context = CompanyContext(
        company_id="5560000000", legal_name="SCB AB", sni_code="62010",
        texts=(TextCandidate("esef", "esef text", None), TextCandidate("bolagsverket", "Bolag en", "Bolag sv")),
    )
    request = build_suggestion_request(context, PROFILE)
    assert request["model"] == "deepseek-v4-flash" and request["response_format"] == {"type": "json_object"}
    payload = json.loads(request["messages"][1]["content"])
    assert payload == {
        "company_id": "5560000000", "legal_name": "SCB AB", "sni_code": "62010",
        "sources": [{"source": "esef", "text": "esef text"}, {"source": "bolagsverket", "text": "Bolag en", "text_sv": "Bolag sv"}],
    }
    assert request["messages"][1]["content"] == json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    assert "exactly one JSON object" in request["messages"][0]["content"]
    hash_a = input_hash_for(request, SUGGESTION_PROMPT_VERSION)
    warmer = build_suggestion_request(context, LlmSuggestionProfile(provider="deepseek", model="deepseek-v4-flash", temperature=1))
    assert input_hash_for(warmer, SUGGESTION_PROMPT_VERSION) == hash_a


def test_config_requires_an_explicit_profile_and_rejects_since() -> None:
    with pytest.raises(ValidationError):
        LlmExtractConfig(execute=True)
    with pytest.raises(ValidationError):
        LlmSuggestionProfile(provider="deepseek")
    with pytest.raises(ValidationError):
        LlmExtractConfig(llm=PROFILE, since="2026-09-01T00:00:00Z")
    assert LlmExtractConfig(llm=PROFILE).llm.prompt_version == SUGGESTION_PROMPT_VERSION


class FakeClient:
    def __init__(self, *, scope_pages, context_rows, sni_rows=(), observations=()):
        self.scope_pages = list(scope_pages)
        self.context_rows = list(context_rows)
        self.sni_rows = list(sni_rows)
        self.observations = list(observations)
        self.statements: list[tuple[str, object]] = []
        self.inserts: list[tuple[str, list]] = []

    def execute(self, sql, params=None, settings=None):
        self.statements.append((sql, params))
        if sql.startswith("INSERT INTO"):
            self.inserts.append((sql, list(params)))
            return []
        if "AS text_sources" in sql:
            return [(i,) for i in (self.scope_pages.pop(0) if self.scope_pages else [])]
        ids = set(params["company_ids"])
        if "suggestion_id, company_id, toString(input_hash)" in sql:
            return [o for o in self.observations if o[1] in ids]
        if "ng1_code" in sql:
            return [r for r in self.sni_rows if r[0] in ids]
        if f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL" in sql:
            return [r for r in self.context_rows if r[0] in ids]
        raise AssertionError(sql)


class FakeResource:
    """Stands in for ClickhouseResource in publish_with_stage: the extractor must call
    `publish_observations` through an injectable seam, see Step 3."""


CONTEXT_ROWS = [
    row("5560000000", "scb", legal_name="SCB AB"),
    row("5560000000", "esef", description="esef text", language="en"),
    row("5560000000", "wikidata", description="wiki text", language="en"),
]


def _stored(company_id, input_hash, created_at=T1):
    return (str(uuid.uuid4()), company_id, input_hash, json.dumps({"description": "cached", "description_sv": "cachad", "language": "en", "rationale": ""}),
            "deepseek", "deepseek-v4-flash", SUGGESTION_PROMPT_VERSION, created_at)


def test_preview_counts_reuse_against_stored_hashes_and_writes_nothing(monkeypatch) -> None:
    context = contexts_from_rows(CONTEXT_ROWS, [])["5560000000"]
    known = input_hash_for(build_suggestion_request(context, PROFILE), SUGGESTION_PROMPT_VERSION)
    client = FakeClient(scope_pages=[["5560000000", "5561111111"], []], context_rows=CONTEXT_ROWS + [row("5561111111", "scb", legal_name="Solo", description="only one", language="sv")],
                        observations=[_stored("5560000000", known)])
    published: list = []
    counts = run_llm_extractor(
        client, clickhouse=FakeResource(), llm_client=None, profile=PROFILE,
        config=LlmExtractConfig(llm=PROFILE, page_size=10), source_run_id="r",
        publish_observations=lambda clickhouse, rows: published.extend(rows),
    )
    assert counts.eligible == 1 and counts.skipped_single_source == 1
    assert counts.would_reuse == 1 and counts.would_call_model == 0
    assert counts.execute is False and client.inserts == [] and published == []


def test_execute_reuses_then_calls_and_writes_observations_before_suggestions() -> None:
    context = contexts_from_rows(CONTEXT_ROWS, [])["5560000000"]
    known = input_hash_for(build_suggestion_request(context, PROFILE), SUGGESTION_PROMPT_VERSION)
    other_rows = [row("5562222222", "scb", legal_name="Two AB"), row("5562222222", "esef", description="a", language="en"), row("5562222222", "ratsit", description="b", language="sv")]
    client = FakeClient(scope_pages=[["5560000000", "5562222222"], []], context_rows=CONTEXT_ROWS + other_rows,
                        observations=[_stored("5560000000", known)])
    calls: list[str] = []

    def fake_call(request, *, provider, prompt_version):
        calls.append(json.loads(request["messages"][1]["content"])["company_id"])
        return ObservationResult(
            suggestion={"description": "fresh", "description_sv": "färsk", "language": "en", "rationale": ""},
            raw_response="{}", model_provider=provider, model_name=request["model"], prompt_version=prompt_version,
            prompt_tokens=10, completion_tokens=5, suggestion_id=uuid.uuid4(),
        )

    published: list = []
    counts = run_llm_extractor(
        client, clickhouse=FakeResource(), llm_client=object(), profile=PROFILE,
        config=LlmExtractConfig(llm=PROFILE, execute=True, page_size=10), source_run_id="run-1",
        publish_observations=lambda clickhouse, rows: published.extend(rows), call_model=fake_call,
    )
    assert calls == ["5562222222"]
    assert counts.reused == 1 and counts.called == 1 and counts.failed == 0
    assert counts.observations_inserted == 1 and counts.inserted == 2
    # The paid call is persisted before the suggestion rows that cite it.
    assert len(published) == 1 and published[0][1] == "5562222222"
    assert len(client.inserts) == 1
    sql, rows = client.inserts[0]
    assert sql == f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} ({', '.join(tables.SUGGESTION_INSERT_COLUMNS)}) VALUES"
    by_company = {r[0]: dict(zip(tables.SUGGESTION_INSERT_COLUMNS, r)) for r in rows}
    reused = by_company["5560000000"]
    assert reused["source"] == "llm" and reused["description"] == "cached" and reused["description_sv"] == "cachad"
    assert reused["observed_at"] == T1 and reused["extractor_version"] == LLM_EXTRACTOR_VERSION
    fresh = by_company["5562222222"]
    assert fresh["description"] == "fresh" and fresh["description_language"] == "en"
    assert fresh["source_record_uid"] == str(published[0][0]) and fresh["source_run_id"] == "run-1"
    for column in ("legal_name", "legal_form_code", "status", "incorporation_date", "lei", "wikidata_id", "decided_by", "note"):
        assert fresh[column] is None, column


def test_a_failed_model_call_is_counted_and_skipped() -> None:
    client = FakeClient(scope_pages=[["5560000000"], []], context_rows=CONTEXT_ROWS)

    def failing(request, *, provider, prompt_version):
        raise ValueError("truncated")

    counts = run_llm_extractor(
        client, clickhouse=FakeResource(), llm_client=object(), profile=PROFILE,
        config=LlmExtractConfig(llm=PROFILE, execute=True), source_run_id="r",
        publish_observations=lambda clickhouse, rows: None, call_model=failing,
    )
    assert counts.failed == 1 and counts.inserted == 0 and client.inserts == []


def test_counts_metadata_names_every_counter() -> None:
    counts = LlmCounts(companies=1, pages=1, eligible=1, skipped_single_source=0, would_reuse=0, would_call_model=1,
                       reused=0, called=1, failed=0, observations_inserted=1, inserted=1, execute=True, stopped_at_cap=False)
    assert set(counts.as_metadata()) == {
        "companies", "pages", "eligible", "skipped_single_source", "would_reuse", "would_call_model",
        "reused", "called", "failed", "observations_inserted", "inserted", "execute", "stopped_at_cap",
    }
