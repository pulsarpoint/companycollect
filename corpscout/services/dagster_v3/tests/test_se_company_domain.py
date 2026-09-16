"""Domain decisions, evidence identity, paid-call reuse and job boundaries."""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
import json

import dagster as dg
import pytest

from dagster_v3.defs.se_company.domain import batch, tables
from dagster_v3.defs.se_company.domain.evidence import input_payload, requires_verification
from dagster_v3.defs.se_company.domain.fold import fold_company
from dagster_v3.defs.se_company.domain.verification import DomainVerificationProfile, fingerprints, parse_verdict, verify_domain

STAMP = datetime(2026, 9, 14, tzinfo=UTC)
COMPANY = {"company_id": "5561552760", "legal_name": "Example AB", "lei": "", "wikidata_id": "Q1"}
PROFILE = DomainVerificationProfile(provider="test", model="model", base_url="https://example.test/v1", system_prompt="Verify supplied evidence", prompt_version="domain:test:r1")


def suggestion(source="wikidata", domain="example.se", **changes):
    return dict(company_id=COMPANY["company_id"], source=source, slot=domain, root_domain=domain,
                website_url=f"https://{domain}", website_host=domain, association="connected", is_primary=1,
                confidence=0.95, confidence_basis="official_website_claim", source_record_id="source-record",
                source_url="https://source.test/claim", evidence='{"claim":"official website"}',
                observed_at=STAMP, removed=0, decided_by="", note="", suggestion_id="id",
                suggested_at=STAMP, source_run_id="source-run", extractor_version="v1") | changes


def fold(rows, previous=(), rules=(), precedence=(), verified=None):
    return fold_company(company_id=COMPANY["company_id"], suggestions=rows, previous=previous, rules=rules,
                        precedence=precedence, verification=verified or {}, folded_at=STAMP, source_run_id="fold-run")


def rule(action, domain="example.se", **changes):
    return dict(company_id=COMPANY["company_id"], root_domain=domain, action=action, removed=0,
                decided_by="reviewer", note="Checked identity", evidence_hash="reviewed", decided_at=STAMP) | changes


def verified(verdict="connected", **changes):
    return dict(status="success", verdict=verdict, confidence=0.96, reason="Evidence confirms company identity", input_hash="hash") | changes


def test_fold_retains_multiple_domains_and_applies_field_precedence():
    rows, history = fold([suggestion(), suggestion("esef_filing", website_url="https://example.se/company"), suggestion(domain="other.se")])
    assert len(rows) == len(history) == 2
    assert sum(row["is_primary"] for row in rows) == 1
    main = next(row for row in rows if row["root_domain"] == "example.se")
    assert main["website_url"].endswith("/company") and main["website_source"] == "esef_filing"
    assert main["sources"] == ["esef_filing", "wikidata"]
    assert all(row["active"] for row in rows)
    override = {"company_id": COMPANY["company_id"], "root_domain": "example.se", "field": "website", "source": "wikidata", "precedence": 1000, "removed": 0}
    changed, _ = fold([suggestion(), suggestion("esef_filing", website_url="https://example.se/company")], precedence=[override])
    assert changed[0]["website_source"] == "wikidata"


@pytest.mark.parametrize("confidence", [0.4, 0.89])
def test_weak_claim_is_not_published_without_decisive_verification(confidence):
    source = suggestion(confidence=confidence)
    assert requires_verification([source])
    assert fold([source])[0][0]["active"] == 0
    assert fold([source], verified={"example.se": verified(confidence=.89)})[0][0]["active"] == 0
    assert fold([source], verified={"example.se": verified()})[0][0]["active"] == 1


def test_strong_claim_beats_uncertain_mention_but_conflicting_claims_need_verification():
    uncertain = suggestion("esef_filing", association="uncertain")
    assert not requires_verification([suggestion(), uncertain])
    assert fold([suggestion(), uncertain])[0][0]["active"] == 1
    negative = suggestion("esef_filing", association="not_connected")
    assert requires_verification([suggestion(), negative])
    row = fold([suggestion(), negative])[0][0]
    assert row["association"] == "uncertain" and not row["active"]
    assert not requires_verification([negative])
    assert fold([negative])[0][0]["association"] == "not_connected"


@pytest.mark.parametrize("action,active", [("rejected", 0), ("confirmed_primary", 1), ("confirmed_related", 1)])
def test_reviewer_decision_is_authoritative(action, active):
    row = fold([suggestion(association="uncertain")], rules=[rule(action)], verified={"example.se": verified("not_connected" if active else "connected")})[0][0]
    assert row["active"] == active and row["association_source"] == "reviewer"
    assert row["reviewed_by"] == "reviewer"


def test_source_withdrawal_preserves_other_sources_and_confirmed_domains():
    original, _ = fold([suggestion(), suggestion("esef_filing")])
    remaining, history = fold([suggestion(removed=1), suggestion("esef_filing")], previous=original)
    assert remaining[0]["active"] == 1 and remaining[0]["sources"] == ["esef_filing"]
    withdrawn, history = fold([], previous=remaining)
    assert withdrawn[0]["active"] == 0 and history[0]["change_kind"] == "withdrawn"
    retained, _ = fold([], previous=remaining, rules=[rule("confirmed_related")])
    assert retained[0]["active"] == 1
    released, _ = fold([], previous=retained, rules=[rule("unreviewed", removed=1)])
    assert released[0]["active"] == 0


def test_unchanged_fold_has_no_history_and_hashes_ignore_extraction_bookkeeping():
    source = suggestion()
    first, _ = fold([source])
    second, history = fold([source], previous=first)
    assert history == []
    original = input_payload(COMPANY, "example.se", [source])
    replayed = suggestion(observed_at=STAMP + timedelta(days=1), source_run_id="new-run", suggestion_id="new-id", source_record_id="new-source-id")
    assert input_payload(COMPANY, "example.se", [replayed]) == original
    hashes = fingerprints(original, PROFILE)
    renamed = PROFILE.model_copy(update={"prompt_version": "renamed-revision"})
    assert fingerprints(original, renamed) == hashes
    for changed in [PROFILE.model_copy(update={"system_prompt": "different instructions"}), PROFILE.model_copy(update={"model": "different-model"})]:
        assert fingerprints(original, changed)["input_hash"] != hashes["input_hash"]
    assert fingerprints(input_payload(COMPANY | {"legal_name": "Namesake AB"}, "example.se", [source]), PROFILE)["data_hash"] != hashes["data_hash"]


@pytest.mark.parametrize("value", [
    {"verdict": "connected", "confidence": .99, "reason": "Yes", "evidence_ids": []},
    {"verdict": "connected", "confidence": .99, "reason": "Yes", "evidence_ids": ["invented"]},
    {"verdict": "connected", "confidence": float("nan"), "reason": "Yes", "evidence_ids": ["e0"]},
    {"verdict": "connected", "confidence": 1.01, "reason": "Yes", "evidence_ids": ["e0"]},
    {"verdict": "yes", "confidence": .99, "reason": "Yes", "evidence_ids": ["e0"]},
])
def test_verdict_validation_rejects_unsupported_answers(value):
    with pytest.raises(ValueError):
        parse_verdict(json.dumps(value), {"e0"})


class FakeModel:
    def __init__(self):
        self.calls = []
        self.chat = SimpleNamespace(completions=SimpleNamespace(create=self.create))

    def create(self, **kwargs):
        self.calls.append(kwargs)
        answer = json.dumps({"verdict": "connected", "confidence": .98, "reason": "Exact company identity in evidence", "evidence_ids": ["e0"]})
        return SimpleNamespace(usage=SimpleNamespace(prompt_tokens=12, completion_tokens=8), choices=[SimpleNamespace(message=SimpleNamespace(content=answer))])

    def close(self):
        pass


class MemoryClient:
    def __init__(self, sources):
        self.data = {table: [] for table in tables.TABLES}
        self.data[tables.SUGGESTION_TABLE] = sources
        self.data["se_company_basic_info"] = [COMPANY]
        self.writes = []

    def execute(self, sql, params=None, settings=None):
        table = sql.split("corpscout.", 1)[1].split()[0]
        if sql.startswith("INSERT"):
            columns = sql.split("(", 1)[1].split(")", 1)[0].split(", ")
            self.writes.append(table)
            rows = [dict(zip(columns, values, strict=True)) for values in params]
            if table == tables.MAIN_TABLE:
                replaced = {(r["company_id"], r["root_domain"]) for r in rows}
                self.data[table] = [r for r in self.data[table] if (r["company_id"], r["root_domain"]) not in replaced]
            self.data[table].extend(rows)
            return []
        columns = sql[7:sql.index(" FROM")].split(", ")
        rows = [row for row in self.data[table] if row["company_id"] in params["company_ids"]]
        if table == tables.VERIFICATION_TABLE:
            hash_field = "input_hash" if "AND input_hash IN" in sql else "data_hash"
            newest = {}
            for row in rows:
                if row[hash_field] in params["hashes"]:
                    newest[row["company_id"], row["root_domain"]] = row
            rows = list(newest.values())
        return [tuple(row[column] for column in columns) for row in rows]


def test_batch_reuses_paid_answers_and_updates_only_changed_evidence(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    client = MemoryClient([suggestion(association="uncertain"), suggestion(domain="trusted.se")])
    model = FakeModel()
    def run(profile=PROFILE, **extra):
        counts = batch.verify_domains(client, retry_failed_only=False, page_size=100, changed_only=True, profile=profile,
                                      max_llm_calls=1, run_id="run", log=lambda *args: None,
                                      llm_factory=lambda *args, **kwargs: model, **extra)
        counts.update(batch.publish_domains(client, page_size=100, changed_only=True, profile=profile,
                                            run_id="run", log=lambda *args: None))
        return counts
    first = run()
    assert first["llm_calls"] == 1 and len(model.calls) == 1
    assert client.writes.index(tables.HISTORY_TABLE) < client.writes.index(tables.MAIN_TABLE)
    assert len(client.data[tables.HISTORY_TABLE]) == 2
    second = run()
    assert second["llm_calls"] == 0 and second["unchanged_companies"] == 1
    assert len(client.data[tables.HISTORY_TABLE]) == 2
    assert run(PROFILE.model_copy(update={"prompt_version": "rename-only"}))["llm_calls"] == 0
    assert run(PROFILE.model_copy(update={"system_prompt": "New policy"}))["llm_calls"] == 1
    client.data[tables.SUGGESTION_TABLE][0]["evidence"] = "New company evidence"
    assert run()["llm_calls"] == 1
    assert len(model.calls) == 3
    assert len(client.data[tables.VERIFICATION_TABLE]) == 3
    assert all(r["input_json"] and r["system_prompt"] for r in client.data[tables.VERIFICATION_TABLE])
    assert run(None)["llm_calls"] == 0


def test_batch_cap_and_reviewer_decisions_never_spend_unnecessary_calls(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    client = MemoryClient([suggestion(domain=domain, association="uncertain") for domain in ("a.se", "b.se", "c.se")])
    client.data[tables.RULE_TABLE] = [rule("rejected", domain="a.se")]
    model = FakeModel()
    def run(profile):
        return batch.verify_domains(client, retry_failed_only=False, page_size=100, changed_only=True, profile=profile,
                                     max_llm_calls=1, run_id="run", log=lambda *args: None,
                                     llm_factory=lambda *args, **kwargs: model)
    assert not run(None)
    assert not client.writes
    assert len(model.calls) == 0
    first = run(PROFILE)
    assert first["llm_calls"] == first["verification_pending"] == 1
    second = run(PROFILE)
    assert second["llm_calls"] == second["verification_reused"] == 1
    assert len(model.calls) == 2
    assert set(client.writes) == {tables.VERIFICATION_TABLE}
    assert all('"domain":"a.se"' not in call["messages"][1]["content"] for call in model.calls)


def test_two_domain_jobs_include_global_sync_and_optional_verification_config():
    from dagster_clickhouse import ClickhouseResource
    from dagster_v3.defs.se_company.domain import assets, brave, common_crawl, esef, jobs, wikidata
    definitions = [assets.se_company_domain_publish, assets.se_company_domain_verification, assets.se_company_domain_precedence_clickhouse,
                   wikidata.se_company_domain_suggestions_wikidata, esef.se_company_domain_suggestions_esef_filing,
                   common_crawl.se_company_domain_suggestions_common_crawl_identity, brave.se_company_domain_suggestions_brave]
    own = set().union(*(asset.keys for asset in definitions))
    external = set().union(*(asset.dependency_keys for asset in definitions)) - own
    repo = dg.Definitions(assets=[*definitions, *(dg.AssetSpec(key) for key in external)],
                          jobs=[jobs.se_company_domain_sync_job, jobs.se_company_domain_refresh_job],
                          resources={"clickhouse": ClickhouseResource(host="localhost", user="test", password="", database="test")}).get_repository_def()
    sync = repo.get_job("se_company_domain_sync_job")
    process = repo.get_job("se_company_domain_refresh_job")
    expected = {*tables.EXTRACTOR_ASSETS, "se_company_domain_precedence_clickhouse"}
    assert {key.to_user_string() for key in sync.asset_layer.executable_asset_keys} == expected
    assert {key.to_user_string() for key in process.asset_layer.executable_asset_keys} == {*expected, "se_company_domain_verification", "se_company_domain_publish"}
    assert repo.asset_graph.get(dg.AssetKey("se_company_domain_publish")).pools == {"se_company_domain_fold"}
    config = {"ops": {name: {"config": {"execute": True}} for name in tables.EXTRACTOR_ASSETS}}
    dg.validate_run_config(sync, config)
    dg.validate_run_config(process, config)
    assert repo.asset_graph.get(dg.AssetKey("se_company_domain_verification")).pools == {"se_company_domain_fold"}
    assert assets.se_company_domain_publish.dependency_keys == {dg.AssetKey("se_company_domain_verification")}
    config["ops"]["se_company_domain_verification"] = {"config": {"verification": PROFILE.model_dump(), "max_llm_calls": 10}}
    config["ops"]["se_company_domain_publish"] = {"config": {"verification": PROFILE.model_dump()}}
    dg.validate_run_config(process, config)


def test_failed_and_invalid_model_answers_are_audited_without_positive_decisions():
    payload = input_payload(COMPANY, "example.se", [suggestion(association="uncertain")])
    hashes = fingerprints(payload, PROFILE)
    def failed(**kwargs):
        raise TimeoutError("provider timed out")
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=failed)))
    failure = verify_domain(client, company_id=COMPANY["company_id"], domain="example.se", payload=payload,
                            profile=PROFILE, hashes=hashes, run_id="failed-run")
    assert failure["status"] == "http_error" and failure["verdict"] == "uncertain"
    assert failure["input_json"] == payload and failure["source_run_id"] == "failed-run"
    model = FakeModel()
    original = model.create
    def invalid(**kwargs):
        response = original(**kwargs)
        response.choices[0].message.content = '{"verdict":"connected","confidence":1,"reason":"guess","evidence_ids":["invented"]}'
        return response
    model.chat.completions.create = invalid
    answer = verify_domain(model, company_id=COMPANY["company_id"], domain="example.se", payload=payload,
                           profile=PROFILE, hashes=hashes, run_id="invalid-run")
    assert answer["status"] == "invalid_response" and answer["verdict"] == "uncertain"
    assert answer["prompt_tokens"] == 12 and answer["raw_response"]


def test_reviewer_suggestions_override_verification_and_disabled_sources_cannot_publish():
    row = fold([suggestion("reviewer"), suggestion("common_crawl_identity", association="uncertain")],
               verified={"example.se": verified("not_connected")})[0][0]
    assert row["active"] == 1 and row["association_source"] == "reviewer"
    precedence = [{"company_id": "", "root_domain": "", "field": "association", "source": "wikidata", "precedence": 0, "removed": 0}]
    assert fold([suggestion()], precedence=precedence)[0][0]["active"] == 0


def test_separate_verification_then_publish_never_reuses_a_stale_prompt_at_the_cap(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    client = MemoryClient([suggestion(domain=domain, association="uncertain") for domain in ("a.se", "b.se")])
    model = FakeModel()
    batch.verify_domains(client, retry_failed_only=False, page_size=100, changed_only=True, profile=PROFILE, max_llm_calls=2,
                         run_id="first", log=lambda *args: None, llm_factory=lambda *args, **kwargs: model)
    assert set(client.writes) == {tables.VERIFICATION_TABLE}
    assert all(row["confidence"] == .98 and row["verdict"] == "connected" for row in client.data[tables.VERIFICATION_TABLE])
    updated = PROFILE.model_copy(update={"system_prompt": "Check the exact company identity"})
    counts = batch.verify_domains(client, retry_failed_only=False, page_size=100, changed_only=True, profile=updated, max_llm_calls=1,
                                 run_id="second", log=lambda *args: None, llm_factory=lambda *args, **kwargs: model)
    assert counts["llm_calls"] == counts["verification_pending"] == 1
    client.writes.clear()
    counts = batch.publish_domains(client, page_size=100, changed_only=True, profile=updated,
                                   run_id="second", log=lambda *args: None)
    assert counts["verification_pending"] == 1
    assert client.writes == [tables.HISTORY_TABLE, tables.MAIN_TABLE]
    assert {row["root_domain"]: row["active"] for row in client.data[tables.MAIN_TABLE]} == {"a.se": 1, "b.se": 0}
    assert len(model.calls) == 3


def test_verification_failure_is_saved_and_retry_resumes_without_repeating_success(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    client = MemoryClient([suggestion(domain=domain, association="uncertain") for domain in ("a.se", "b.se")])
    model = FakeModel()
    def partial(**kwargs):
        if len(model.calls) == 1:
            raise TimeoutError("provider timed out")
        return model.create(**kwargs)
    model.chat.completions.create = partial
    def run():
        return batch.verify_domains(client, retry_failed_only=False, page_size=100, changed_only=True, profile=PROFILE, max_llm_calls=2,
                                    run_id="retry", log=lambda *args: None, llm_factory=lambda *args, **kwargs: model)
    first = run()
    assert first["http_error"] == first["success"] == 1
    assert not client.data[tables.MAIN_TABLE]
    model.chat.completions.create = model.create
    second = run()
    assert second["success"] == second["verification_reused"] == 1
    assert len(model.calls) == 2
    assert len(client.data[tables.VERIFICATION_TABLE]) == 3


@pytest.mark.parametrize("fail", [False, True])
def test_dagster_verification_step_persists_scores_before_publication(monkeypatch, fail):
    from contextlib import nullcontext
    from dagster_clickhouse import ClickhouseResource
    from dagster_v3.defs.se_company.domain import assets

    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    client = MemoryClient([suggestion(association="uncertain")])
    model = FakeModel()
    if fail:
        def unavailable(**kwargs):
            raise TimeoutError("provider timed out")
        model.chat.completions.create = unavailable
    monkeypatch.setattr(ClickhouseResource, "get_connection", lambda self: nullcontext(client))
    monkeypatch.setattr(assets, "assert_clickhouse_tables_exist", lambda *args, **kwargs: None)
    monkeypatch.setattr(assets, "verify_domains", lambda *args, **kwargs: batch.verify_domains(
        *args, **kwargs, llm_factory=lambda *args, **kwargs: model))
    external = assets.se_company_domain_verification.dependency_keys
    config = {"ops": {name: {"config": {"verification": PROFILE.model_dump()}} for name in (
        "se_company_domain_verification", "se_company_domain_publish")}}
    result = dg.materialize(
        [assets.se_company_domain_verification, assets.se_company_domain_publish,
         *(dg.AssetSpec(key) for key in external)], run_config=config,
        resources={"clickhouse": ClickhouseResource(host="localhost", user="test", password="", database="test")},
        raise_on_error=False,
    )
    assert result.success is not fail
    assert len(client.data[tables.VERIFICATION_TABLE]) == 1
    assert client.writes[0] == tables.VERIFICATION_TABLE
    if fail:
        assert client.data[tables.VERIFICATION_TABLE][0]["status"] == "http_error"
        assert not client.data[tables.MAIN_TABLE]
        assert not result.asset_materializations_for_node("se_company_domain_publish")
    else:
        assert len(model.calls) == 1
        assert client.data[tables.MAIN_TABLE][0]["active"] == 1
        metadata = result.asset_materializations_for_node("se_company_domain_verification")[0].metadata
        assert metadata["score_column"].value == "confidence"
        assert metadata["llm_calls"].value == 1


@pytest.mark.parametrize("wrap", [
    lambda text: text,
    lambda text: "```json\n" + text + "\n```",
    lambda text: "```\n" + text + "\n```",
    lambda text: text + "\n``",
    lambda text: text + "`",
    lambda text: json.dumps({"answer": text}),
    lambda text: json.dumps({"answer": json.loads(text)}),
    lambda text: json.dumps({"answer": "```json\n" + text + "\n```"}),
])
def test_verdict_accepts_complete_json_in_known_provider_wrappers(wrap):
    value = {"verdict": "not_connected", "confidence": .95, "reason": "External report reference", "evidence_ids": ["e0"]}
    assert parse_verdict(wrap(json.dumps(value)), {"e0"}).model_dump() == value


@pytest.mark.parametrize("wrap", [
    lambda text: text[:-4],
    lambda text: text + " Actually, the opposite conclusion is correct.",
    lambda text: text + text,
    lambda text: "```json\n" + text,
    lambda text: json.dumps({"answer": text, "verdict": "connected"}),
    lambda text: text.replace('"e0"', '"invented"'),
])
def test_recovery_never_accepts_truncation_ambiguity_or_invented_evidence(wrap):
    value = {"verdict": "not_connected", "confidence": .95, "reason": "External report reference", "evidence_ids": ["e0"]}
    with pytest.raises(ValueError):
        parse_verdict(wrap(json.dumps(value)), {"e0"})


def test_saved_formatting_failure_recovers_without_call_or_audit_rewrite(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    source = suggestion(association="uncertain")
    client = MemoryClient([source])
    payload = input_payload(COMPANY, "example.se", [source])
    saved = verify_domain(FakeModel(), company_id=COMPANY["company_id"], domain="example.se", payload=payload,
                          profile=PROFILE, hashes=fingerprints(payload, PROFILE), run_id="original")
    saved.update(status="invalid_response", verdict="uncertain", confidence=0, reason="", error="Original parser failure",
                 raw_response="```json\n" + saved["raw_response"] + "\n```")
    client.data[tables.VERIFICATION_TABLE] = [saved.copy()]
    def unexpected_client(*args, **kwargs):
        pytest.fail("A valid saved response must not spend another model call")
    counts = batch.verify_domains(client, page_size=100, changed_only=True, profile=PROFILE, max_llm_calls=1,
                                  run_id="retry", log=lambda *args: None, retry_failed_only=True, llm_factory=unexpected_client)
    assert counts["verification_reused"] == counts["verification_recovered"] == 1
    assert counts["llm_calls"] == 0 and not client.writes
    published = batch.publish_domains(client, page_size=100, changed_only=True, profile=PROFILE,
                                      run_id="publish", log=lambda *args: None)
    assert published["verification_recovered"] == 1
    assert client.data[tables.MAIN_TABLE][0]["active"] == 1
    assert client.data[tables.MAIN_TABLE][0]["verification_status"] == "success"
    assert client.data[tables.VERIFICATION_TABLE] == [saved]
    assert client.data[tables.HISTORY_TABLE][0]["verification_status"] == "success"
    repeated = batch.publish_domains(client, page_size=100, changed_only=True, profile=PROFILE,
                                     run_id="publish-again", log=lambda *args: None)
    assert repeated["unchanged_companies"] == 1


def test_retry_failed_only_keeps_successes_and_skips_new_associations(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    sources = [suggestion(domain=domain, association="uncertain") for domain in ("a.se", "b.se", "c.se")]
    client = MemoryClient(sources)
    for source in sources[:2]:
        payload = input_payload(COMPANY, source["root_domain"], [source])
        saved = verify_domain(FakeModel(), company_id=COMPANY["company_id"], domain=source["root_domain"], payload=payload,
                              profile=PROFILE, hashes=fingerprints(payload, PROFILE), run_id="original")
        if source["root_domain"] == "b.se":
            saved.update(status="invalid_response", verdict="uncertain", confidence=0, reason="", raw_response="", error="empty response")
        client.data[tables.VERIFICATION_TABLE].append(saved)
    model = FakeModel()
    counts = batch.verify_domains(client, page_size=100, changed_only=False, profile=PROFILE, max_llm_calls=10,
                                  run_id="retry", log=lambda *args: None, retry_failed_only=True,
                                  llm_factory=lambda *args, **kwargs: model)
    assert counts["llm_calls"] == counts["verification_reused"] == counts["new_associations_skipped"] == 1
    assert len(model.calls) == 1
    assert '"domain":"b.se"' in model.calls[0]["messages"][1]["content"]
    assert len(client.data[tables.VERIFICATION_TABLE]) == 3


def test_empty_response_reports_the_output_limit_and_stays_unverified():
    model = FakeModel()
    profile = PROFILE.model_copy(update={"max_tokens": 4_000})
    def empty(**kwargs):
        return SimpleNamespace(usage=SimpleNamespace(prompt_tokens=10, completion_tokens=profile.max_tokens),
                               choices=[SimpleNamespace(finish_reason="length", message=SimpleNamespace(content=""))])
    model.chat.completions.create = empty
    payload = input_payload(COMPANY, "example.se", [suggestion(association="uncertain")])
    record = verify_domain(model, company_id=COMPANY["company_id"], domain="example.se", payload=payload,
                           profile=profile, hashes=fingerprints(payload, profile), run_id="empty")
    assert record["status"] == "invalid_response" and record["verdict"] == "uncertain"
    assert "token_limit" in record["error"] and "finish_reason=length" in record["error"]
    assert f"max_tokens={profile.max_tokens}" in record["error"]


@pytest.mark.parametrize("max_tokens", [None, 4_000])
def test_domain_request_omits_output_limit_unless_explicitly_configured(max_tokens):
    assert PROFILE.max_tokens is None
    profile = PROFILE.model_copy(update={"max_tokens": max_tokens})
    model = FakeModel()
    payload = input_payload(COMPANY, "example.se", [suggestion(association="uncertain")])
    record = verify_domain(model, company_id=COMPANY["company_id"], domain="example.se", payload=payload,
                           profile=profile, hashes=fingerprints(payload, profile), run_id="tokens")
    assert record["status"] == "success"
    if max_tokens is None:
        assert "max_tokens" not in model.calls[0]
    else:
        assert model.calls[0]["max_tokens"] == max_tokens


@pytest.mark.parametrize("finish_reason,category", [("length", "token_limit"), ("stop", "empty_response")])
def test_uncapped_request_reports_provider_truncation_or_empty_response(finish_reason, category):
    model = FakeModel()
    model.chat.completions.create = lambda **kwargs: SimpleNamespace(
        usage=SimpleNamespace(prompt_tokens=10, completion_tokens=8_192),
        choices=[SimpleNamespace(finish_reason=finish_reason, message=SimpleNamespace(content=""))])
    payload = input_payload(COMPANY, "example.se", [suggestion(association="uncertain")])
    record = verify_domain(model, company_id=COMPANY["company_id"], domain="example.se", payload=payload,
                           profile=PROFILE, hashes=fingerprints(payload, PROFILE), run_id="provider-limit")
    assert record["status"] == "invalid_response"
    assert record["error"].startswith(category + ":")
    assert "max_tokens=provider_default" in record["error"]


def test_removing_backoffice_cap_preserves_successes_and_retries_old_failures(monkeypatch):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    sources = [suggestion(domain=domain, association="uncertain") for domain in ("a.se", "b.se", "c.se")]
    client = MemoryClient(sources)
    old_profile = PROFILE.model_copy(update={"max_tokens": 4_000})
    for source in sources[:2]:
        payload = input_payload(COMPANY, source["root_domain"], [source])
        record = verify_domain(FakeModel(), company_id=COMPANY["company_id"], domain=source["root_domain"], payload=payload,
                               profile=old_profile, hashes=fingerprints(payload, old_profile), run_id="capped")
        if source["root_domain"] == "b.se":
            record.update(status="invalid_response", verdict="uncertain", raw_response="", error="token_limit")
        client.data[tables.VERIFICATION_TABLE].append(record)
    saved = [row.copy() for row in client.data[tables.VERIFICATION_TABLE]]
    model = FakeModel()
    counts = batch.verify_domains(client, page_size=100, changed_only=True, profile=PROFILE, max_llm_calls=10,
                                  run_id="uncapped", log=lambda *args: None, retry_failed_only=True,
                                  llm_factory=lambda *args, **kwargs: model)
    assert counts["llm_calls"] == counts["verification_reused"] == counts["new_associations_skipped"] == 1
    assert "max_tokens" not in model.calls[0]
    assert '"domain":"b.se"' in model.calls[0]["messages"][1]["content"]
    assert client.data[tables.VERIFICATION_TABLE][:2] == saved
    batch.publish_domains(client, page_size=100, changed_only=True, profile=PROFILE, run_id="publish", log=lambda *args: None)
    assert {row["root_domain"]: row["active"] for row in client.data[tables.MAIN_TABLE]} == {"a.se": 1, "b.se": 1, "c.se": 0}

    # A newer uncapped failure must supersede the old successful capped answer.
    payload = input_payload(COMPANY, "a.se", [sources[0]])
    failed = saved[0] | fingerprints(payload, PROFILE) | {"status": "http_error", "verified_at": datetime.now(UTC), "error": "timeout"}
    client.data[tables.VERIFICATION_TABLE].append(failed)
    batch.publish_domains(client, page_size=100, changed_only=True, profile=PROFILE, run_id="after-failure", log=lambda *args: None)
    assert next(row for row in client.data[tables.MAIN_TABLE] if row["root_domain"] == "a.se")["verification_status"] == "http_error"


@pytest.mark.parametrize("change", [
    {"system_prompt": "different instructions"}, {"model": "different-model"},
    {"base_url": "https://other.test/v1"}, {"temperature": .5}, {"max_tokens": 8_000},
])
def test_retired_cap_cache_reuse_still_requires_matching_prompt_and_model_settings(monkeypatch, change):
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    source = suggestion(association="uncertain")
    client = MemoryClient([source])
    old_profile = PROFILE.model_copy(update={"max_tokens": 4_000})
    payload = input_payload(COMPANY, "example.se", [source])
    client.data[tables.VERIFICATION_TABLE].append(verify_domain(
        FakeModel(), company_id=COMPANY["company_id"], domain="example.se", payload=payload,
        profile=old_profile, hashes=fingerprints(payload, old_profile), run_id="capped"))
    model = FakeModel()
    counts = batch.verify_domains(client, page_size=100, changed_only=True, profile=PROFILE.model_copy(update=change),
                                  max_llm_calls=1, run_id="changed-settings", log=lambda *args: None,
                                  retry_failed_only=False, llm_factory=lambda *args, **kwargs: model)
    assert counts["llm_calls"] == 1 and counts["verification_reused"] == 0


def test_verification_call_cap_is_shared_across_companies_and_pages(monkeypatch):
    second_company = COMPANY | {"company_id": "5560123456"}
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]], [second_company["company_id"]]]))
    client = MemoryClient([suggestion(domain="a.se", association="uncertain"), suggestion(domain="b.se", association="uncertain"),
                           suggestion(domain="c.se", association="uncertain", company_id=second_company["company_id"])])
    client.data["se_company_basic_info"].append(second_company)
    model = FakeModel()
    counts = batch.verify_domains(client, page_size=1, changed_only=True, profile=PROFILE, max_llm_calls=1,
                                  run_id="run-wide-cap", log=lambda *args: None, retry_failed_only=False,
                                  llm_factory=lambda *args, **kwargs: model)
    assert counts["companies"] == counts["pages"] == 2
    assert counts["eligible_associations"] == 3
    assert counts["llm_calls"] == len(model.calls) == 1
    assert counts["verification_pending"] == 2


def test_default_verification_processes_every_association_beyond_1000_and_reuses_results(monkeypatch):
    from dagster_v3.defs.se_company.domain.assets import DomainVerificationConfig

    config = DomainVerificationConfig(verification=PROFILE)
    assert config.max_llm_calls is None
    second_company = COMPANY | {"company_id": "5560123456"}
    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]], [second_company["company_id"]]]))
    sources = [suggestion(domain=f"domain-{index}.se", association="uncertain", company_id=company["company_id"])
               for company in (COMPANY, second_company) for index in range(550)]
    client = MemoryClient(sources)
    client.data["se_company_basic_info"].append(second_company)
    model = FakeModel()
    for run_id in ("first", "repeat"):
        counts = batch.verify_domains(client, page_size=1, changed_only=True, profile=config.verification,
                                      max_llm_calls=config.max_llm_calls, run_id=run_id, log=lambda *args: None,
                                      retry_failed_only=False, llm_factory=lambda *args, **kwargs: model)
        assert counts["eligible_associations"] == 1100
        assert counts["verification_pending"] == 0
        assert counts["llm_calls"] == (1100 if run_id == "first" else 0)
        assert counts["verification_reused"] == (0 if run_id == "first" else 1100)
    assert len(model.calls) == len(client.data[tables.VERIFICATION_TABLE]) == 1100


@pytest.mark.parametrize("failure_stage", ["startup", "insert"])
def test_clickhouse_outage_fails_asset_and_stops_further_llm_calls(monkeypatch, failure_stage):
    from contextlib import nullcontext
    from dagster_clickhouse import ClickhouseResource
    from dagster_v3.defs.se_company.domain import assets

    monkeypatch.setattr(batch, "scope_pages", lambda *args, **kwargs: (page for page in [[COMPANY["company_id"]]]))
    client = MemoryClient([suggestion(domain=domain, association="uncertain") for domain in ("a.se", "b.se", "c.se")])
    execute = client.execute
    def failing_execute(sql, params=None, settings=None):
        if "FROM system.tables" in sql:
            if failure_stage == "startup":
                raise ConnectionError("ClickHouse unavailable at startup")
            return [(name,) for name in tables.TABLES]
        if sql.startswith("INSERT") and len(client.data[tables.VERIFICATION_TABLE]) == 1:
            raise ConnectionError("ClickHouse unavailable while saving response")
        return execute(sql, params, settings)
    client.execute = failing_execute
    model = FakeModel()
    monkeypatch.setattr(ClickhouseResource, "get_connection", lambda self: nullcontext(client))
    monkeypatch.setattr(assets, "verify_domains", lambda *args, **kwargs: batch.verify_domains(
        *args, **kwargs, llm_factory=lambda *args, **kwargs: model))
    result = dg.materialize(
        [assets.se_company_domain_verification, assets.se_company_domain_publish,
         *(dg.AssetSpec(key) for key in assets.se_company_domain_verification.dependency_keys)],
        run_config={"ops": {"se_company_domain_verification": {"config": {"verification": PROFILE.model_dump()}}}},
        resources={"clickhouse": ClickhouseResource(host="localhost", user="test", password="", database="test")},
        raise_on_error=False,
    )
    assert not result.success
    assert len(model.calls) == (0 if failure_stage == "startup" else 2)
    assert len(client.data[tables.VERIFICATION_TABLE]) == (0 if failure_stage == "startup" else 1)
    assert not client.data[tables.MAIN_TABLE]
    assert not result.asset_materializations_for_node("se_company_domain_publish")


def test_distinct_source_support_beats_brave_and_duplicate_observations():
    rows = [suggestion('brave', domain='brave.se'),
            suggestion('esef_filing', domain='brave.se', slot='filing-one'),
            suggestion('esef_filing', domain='brave.se', slot='filing-two'),
            suggestion('wikidata', domain='supported.se', is_primary=0),
            suggestion('esef_filing', domain='supported.se', is_primary=0),
            suggestion('common_crawl_identity', domain='supported.se', is_primary=0)]
    result, _ = fold(rows)
    primary = next(row for row in result if row['is_primary'])
    assert primary['root_domain'] == 'supported.se'
    assert primary['supporting_sources'] == ['common_crawl_identity', 'esef_filing', 'wikidata']
    brave_row = next(row for row in result if row['root_domain'] == 'brave.se')
    assert brave_row['supporting_sources'] == ['brave', 'esef_filing']
    # One source with repeated records still loses to two independent sources.
    result, _ = fold([r for r in rows if r['source'] not in ('brave', 'common_crawl_identity')])
    assert next(row for row in result if row['is_primary'])['root_domain'] == 'supported.se'


def test_brave_breaks_equal_support_ties_but_never_overrides_human_primary():
    rows = [suggestion('brave', domain='brave.se'), suggestion('esef_filing', domain='reviewed.se')]
    result, _ = fold(rows)
    assert next(row for row in result if row['is_primary'])['root_domain'] == 'brave.se'
    for manual in ([rule('confirmed_primary', domain='reviewed.se')], []):
        sources = rows if manual else [*rows, suggestion('reviewer', domain='reviewed.se')]
        result, _ = fold(sources, rules=manual)
        primary = next(row for row in result if row['is_primary'])
        assert primary['root_domain'] == 'reviewed.se'
        assert primary['primary_source'] == 'reviewer'


def test_withdrawn_negative_and_disabled_sources_do_not_count_as_support():
    rows = [suggestion('brave'), suggestion('esef_filing', association='not_connected'),
            suggestion('wikidata', removed=1), suggestion('common_crawl_identity'),
            suggestion('reviewer_draft')]
    override = dict(company_id='', root_domain='', field='primary', source='common_crawl_identity', precedence=0, removed=0)
    result, _ = fold(rows, precedence=[override])
    assert result[0]['supporting_sources'] == ['brave']
    assert not result[0]['active']
    assert not result[0]['is_primary']
