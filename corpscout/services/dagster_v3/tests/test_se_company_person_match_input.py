"""Input-change semantics and safe reuse, independent of observation identifiers."""

import json
from dataclasses import replace
from datetime import timedelta

import pytest

from dagster_v3.defs.se_company.person import match, match_input, tables
from dagster_v3.defs.se_company.person.candidates import build_candidates
from dagster_v3.defs.se_company.person.normalize import normalize_companies
from tests.test_se_company_person_match import A, CONFIG, FakeClient, FakeModel, multi, one_pair, row
from tests.test_se_company_person_normalize import FakeClient as NormalizeClient
from tests.test_se_company_person_normalize import RAW_BV, RAW_ESEF, STAMP


def last_state(client):
    return dict(zip(tables.MATCH_STATE_COLUMNS, client.rows_for(tables.QUALIFIED_MATCH_STATE_TABLE)[-1], strict=True))


def stored_projection(state):
    return tuple(state[key] for key in (
        "company_id", "input_hash", "error", "data_hash", "bindings_hash", "prompt_hash",
        "model_hash", "input_snapshot", "config_snapshot", "raw_response", "prompt_tokens",
        "completion_tokens", "prompt_version",
    ))


def match_once(rows, *, config=CONFIG, state=()):
    client = FakeClient(scope_pages=[[A]], rows=rows, state=state)
    model = FakeModel({A: one_pair()})
    counts = match.run_match(client, llm_client=None, config=config, source_run_id="test-run", call_model=model)
    return client, model, counts


def test_data_hash_ignores_record_ids_order_and_duplicate_observations():
    rows = [row(normalized_id="a" * 64), row(first="bo", normalized_id="b" * 64), row("ratsit")]
    before = match_input.input_metadata(build_candidates(rows))
    # Reverse the within-source ID order and add a duplicate filing of Anna.
    updated = [replace(rows[0], normalized_id="z" * 64), replace(rows[1], normalized_id="c" * 64),
               rows[2], replace(rows[0], slot="new-filing", normalized_id="y" * 64)]
    after = match_input.input_metadata(build_candidates(list(reversed(updated))))
    assert before["data_hash"] == after["data_hash"]
    assert before["bindings_hash"] != after["bindings_hash"]
    assert after == match_input.input_metadata(match_input.snapshot_candidates(after["input_snapshot"]))


@pytest.mark.parametrize("change", [
    {"first": "maria"}, {"display": "Anna Maria Svensson"}, {"birth_year": 1975},
    {"data": '{"age":"51"}'}, {"data": '{"external":"true"}'},
    {"role_year": 2026}, {"role_code": "chief_executive_officer"},
    {"parse_status": "no_person"},
])
def test_every_piece_of_model_visible_information_changes_data_hash(change):
    before = match_input.input_metadata(build_candidates([row("ratsit"), row("bolagsverket")]))
    after = match_input.input_metadata(build_candidates([row("ratsit", **change), row("bolagsverket")]))
    assert before["data_hash"] != after["data_hash"]


def test_reviewer_and_irrelevant_source_metadata_do_not_change_hashes():
    before = match_input.input_metadata(build_candidates([row("ratsit"), row("bolagsverket")]))
    after = match_input.input_metadata(build_candidates([
        row("ratsit", data='{"document_ref":"new-download"}'), row("bolagsverket"), row("reviewer"),
    ]))
    assert before == after


def test_new_attempt_records_exact_input_and_effective_config_without_key():
    client, model, counts = match_once(multi(A))
    state = last_state(client)
    snapshot = match_input.snapshot_candidates(state["input_snapshot"])
    assert state["data_hash"] == match_input.input_metadata(snapshot)["data_hash"]
    config = json.loads(state["config_snapshot"])
    assert config["system_prompt"] == model.requests[0][1]["messages"][0]["content"]
    assert config["max_tokens"] == model.requests[0][1]["max_tokens"]
    assert config["extra_body"] == {"thinking": {"type": "disabled"}}
    assert "api_key" not in state["config_snapshot"]
    assert counts.called == 1


def test_same_data_reuses_state_and_keeps_match_watermark_unchanged():
    first, _, _ = match_once(multi(A))
    again, model, counts = match_once(multi(A), state=[stored_projection(last_state(first))])
    assert counts.reused == 1 and counts.called == 0
    assert model.requests == [] and again.inserts == []


def test_binding_only_change_replays_answer_with_current_members_without_call():
    rows = multi(A)
    first, _, _ = match_once(rows)
    state = last_state(first)
    updated = [replace(r, normalized_id=(str(i) * 64)) for i, r in enumerate(rows)]
    updated.append(replace(updated[0], slot="second-filing", normalized_id="d" * 64))
    again, model, counts = match_once(updated, state=[stored_projection(state)])
    after = last_state(again)
    pair = dict(zip(tables.MATCH_COLUMNS, again.rows_for(tables.QUALIFIED_MATCH_TABLE)[0], strict=True))
    assert model.requests == []
    assert counts.called == 0 and counts.reused == 1 and counts.rebound == 1
    assert counts.prompt_tokens == counts.completion_tokens == 0
    assert set(pair["members_a"] + pair["members_b"]) == {r.normalized_id for r in updated}
    assert after["input_hash"] == pair["input_hash"] != state["input_hash"]
    assert after["matched_at"] == pair["matched_at"]
    assert after["data_hash"] == state["data_hash"]
    assert after["raw_response"] == state["raw_response"]
    # Once rebound, the next run writes nothing and does not call again.
    final, model, counts = match_once(updated, state=[stored_projection(after)])
    assert counts.reused == 1 and not model.requests and not final.inserts


@pytest.mark.parametrize("update,called", [
    ({"prompt_version": "people:renamed:r20"}, 0),
    ({"system_prompt": "Different instructions"}, 1),
    ({"model": "other-model"}, 1),
    ({"temperature": 0.5}, 1),
    ({"max_tokens": 9000}, 1),
    ({"max_tokens": 256}, 0),  # Effective floor is still 4,000 for this company.
])
def test_configuration_compares_effective_content_not_revision(update, called):
    config = CONFIG.model_copy(update={"system_prompt": "Match people.", "prompt_version": "people:p:r1"})
    first, _, _ = match_once(multi(A), config=config)
    _, model, counts = match_once(multi(A), config=config.model_copy(update=update),
                                 state=[stored_projection(last_state(first))])
    assert len(model.requests) == counts.called == called


def input_writes(client):
    return [dict(zip(tables.MATCH_INPUT_COLUMNS, row, strict=True))
            for sql, rows, _ in client.statements
            if sql.startswith(f"INSERT INTO {tables.QUALIFIED_MATCH_INPUT_TABLE} ") for row in rows]


def test_normalization_hashes_full_company_after_delta_and_handles_withdrawals():
    client = NormalizeClient(scope_ids=[], rows=[RAW_BV, RAW_ESEF])
    normalize_companies(client, [RAW_BV[0]], changed_only=True, normalized_at=STAMP)
    initial = input_writes(client)[-1]
    assert initial["eligible"]
    # Only ESEF changed; Bolagsverket must remain in the rebuilt snapshot.
    changed = list(RAW_ESEF)
    changed[3], changed[4] = "c" * 64, "Svensson, Anna"
    client.rows = [tuple(changed)]
    normalize_companies(client, [RAW_BV[0]], changed_only=True, normalized_at=STAMP + timedelta(seconds=1))
    second = input_writes(client)[-1]
    assert {c.source for c in match_input.snapshot_candidates(second["input_snapshot"])} == {"esef", "bolagsverket"}
    assert second["data_hash"] != initial["data_hash"]
    # Withdraw the ESEF source: the previously eligible company needs an ineligible row.
    changed[3], changed[4] = "d" * 64, None
    client.rows = [tuple(changed)]
    normalize_companies(client, [RAW_BV[0]], changed_only=True, normalized_at=STAMP + timedelta(seconds=2))
    withdrawn = input_writes(client)[-1]
    assert not withdrawn["eligible"]
    assert withdrawn["data_hash"] != second["data_hash"]
    client.normalized.clear()
    match_input.refresh_inputs(client, [RAW_BV[0]])
    empty = input_writes(client)[-1]
    assert not empty["eligible"]
    assert match_input.snapshot_candidates(empty["input_snapshot"]) == ()


def test_retry_repairs_snapshot_even_when_normalized_insert_already_succeeded():
    client = NormalizeClient(scope_ids=[], rows=[RAW_BV, RAW_ESEF])
    execute = client.execute

    def interrupt_snapshot(sql, params=None, settings=None):
        if sql.startswith(f"INSERT INTO {tables.QUALIFIED_MATCH_INPUT_TABLE} "):
            raise RuntimeError("interrupted after normalized write")
        return execute(sql, params, settings)

    client.execute = interrupt_snapshot
    with pytest.raises(RuntimeError, match="interrupted"):
        normalize_companies(client, [RAW_BV[0]], changed_only=True, normalized_at=STAMP)
    client.execute = execute
    client.rows = []  # No suggestions now need re-normalizing.
    counts = normalize_companies(client, [RAW_BV[0]], changed_only=True, normalized_at=STAMP)
    assert counts.rows == 0
    assert input_writes(client)[-1]["eligible"]
