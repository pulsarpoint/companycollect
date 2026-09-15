"""The actual migration, full-company reads and repair scan on ClickHouse."""

from datetime import timedelta

import pytest

from dagster_v3.defs.se_company.person import match, match_input, tables
from tests.test_se_company_person_fold_clickhouse_local import _LocalClient, _normalized_insert
from tests.test_se_company_person_normalize import RAW_BV, RAW_ESEF, STAMP

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("join_use_nulls", [0, 1])
def test_repair_detects_missing_stale_withdrawn_and_deleted_companies(join_use_nulls):
    client = _LocalClient(join_use_nulls)
    company_id = RAW_BV[0]
    client.add(_normalized_insert([RAW_BV, RAW_ESEF], STAMP))
    assert client.execute(match_input.stale_scope_sql()) == [(company_id,)]
    match_input.refresh_inputs(client, [company_id])
    assert client.execute(match_input.stale_scope_sql()) == []
    initial = client.execute(match_input.current_inputs_sql(), {"company_ids": [company_id]})[0]
    assert initial[4] is True
    assert client.execute(match.match_scope_sql()) == [(company_id,)]
    assert {c.source for c in match_input.snapshot_candidates(initial[3])} == {"bolagsverket", "esef"}
    # Normalized data landed but the process died before refreshing the input.
    tombstone = list(RAW_ESEF)
    tombstone[3], tombstone[4] = "d" * 64, None
    client.add(_normalized_insert([tuple(tombstone)], STAMP + timedelta(seconds=1)))
    assert client.execute(match_input.stale_scope_sql()) == [(company_id,)]
    match_input.refresh_inputs(client, [company_id])
    current = client.execute(match_input.current_inputs_sql(), {"company_ids": [company_id]})[0]
    assert current[4] is False
    assert initial[1] != current[1] and initial[2] != current[2]
    assert client.execute(match.match_scope_sql()) == []
    assert client.execute(match_input.stale_scope_sql()) == []
    # Even an exceptional removal of all normalized rows gets an empty snapshot.
    client.add(f"TRUNCATE TABLE {tables.QUALIFIED_NORMALIZED_TABLE}")
    assert client.execute(match_input.stale_scope_sql()) == [(company_id,)]
    match_input.refresh_inputs(client, [company_id])
    assert client.execute(match_input.stale_scope_sql()) == []
    empty = client.execute(match_input.current_inputs_sql(), {"company_ids": [company_id]})[0]
    assert match_input.snapshot_candidates(empty[3]) == ()


def test_state_additions_preserve_unknown_legacy_fingerprints():
    client = _LocalClient(0)
    client.add(
        f"INSERT INTO {tables.QUALIFIED_MATCH_STATE_TABLE} (company_id, input_hash, matched_at) "
        "VALUES ('5561552760', repeat('a', 64), now64(3))"
    )
    state = client.execute(match.match_state_sql(), {"company_ids": [RAW_BV[0]]})[0]
    assert state[3:9] == ("", "", "", "", "", "")


def test_match_and_binding_replay_write_state_that_certifies_current_fold_pairs():
    from dagster_v3.defs.se_company.person import batch
    from tests.test_se_company_person_match import CONFIG, FakeModel, one_pair

    client = _LocalClient(0)
    company_id = RAW_BV[0]
    config = CONFIG.model_copy(update={"company_ids": [company_id]})
    client.add(_normalized_insert([RAW_BV, RAW_ESEF], STAMP))
    match_input.refresh_inputs(client, [company_id])
    model = FakeModel({company_id: one_pair()})
    first = match.run_match(client, llm_client=None, config=config, source_run_id="first", call_model=model)
    assert first.called == 1 and len(model.requests) == 1
    original = client.execute(batch.match_pairs_sql(), {"company_ids": [company_id]})
    assert len(original) == 1
    # Identical observation content under a new normalized record ID.
    updated = list(RAW_BV)
    updated[3] = "f" * 64
    client.add(_normalized_insert([tuple(updated)], STAMP + timedelta(seconds=1)))
    match_input.refresh_inputs(client, [company_id])
    rebound = match.run_match(client, llm_client=None, config=config, source_run_id="rebound", call_model=model)
    assert rebound.rebound == 1 and rebound.called == 0 and len(model.requests) == 1
    current = client.execute(batch.match_pairs_sql(), {"company_ids": [company_id]})
    assert len(current) == 1 and current != original
    state = client.execute(match.match_state_sql(), {"company_ids": [company_id]})[0]
    assert len(state[3]) == len(state[4]) == len(state[5]) == len(state[6]) == 64


def test_normalizer_scope_includes_snapshot_repair_without_raw_changes():
    from dagster_v3.defs.se_company.person.normalize import changed_scope_sql
    from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION

    client = _LocalClient(0)
    client.add(_normalized_insert([RAW_BV, RAW_ESEF], STAMP))
    # This is the combined scope normalize_all hands to the shared scratch-table pager.
    scope = f"SELECT DISTINCT company_id FROM ({changed_scope_sql()} UNION DISTINCT {match_input.stale_scope_sql()})"
    params = {"normalizer_version": NORMALIZER_VERSION}
    assert client.execute(scope, params) == [(RAW_BV[0],)]
    match_input.refresh_inputs(client, [RAW_BV[0]])
    assert client.execute(scope, params) == []
