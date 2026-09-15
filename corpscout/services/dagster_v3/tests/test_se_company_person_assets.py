"""People publication and targeted correction assets: resources, pools and execution order."""

from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_v3.defs.se_company.person import assets, batch, match, tables


def test_publication_is_unpartitioned_and_corrections_remain_available() -> None:
    for asset in (assets.se_company_person_publish, assets.se_company_person_fold_companies):
        assert asset.partitions_def is None
        assert set(asset.required_resource_keys) >= {"clickhouse"}
        assert asset.group_names_by_key[asset.key] == assets.GROUP_NAME


def test_all_normalization_writers_share_the_snapshot_pool() -> None:
    """The bulk fold has its own pool; all normalized/input writers share one pool."""
    assert assets.se_company_person_publish.op.pool == assets.FOLD_POOL
    assert assets.FOLD_POOL == "se_company_person_fold"
    assert assets.se_company_person_fold_companies.op.pool == assets.NORMALIZE_POOL
    assert assets.se_company_person_match_input.op.pool == assets.NORMALIZE_POOL


def test_the_normalize_asset_keeps_its_own_pool() -> None:
    """The normalize asset's pool stays as slice 0 shipped it, distinct from FOLD_POOL."""
    assert assets.se_company_person_normalize.op.pool == assets.NORMALIZE_POOL
    assert assets.NORMALIZE_POOL != assets.FOLD_POOL


def test_config_defaults_and_bounds() -> None:
    assert assets.PersonFoldConfig().changed_only is True
    assert assets.PersonFoldConfig().page_size == batch.PAGE_SIZE
    with pytest.raises(ValueError):
        assets.PersonFoldConfig(page_size=0)
    targeted = assets.PersonFoldCompaniesConfig(company_ids=["5560000002", "5560000001", "5560000001"])
    assert targeted.company_ids == ["5560000001", "5560000002"] and targeted.changed_only is False
    with pytest.raises(ValueError):
        assets.PersonFoldCompaniesConfig(company_ids=[])


def test_targeted_fold_normalizes_the_ids_before_folding_them(monkeypatch) -> None:
    """Spec section 7: Fold now must parse a reviewer's brand-new suggestion, so the
    targeted asset normalizes first -- always changed_only, so it touches only rows that
    need it -- and then folds the same ids with the caller's changed_only."""
    from datetime import UTC, datetime

    calls: list[tuple] = []

    def fake_normalize(client, ids, *, changed_only, normalized_at, page_size, log):
        calls.append(("normalize", list(ids), changed_only))
        return SimpleNamespace(as_metadata=lambda: {"rows": 3})

    def fake_fold(client, ids, *, changed_only, source_run_id, folded_at, page_size, log):
        calls.append(("fold", list(ids), changed_only, source_run_id))
        return SimpleNamespace(as_metadata=lambda: {"persons": 2})

    monkeypatch.setattr(assets, "normalize_companies", fake_normalize)
    monkeypatch.setattr(assets, "fold_companies", fake_fold)
    now = datetime(2026, 9, 10, 20, 0, tzinfo=UTC)
    normalized, folded = assets.targeted_fold(
        object(), ["5560000001"], changed_only=False, source_run_id="run-1", folded_at=now,
        page_size=20_000, log=None, logger=None,
    )
    assert calls == [("normalize", ["5560000001"], True), ("fold", ["5560000001"], False, "run-1")]
    assert normalized.as_metadata() == {"rows": 3} and folded.as_metadata() == {"persons": 2}


def test_the_match_asset_is_pooled_grouped_and_retried() -> None:
    asset = assets.se_company_person_match
    assert asset.op.pool == assets.MATCH_POOL == "se_company_person_match"
    assert asset.group_names_by_key[asset.key] == assets.GROUP_NAME
    assert set(asset.required_resource_keys) >= {"clickhouse"}
    # One pool of limit 1 (the instance default), so two runs never race the same companies.
    assert assets.MATCH_POOL not in {assets.NORMALIZE_POOL, assets.FOLD_POOL}
    policy = asset.op.retry_policy
    assert (policy.max_retries, policy.delay, policy.backoff) == (3, 60, dg.Backoff.EXPONENTIAL)
    assert asset.partitions_def is None


def test_the_match_asset_description_states_the_retry_rule() -> None:
    """What the UI tells an operator about a re-run has to be the rule the code applies:
    which errors come back next run, which are sticky, and what the counter for those is
    called in the materialization's metadata."""
    description = assets.se_company_person_match.descriptions_by_key[
        assets.se_company_person_match.key
    ]
    for phrase in (*match.TRANSIENT_ERROR_PREFIXES, "STICKY", "skipped_sticky",
                   "input_hash", "candidate cap"):
        assert phrase in description, phrase
    assert "whose last attempt errored" not in description        # the pre-fix-wave rule
    assert "skipped_sticky" in match.MatchCounts(
        companies=0, pages=0, called=0, reused=0, skipped_sticky=0, skipped_single_source=0,
        pairs=0, pairs_above_threshold=0, errors=0, prompt_tokens=0, completion_tokens=0,
        stopped_at_cap=False,
    ).as_metadata()


def test_the_match_asset_runs_after_the_normalizer() -> None:
    from dagster_v3.definitions import defs as load_defs

    node = load_defs().get_repository_def().asset_graph.get(
        dg.AssetKey("se_company_person_match"))
    assert {key.path[-1] for key in node.parent_keys} == {"se_company_person_match_input"}


def test_the_targeted_fold_reads_the_match_tables_too() -> None:
    """The fold's page read now joins the pair table, so the existence assertion must name
    it -- otherwise a fold on a host without 000399 fails deep inside a page."""
    assert tables.MATCH_TABLE in assets._FOLD_TABLES
    assert tables.MATCH_STATE_TABLE in assets._FOLD_TABLES
