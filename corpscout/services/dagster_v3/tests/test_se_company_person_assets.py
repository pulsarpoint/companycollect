"""Wiring of the person fold assets: 64 bucket partitions, NO pool (this fold opens no
DuckDB, so buckets run in parallel), config bounds, and the targeted fold's order."""

from types import SimpleNamespace

import dagster as dg
import pytest

from dagster_v3.defs.se_company.person import assets, batch


def test_the_fold_has_sixty_four_bucket_partitions_and_no_pool() -> None:
    keys = assets.PERSON_FOLD_PARTITIONS.get_partition_keys()
    assert keys[0] == "bucket_00" and keys[-1] == "bucket_63" and len(keys) == batch.BUCKET_COUNT
    fold = assets.se_company_person_fold
    assert fold.partitions_def is assets.PERSON_FOLD_PARTITIONS
    assert fold.backfill_policy == dg.BackfillPolicy.multi_run(max_partitions_per_run=1)
    for asset in (fold, assets.se_company_person_fold_companies):
        assert asset.op.pool is None
        assert set(asset.required_resource_keys) >= {"clickhouse"}
        assert asset.group_names_by_key[asset.key] == assets.GROUP_NAME


def test_the_normalize_asset_keeps_its_own_pool() -> None:
    """The fold has no pool; the normalize asset's stays as slice 0 shipped it."""
    assert assets.se_company_person_normalize.op.pool == assets.NORMALIZE_POOL


def test_bucket_index_parses_and_refuses() -> None:
    assert assets.person_bucket_index("bucket_07") == 7
    with pytest.raises(ValueError):
        assets.person_bucket_index("bucket_64")
    with pytest.raises(ValueError):
        assets.person_bucket_index("07")


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
