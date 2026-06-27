"""Tests for translator/import_legacy.py — one-time legacy-queue import CLI."""
from pathlib import Path

from translator.import_legacy import main
from translator.queue import TranslationQueue, TranslationQueueItem
from translator.types import TranslationResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _item(field: str, text: str) -> TranslationQueueItem:
    return TranslationQueueItem(
        source_duckdb_path="clickhouse",
        source_table="corpscout.companies",
        source_pk="1",
        source_field=field,
        source_text=text,
        target_language="en",
    )


def _build_old_queue(path: Path) -> TranslationQueue:
    """Create a synthetic old-style queue with 4 fields, all completed.

    The old queue records source_field as the *_original COLUMN name (as the real
    norway_brreg queue does), which the importer must remap to the logical field.

    source_field → outcome:
        activity_text_original          — dynamic → imported as 'activity_text'
        articles_purpose_original       — dynamic → imported as 'articles_purpose'
        legal_form_description_original — static  → skipped
        bogus_field                     — unknown → skipped
    """
    queue = TranslationQueue(path)
    queue.initialize()
    items = [
        _item("activity_text_original", "Holdingselskap"),
        _item("articles_purpose_original", "Produksjon av software"),
        _item("legal_form_description_original", "Aksjeselskap"),
        _item("bogus_field", "Noe ukjent"),
    ]
    queue.enqueue_items(items)
    claimed = queue.claim_batch(limit=10, worker_id="test-worker")
    translations = [
        TranslationResult(
            item_id=c.item_id,
            translated_text=f"translated_{c.source_text}",
        )
        for c in claimed
    ]
    queue.complete_batch(
        claimed,
        translations,
        provider="test",
        model="test-model",
        duration_seconds=0.1,
    )
    return queue


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_import_only_dynamic_fields(tmp_path, monkeypatch):
    """Only dynamic (non-static) fields present in the registry are flushed."""
    db_path = tmp_path / "old_queue.duckdb"
    _build_old_queue(db_path)

    flushed_calls: list[dict] = []

    def fake_flush(client, config, rows, *, provider, model, version, run_id):
        flushed_calls.append(
            {"config": config, "rows": list(rows), "provider": provider, "model": model, "run_id": run_id}
        )
        return len(rows)

    monkeypatch.setattr("translator.import_legacy.clickhouse_client_from_env", lambda: object())
    monkeypatch.setattr("translator.import_legacy.flush_translations", fake_flush)

    rc = main(["--duckdb", str(db_path), "--source", "norway_brreg", "--env-file", str(tmp_path / "no.env")])

    assert rc == 0
    assert len(flushed_calls) == 1

    call = flushed_calls[0]
    imported_fields = {r.source_column for r in call["rows"]}
    assert imported_fields == {"activity_text_original", "articles_purpose_original"}
    assert "legal_form_description_original" not in imported_fields, "static field must be skipped"
    assert "bogus_field" not in imported_fields, "unknown field must be skipped"

    assert call["provider"] == "legacy-import"
    assert call["model"] == "legacy"
    assert call["run_id"].startswith("legacy-import")


def test_import_counts_are_correct(tmp_path, monkeypatch):
    """Exactly 2 rows (one per dynamic field) reach flush_translations."""
    db_path = tmp_path / "old_queue.duckdb"
    _build_old_queue(db_path)

    captured: list[list] = []

    def fake_flush(client, config, rows, *, provider, model, version, run_id):
        captured.append(list(rows))
        return len(rows)

    monkeypatch.setattr("translator.import_legacy.clickhouse_client_from_env", lambda: object())
    monkeypatch.setattr("translator.import_legacy.flush_translations", fake_flush)

    rc = main(["--duckdb", str(db_path), "--source", "norway_brreg", "--env-file", str(tmp_path / "no.env")])

    assert rc == 0
    assert len(captured) == 1
    assert len(captured[0]) == 2, "only the 2 dynamic-field rows should reach flush"


def test_dry_run_writes_nothing(tmp_path, monkeypatch):
    """--dry-run scans but never calls flush_translations or clickhouse_client_from_env."""
    db_path = tmp_path / "old_queue.duckdb"
    _build_old_queue(db_path)

    flush_calls: list = []
    client_calls: list = []

    monkeypatch.setattr(
        "translator.import_legacy.clickhouse_client_from_env",
        lambda: client_calls.append(1) or object(),
    )
    monkeypatch.setattr(
        "translator.import_legacy.flush_translations",
        lambda *a, **kw: flush_calls.append(a) or 0,
    )

    rc = main(["--duckdb", str(db_path), "--source", "norway_brreg", "--dry-run", "--env-file", str(tmp_path / "no.env")])

    assert rc == 0
    assert flush_calls == [], "--dry-run must not call flush_translations"
    assert client_calls == [], "--dry-run must not open a ClickHouse client"


def test_missing_duckdb_returns_nonzero(tmp_path):
    """A non-existent --duckdb path returns exit code 1."""
    rc = main(["--duckdb", str(tmp_path / "does_not_exist.duckdb"), "--env-file", str(tmp_path / "no.env")])
    assert rc == 1


def test_unknown_source_returns_nonzero(tmp_path):
    """An unregistered --source slug returns exit code 1."""
    db_path = tmp_path / "q.duckdb"
    TranslationQueue(db_path).initialize()
    rc = main(["--duckdb", str(db_path), "--source", "nonexistent_source", "--env-file", str(tmp_path / "no.env")])
    assert rc == 1


def test_empty_translated_text_excluded(tmp_path, monkeypatch):
    """Rows with empty translated_text are dropped before flush."""
    db_path = tmp_path / "q.duckdb"
    queue = TranslationQueue(db_path)
    queue.initialize()
    # Enqueue one item; complete it with an empty translation.
    items = [_item("activity_text_original", "Tom tekst")]
    queue.enqueue_items(items)
    claimed = queue.claim_batch(limit=10, worker_id="w")
    queue.complete_batch(
        claimed,
        [TranslationResult(item_id=claimed[0].item_id, translated_text="")],
        provider="p",
        model="m",
        duration_seconds=0.1,
    )

    flush_calls: list = []
    monkeypatch.setattr("translator.import_legacy.clickhouse_client_from_env", lambda: object())
    monkeypatch.setattr(
        "translator.import_legacy.flush_translations",
        lambda *a, **kw: flush_calls.append(kw) or 0,
    )

    rc = main(["--duckdb", str(db_path), "--source", "norway_brreg", "--env-file", str(tmp_path / "no.env")])

    assert rc == 0
    # empty translated_text -> nothing to import -> flush_translations never called
    assert flush_calls == [], "empty translation must not reach flush"
