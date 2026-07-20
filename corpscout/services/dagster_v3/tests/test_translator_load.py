"""Tests for the translator_load loader helpers and assets."""

import os
from contextlib import contextmanager

import dagster as dg
import pytest

from dagster_v3.defs.translator_load import resource as translator_resource
from dagster_v3.defs.translator_load.loader import (
    build_scan_sql,
    build_static_scan_sql,
    insert_static_translations,
)
from dagster_v3.defs.translator_load.resource import (
    TranslationQueueFailedError,
    TranslationQueueTimeoutError,
    TranslatorResource,
    translator_queue_health_check,
)


def test_build_scan_sql_is_anti_join_with_cityhash():
    sql = build_scan_sql("corpscout.no_companies", "activity_text_original")
    for fragment in (
        "SELECT DISTINCT",
        "c.activity_text_original AS source_text",
        "cityHash64(c.activity_text_original) AS source_text_hash",
        "FROM corpscout.no_companies AS c",
        "LEFT ANTI JOIN",
        "FROM corpscout.text_translations",
        "WHERE source_table = 'corpscout.no_companies' AND source_column = 'activity_text_original'",
        # Whitespace-only texts are excluded: the model returns an empty
        # translation for them, which becomes a PERMANENT failed queue item.
        "WHERE trim(BOTH ' \\t\\r\\n' FROM c.activity_text_original) != ''",
    ):
        assert fragment in sql, f"missing {fragment!r} in:\n{sql}"


def test_build_static_scan_sql_selects_key_column():
    sql = build_static_scan_sql(
        "corpscout.no_companies", "legal_form_description_original", "legal_form_code"
    )
    assert "c.legal_form_code AS legal_form_code" in sql
    assert "cityHash64(c.legal_form_description_original)" in sql


class _FakeSession:
    def __init__(
        self,
        *,
        inserted_per_call: int = 1,
        stats: dict[str, int] | list[dict[str, int]] | None = None,
        error: Exception | None = None,
        warning: str | None = None,
    ) -> None:
        self.posts: list[tuple[str, dict]] = []
        self._inserted = inserted_per_call
        self._stats = list(stats) if isinstance(stats, list) else [stats]
        self._error = error
        self._warning = warning

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def post(self, url: str, json: dict, timeout: int = 60):
        self.posts.append((url, json))
        received = len(json["items"])
        response_body = {"received": received, "inserted": self._inserted}
        if self._warning is not None:
            response_body["warning"] = self._warning

        class _Resp:
            status_code = 202

            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return _Resp(response_body)

    def get(self, url: str, timeout: int = 10):
        if self._error is not None:
            raise self._error

        class _Resp:
            def raise_for_status(self_inner):
                return None

            def json(self_inner):
                if len(self._stats) > 1:
                    return self._stats.pop(0)
                return self._stats[0]

        return _Resp()


def test_translator_resource_chunks_enqueues_and_surfaces_warnings(monkeypatch):
    session = _FakeSession(
        inserted_per_call=2,
        warning="items queued but workflow start failed",
    )
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    monkeypatch.setattr(translator_resource, "MAX_ITEMS_PER_REQUEST", 10)
    rows = [(f"text {i}", i) for i in range(25)]

    result = TranslatorResource(
        base_url="http://translator:8080"
    ).enqueue_translation_rows(
        source_table="corpscout.no_companies",
        source_column="activity_text_original",
        source_lang="no",
        target_lang="en",
        source_language_name="Norwegian",
        target_language_name="English",
        rows=rows,
    )

    assert len(session.posts) == 3  # 10 + 10 + 5
    assert result.received == 25
    assert result.inserted == 6
    assert result.workflow_start_warnings == (
        "items queued but workflow start failed",
        "items queued but workflow start failed",
        "items queued but workflow start failed",
    )
    url, payload = session.posts[0]
    assert url == "http://translator:8080/v1/queue/items"
    assert payload["source_lang"] == "no"
    assert payload["source_language_name"] == "Norwegian"
    item = payload["items"][0]
    assert item["source_table"] == "corpscout.no_companies"
    assert item["source_column"] == "activity_text_original"
    assert isinstance(item["source_text_hash"], str)  # decimal string, never int


class _FakeInsertClient:
    def __init__(self) -> None:
        self.executed: list[tuple[str, object]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        return []


def test_insert_static_translations_maps_and_skips_unknown_keys():
    client = _FakeInsertClient()
    rows = [("Aksjeselskap", 9001, "AS"), ("Ukjent", 9002, "NOPE")]

    inserted = insert_static_translations(
        client,
        "corpscout.no_companies",
        "legal_form_description_original",
        "no",
        "en",
        rows,
        {"AS": "Private limited company"},
    )

    assert inserted == 1
    sql, params = client.executed[-1]
    assert "text_translations" in sql
    (row,) = params
    assert row[:3] == (
        "corpscout.no_companies",
        "legal_form_description_original",
        9001,
    )
    assert row[6:8] == ("static", "static")


def test_stats_check_passes_and_reports_counts(monkeypatch):
    session = _FakeSession(stats={"input": 1, "pending": 1, "output": 0, "failed": 0})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    result = translator_queue_health_check(
        TranslatorResource(base_url="http://translator:8080")
    )

    assert result.passed is True
    assert result.metadata["input"].value == 1
    assert result.metadata["pending"].value == 1
    assert result.metadata["output"].value == 0
    assert result.metadata["failed"].value == 0


def test_stats_check_fails_when_queue_contains_failed_items(monkeypatch):
    session = _FakeSession(stats={"input": 4, "pending": 0, "output": 0, "failed": 4})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    result = translator_queue_health_check(
        TranslatorResource(base_url="http://translator:8080")
    )

    assert result.passed is False
    assert result.metadata["failed"].value == 4


def test_stats_check_fails_when_unreachable(monkeypatch):
    session = _FakeSession(error=RuntimeError("connection refused"))
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    result = translator_queue_health_check(
        TranslatorResource(base_url="http://translator:8080")
    )

    assert result.passed is False
    assert "connection refused" in result.metadata["error"].value


def test_wait_for_queue_completion_returns_only_after_flush(monkeypatch):
    session = _FakeSession(
        stats=[
            {"input": 2, "pending": 2, "output": 0, "failed": 0},
            {"input": 2, "pending": 0, "output": 2, "failed": 0},
            {"input": 0, "pending": 0, "output": 0, "failed": 0},
        ]
    )
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    monkeypatch.setattr(translator_resource.time, "sleep", lambda _seconds: None)

    stats = TranslatorResource(
        base_url="http://translator:8080",
        completion_timeout_seconds=10,
        completion_poll_interval_seconds=0.01,
    ).wait_for_queue_completion()

    assert stats.is_idle()
    assert stats.failed == 0


def test_wait_for_queue_completion_raises_for_failed_items(monkeypatch):
    session = _FakeSession(stats={"input": 19, "pending": 0, "output": 0, "failed": 19})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    with pytest.raises(
        TranslationQueueFailedError, match="19 failed translation items"
    ):
        TranslatorResource(
            base_url="http://translator:8080"
        ).wait_for_queue_completion()


def test_wait_for_queue_completion_times_out_when_processing_stalls(monkeypatch):
    session = _FakeSession(stats={"input": 1, "pending": 1, "output": 0, "failed": 0})
    monotonic_values = iter((0.0, 2.0))
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    monkeypatch.setattr(
        translator_resource.time,
        "monotonic",
        lambda: next(monotonic_values),
    )

    with pytest.raises(
        TranslationQueueTimeoutError, match="did not finish within 1 seconds"
    ):
        TranslatorResource(
            base_url="http://translator:8080",
            completion_timeout_seconds=1,
        ).wait_for_queue_completion()


class _FakeTranslationClickHouseClient:
    def execute(self, sql, params=None):
        if "articles_purpose_original" in sql:
            return [("Utvikling av programvare", 123)]
        return []


class _FakeTranslationClickHouseResource:
    @contextmanager
    def get_connection(self):
        yield _FakeTranslationClickHouseClient()


def test_norway_asset_does_not_materialize_when_translation_fails(monkeypatch):
    from dagster_v3.defs.norway_brreg.assets.translation import (
        norway_brreg_translation_load,
    )

    # First GET is the loader's pre-enqueue baseline (clean queue); the
    # failure appears DURING this run's wait -> the asset must fail.
    session = _FakeSession(
        stats=[
            {"input": 0, "pending": 0, "output": 0, "failed": 0},
            {"input": 1, "pending": 0, "output": 0, "failed": 1},
        ]
    )
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    with pytest.raises(TranslationQueueFailedError, match="1 failed translation items"):
        norway_brreg_translation_load.node_def.compute_fn.decorated_fn(
            dg.build_asset_context(),
            _FakeTranslationClickHouseResource(),
            TranslatorResource(base_url="http://translator:8080"),
        )


def test_wait_tolerates_pre_existing_failed_items_from_other_sources(monkeypatch):
    # Failed items are permanent in the queue db and stats are global: a
    # leftover failure from another source's load must not fail this run.
    # The wait is terminal when only the failed residue remains in input.
    session = _FakeSession(
        stats={"input": 19, "pending": 0, "output": 0, "failed": 19}
    )
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)

    stats = TranslatorResource(
        base_url="http://translator:8080"
    ).wait_for_queue_completion(baseline_failed=19)

    assert stats.failed == 19
    assert stats.pending == 0


def test_assets_are_defined_with_expected_deps():
    from dagster_v3.defs.latvia_ur import translation as latvia_translation
    from dagster_v3.defs.norway_brreg.assets import translation as norway_translation

    norway = norway_translation.norway_brreg_translation_load
    assert dg.AssetKey("norway_brreg_entities_snapshot_clickhouse") in {
        dep.asset_key for dep in norway.specs_by_key[norway.key].deps
    }
    assert norway.op.required_resource_keys == {"clickhouse", "translator"}

    latvia = latvia_translation.latvia_ur_translation_load
    assert dg.AssetKey("latvia_ur_clickhouse_companies") in {
        dep.asset_key for dep in latvia.specs_by_key[latvia.key].deps
    }
    assert latvia.op.required_resource_keys == {"clickhouse", "translator"}


@pytest.mark.integration
@pytest.mark.skipif(
    os.environ.get("RUN_TRANSLATION_INTEGRATION_TESTS") != "1",
    reason="set RUN_TRANSLATION_INTEGRATION_TESTS=1 and CLICKHOUSE_* env vars to run",
)
def test_scan_sql_executes_against_real_clickhouse():
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "localhost"),
        port=int(os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
    )
    for table, column in (
        ("corpscout.no_companies", "articles_purpose_original"),
        ("corpscout.no_companies", "activity_text_original"),
        ("corpscout.lv_companies", "activity_text_original"),
    ):
        sql = build_scan_sql(table, column)
        count = client.query(f"SELECT count() FROM ({sql})").result_rows[0][0]
        assert count >= 0  # proves the anti-join shape is valid against the real schema
    static_sql = build_static_scan_sql(
        "corpscout.no_companies", "legal_form_description_original", "legal_form_code"
    )
    assert client.query(f"SELECT count() FROM ({static_sql})").result_rows[0][0] >= 0
