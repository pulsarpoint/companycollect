"""Tests for the translator_load loader helpers and assets."""

import os

import pytest

from dagster_v3.defs.translator_load.loader import (
    LoaderField,
    LoaderSource,
    build_scan_sql,
    build_static_scan_sql,
    enqueue_items,
    insert_static_translations,
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
        "WHERE c.activity_text_original <> ''",
    ):
        assert fragment in sql, f"missing {fragment!r} in:\n{sql}"


def test_build_static_scan_sql_selects_key_column():
    sql = build_static_scan_sql(
        "corpscout.no_companies", "legal_form_description_original", "legal_form_code"
    )
    assert "c.legal_form_code AS legal_form_code" in sql
    assert "cityHash64(c.legal_form_description_original)" in sql


class _FakeSession:
    def __init__(self, inserted_per_call: int = 1) -> None:
        self.posts: list[tuple[str, dict]] = []
        self._inserted = inserted_per_call

    def post(self, url: str, json: dict, timeout: int = 60):
        self.posts.append((url, json))
        received = len(json["items"])

        class _Resp:
            status_code = 202

            def __init__(self, payload):
                self._payload = payload

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        return _Resp({"received": received, "inserted": self._inserted})


def _norway_source() -> LoaderSource:
    return LoaderSource(
        source_lang="no",
        target_lang="en",
        source_language_name="Norwegian",
        target_language_name="English",
        fields=(LoaderField("corpscout.no_companies", "activity_text_original"),),
    )


def test_enqueue_items_chunks_and_sums():
    session = _FakeSession(inserted_per_call=2)
    rows = [(f"text {i}", i) for i in range(25)]

    totals = enqueue_items(
        session,
        "http://translator:8080",
        _norway_source(),
        _norway_source().fields[0],
        rows,
        chunk_size=10,
    )

    assert len(session.posts) == 3  # 10 + 10 + 5
    assert totals == {"received": 25, "inserted": 6}
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
    assert row[:3] == ("corpscout.no_companies", "legal_form_description_original", 9001)
    assert row[6:8] == ("static", "static")


class _FakeStatsSession:
    def __init__(self, payload: dict | None = None, error: Exception | None = None) -> None:
        self._payload = payload
        self._error = error

    def get(self, url: str, timeout: int = 10):
        if self._error is not None:
            raise self._error

        class _Resp:
            def raise_for_status(self_inner):
                return None

            def json(self_inner):
                return self._payload

        return _Resp()


def test_stats_check_passes_and_reports_counts():
    from dagster_v3.defs.translator_load.assets import _stats_check

    session = _FakeStatsSession(payload={"input": 1, "pending": 2, "output": 3, "failed": 4})
    result = _stats_check(session=session)

    assert result.passed is True
    assert result.metadata["input"].value == 1
    assert result.metadata["pending"].value == 2
    assert result.metadata["output"].value == 3
    assert result.metadata["failed"].value == 4


def test_stats_check_fails_when_unreachable():
    from dagster_v3.defs.translator_load.assets import _stats_check

    session = _FakeStatsSession(error=RuntimeError("connection refused"))
    result = _stats_check(session=session)

    assert result.passed is False
    assert "connection refused" in result.metadata["error"].value


def test_assets_are_defined_with_expected_deps():
    from dagster_v3.defs.translator_load import assets as translator_assets

    import dagster as dg

    norway = translator_assets.norway_brreg_translation_load
    assert dg.AssetKey("norway_brreg_entities_snapshot_clickhouse") in {
        dep.asset_key for dep in norway.specs_by_key[norway.key].deps
    }

    latvia = translator_assets.latvia_ur_translation_load
    assert dg.AssetKey("latvia_ur_clickhouse_companies") in {
        dep.asset_key for dep in latvia.specs_by_key[latvia.key].deps
    }


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
