"""The Sweden company translation loader is keyed on the Bolagsverket register table
(spec 2026-09-07-se-translated-source-views), not on the retiring se_companies spine."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.sweden_company import translation
from dagster_v3.defs.translator_load import resource as translator_resource
from dagster_v3.defs.translator_load.loader import TranslationField, build_coverage_sql, build_scan_sql
from dagster_v3.defs.translator_load.resource import TranslatorResource
from tests.test_translator_load import _FakeSession


def test_activity_description_field_is_keyed_on_the_bolagsverket_register() -> None:
    assert translation.ACTIVITY_DESCRIPTION_FIELD == TranslationField(
        "corpscout.se_bolagsverket_companies", "activity_description", "sv", "en", extra_where="has_company = 1"
    )


def test_scan_and_coverage_read_register_rows_with_a_company() -> None:
    field = translation.ACTIVITY_DESCRIPTION_FIELD
    kwargs = dict(source_lang=field.source_lang, target_lang=field.target_lang, extra_where=field.extra_where)
    for sql in (build_scan_sql(field.table, field.column, **kwargs), build_coverage_sql(field.table, field.column, **kwargs)):
        assert "FROM corpscout.se_bolagsverket_companies AS c" in sql
        assert "source_table = 'corpscout.se_bolagsverket_companies' AND source_column = 'activity_description'" in sql
        assert "(has_company = 1)" in sql
        assert "se_companies'" not in sql


def test_load_asset_depends_on_the_register_export() -> None:
    asset = translation.sweden_company_translation_load
    assert {dep.asset_key for dep in asset.specs_by_key[asset.key].deps} == {
        dg.AssetKey("sweden_company_bolagsverket_companies_clickhouse")
    }
    assert asset.op.required_resource_keys == {"clickhouse", "translator"}


class _ScanClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, sql, params=None, **kwargs):
        self.queries.append(sql)
        return [("Handel med kaffe", 123)] if "text_translations" in sql else []


class _ScanResource:
    def __init__(self) -> None:
        self.client = _ScanClient()

    @contextmanager
    def get_connection(self):
        yield self.client


def test_load_asset_enqueues_under_the_field_key(monkeypatch) -> None:
    """The scan's anti-join and the enqueue must name the same key, or the loader
    re-enqueues every text on every run."""
    session = _FakeSession(stats={"input": 0, "pending": 0, "output": 0, "failed": 0})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    clickhouse = _ScanResource()

    result = translation.sweden_company_translation_load.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), clickhouse, TranslatorResource(base_url="http://translator:8080")
    )

    received = result.metadata["enqueued_received"]
    assert getattr(received, "value", received) == 1
    scan = clickhouse.client.queries[0]
    assert "FROM corpscout.se_bolagsverket_companies AS c" in scan and "(has_company = 1)" in scan
    (url, payload), = session.posts
    assert url.endswith("/v1/queue/items")
    assert payload["source_lang"] == "sv" and payload["target_lang"] == "en"
    assert payload["items"] == [
        {
            "source_table": "corpscout.se_bolagsverket_companies",
            "source_column": "activity_description",
            "source_text": "Handel med kaffe",
            "source_text_hash": "123",
        }
    ]
