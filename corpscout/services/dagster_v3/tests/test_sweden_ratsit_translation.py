"""Ratsit business descriptions join the translator pipeline under their own table's key
(spec 2026-09-07-se-translated-source-views)."""

from contextlib import contextmanager

import dagster as dg

from dagster_v3.defs.czech_legal_forms.assets import TRANSLATION_LOAD_ASSETS
from dagster_v3.defs.sweden_ratsit import translation
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from dagster_v3.defs.translator_load import resource as translator_resource
from dagster_v3.defs.translator_load.loader import TranslationField, build_coverage_sql, build_scan_sql
from dagster_v3.defs.translator_load.resource import TranslatorResource
from tests.test_translator_load import _FakeSession


def test_business_description_field_is_keyed_on_the_ratsit_table_and_scoped_to_the_normalizer() -> None:
    assert translation.BUSINESS_DESCRIPTION_FIELD == TranslationField(
        "corpscout.se_ratsit_company",
        "business_description",
        "sv",
        "en",
        extra_where=f"normalizer_version = '{RATSIT_NORMALIZER_VERSION}'",
    )


def test_scan_and_coverage_share_the_normalizer_scope() -> None:
    field = translation.BUSINESS_DESCRIPTION_FIELD
    kwargs = dict(source_lang=field.source_lang, target_lang=field.target_lang, extra_where=field.extra_where)
    for sql in (build_scan_sql(field.table, field.column, **kwargs), build_coverage_sql(field.table, field.column, **kwargs)):
        assert "FROM corpscout.se_ratsit_company AS c" in sql
        assert "source_table = 'corpscout.se_ratsit_company' AND source_column = 'business_description'" in sql
        assert f"(normalizer_version = '{RATSIT_NORMALIZER_VERSION}')" in sql


def test_load_asset_follows_the_normalizer_and_is_covered_by_the_ten_minute_job() -> None:
    asset = translation.sweden_ratsit_translation_load
    assert asset.key == dg.AssetKey("sweden_ratsit_translation_load")
    assert {dep.asset_key for dep in asset.specs_by_key[asset.key].deps} == {dg.AssetKey("se_ratsit_normalized")}
    assert asset.specs_by_key[asset.key].group_name == "sweden_ratsit"
    assert asset.op.required_resource_keys == {"clickhouse", "translator"}
    assert "sweden_ratsit_translation_load" in TRANSLATION_LOAD_ASSETS


class _ScanClient:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute(self, sql, params=None, **kwargs):
        self.queries.append(sql)
        return [("Ny text", 456)] if "text_translations" in sql else []


class _ScanResource:
    def __init__(self) -> None:
        self.client = _ScanClient()

    @contextmanager
    def get_connection(self):
        yield self.client


def test_load_asset_enqueues_under_the_field_key(monkeypatch) -> None:
    session = _FakeSession(stats={"input": 0, "pending": 0, "output": 0, "failed": 0})
    monkeypatch.setattr(translator_resource.requests, "Session", lambda: session)
    clickhouse = _ScanResource()

    result = translation.sweden_ratsit_translation_load.node_def.compute_fn.decorated_fn(
        dg.build_asset_context(), clickhouse, TranslatorResource(base_url="http://translator:8080")
    )

    received = result.metadata["enqueued_received"]
    assert getattr(received, "value", received) == 1
    assert f"(normalizer_version = '{RATSIT_NORMALIZER_VERSION}')" in clickhouse.client.queries[0]
    (url, payload), = session.posts
    assert url.endswith("/v1/queue/items")
    assert payload["source_language_name"] == "Swedish" and payload["target_language_name"] == "English"
    assert payload["items"] == [
        {
            "source_table": "corpscout.se_ratsit_company",
            "source_column": "business_description",
            "source_text": "Ny text",
            "source_text_hash": "456",
        }
    ]
