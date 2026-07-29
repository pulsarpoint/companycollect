"""Brazil PNCP contract-object translation.

112,943 contracts carried Portuguese-only objects while the page labels them in
English, and text_translations held zero PNCP rows -- the design doc's §6
described a translation.py that did not exist.
"""

import importlib

import dagster as dg

from dagster_v3.defs.brazil_pncp.assets import defs
from dagster_v3.defs.brazil_pncp.translation import (
    SOURCE_LANG,
    SOURCE_LANGUAGE_NAME,
    TARGET_LANG,
    TRANSLATION_FIELDS,
)
from dagster_v3.defs.translator_load.loader import build_scan_sql


def test_the_loader_is_registered() -> None:
    """An @dg.asset missing from Definitions(assets=[...]) is silently orphaned:
    it never appears in the UI and never runs, and both `dg check defs` and
    pytest pass regardless."""
    names = {a.key.to_user_string() for a in defs.assets}

    assert "brazil_pncp_translation_load" in names


def test_it_runs_after_both_clickhouse_landing_paths() -> None:
    """The backfill chain and the daily chain each land untranslated rows, so a
    loader wired to only one would leave the other's contracts untranslated
    forever."""
    by_key = {a.key.to_user_string(): a for a in defs.assets}
    asset = by_key["brazil_pncp_translation_load"]

    assert {d.asset_key for d in asset.specs_by_key[asset.key].deps} == {
        dg.AssetKey("brazil_pncp_contracts_clickhouse"),
        dg.AssetKey("brazil_pncp_daily_clickhouse"),
    }


def test_it_translates_the_contract_object_from_portuguese() -> None:
    assert SOURCE_LANG == "pt"
    assert TARGET_LANG == "en"
    assert SOURCE_LANGUAGE_NAME == "Portuguese"
    assert [(f.table, f.column) for f in TRANSLATION_FIELDS] == [
        ("corpscout.br_pncp_contracts", "objeto_contrato")
    ]


def test_it_scans_the_table_not_the_view() -> None:
    """br_government_contracts filters to company_match_status = 'exact', which
    drops 3,283 contracts. Scanning the view would leave those objects
    permanently untranslated -- including every award to a natural person."""
    field = TRANSLATION_FIELDS[0]
    sql = build_scan_sql(field.table, field.column)

    assert "br_pncp_contracts" in sql
    assert "br_government_contracts" not in sql


def test_the_scan_only_asks_for_texts_it_does_not_already_have() -> None:
    """Dedup is the whole economics: 116,226 rows carry 57,229 distinct objects,
    and one object repeats 6,979 times."""
    field = TRANSLATION_FIELDS[0]
    sql = build_scan_sql(field.table, field.column)

    assert "DISTINCT" in sql
    assert "LEFT ANTI JOIN" in sql
    assert "corpscout.text_translations" in sql
    # The two guards that previously froze the shared queue: whitespace-only
    # texts became permanent failures, and one 1.8M-char blob stalled 1.9M
    # pending rows. Brazil has 0 of each today, so these must not regress.
    assert "!= ''" in sql
    assert "length" in sql and "8000" in sql


def test_the_queue_health_check_is_attached() -> None:
    """The translator queue is shared across every source, so a poisoned item
    from one stalls all of them. The check has to be registered, not just
    defined."""
    specs = {
        (spec.asset_key.to_user_string(), spec.name)
        for checks_def in defs.asset_checks or ()
        for spec in checks_def.check_specs
    }

    assert ("brazil_pncp_translation_load", "translator_queue_healthy") in specs


def test_the_loader_uses_no_static_map() -> None:
    """Unlike Norway's legal-form codes, the only PNCP field worth translating is
    free text. tipo_contrato and categoria_processo are closed domains decoded in
    the UI, so nothing here belongs in a static map -- and the closed domains
    must not be sent to an LLM either."""
    module = importlib.import_module("dagster_v3.defs.brazil_pncp.translation")

    assert not hasattr(module, "insert_static_translations")
    assert not hasattr(module, "build_static_scan_sql")
