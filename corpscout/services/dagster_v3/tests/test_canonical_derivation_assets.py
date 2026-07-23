"""Lock-step tests for canonical contact/domain derivation assets.

The Task 2 backfill migrations (clickhouse/migrations/000097_*, 000098_*, 000099_*)
contain the authoritative INSERT SELECT bodies for reshaping each source's
*_websites table into the canonical *_company_contacts/*_company_domains pair. The
Finland and Wikidata replay both SELECTs. Norway now publishes complete contacts
directly from BRREG entity records and only replays the domain SELECT.
"""

import re
from pathlib import Path

import dagster as dg

from dagster_v3.contact_extraction import (
    COMPANY_CONTACTS_COLUMNS,
    COMPANY_DOMAINS_COLUMNS,
)
from dagster_v3.defs.finland_ytj import contacts as fi_contacts
from dagster_v3.defs.norway_brreg.assets import contacts as no_contacts
from dagster_v3.defs.norway_brreg import resolved_tables as no_tables
from dagster_v3.defs.wikidata import contacts as wikidata_contacts
from dagster_v3.definitions import defs as load_project_defs

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"


def _read_migration(pattern: str) -> str:
    matches = sorted(MIGRATIONS_DIR.glob(pattern))
    assert matches, pattern
    return matches[-1].read_text()


def _extract_insert_select(sql: str, table: str) -> str:
    match = re.search(
        rf"INSERT INTO corpscout\.{re.escape(table)} \([^)]*\)\s*(SELECT.*?)\s*;",
        sql,
        re.DOTALL,
    )
    assert match, f"no INSERT SELECT found for corpscout.{table}"
    return match.group(1)


def _normalize(sql: str) -> str:
    return " ".join(sql.split())


def test_norway_contacts_are_not_rebuilt_from_website_rows():
    assert not hasattr(no_contacts, "build_contacts_select")
    assert (
        no_tables.RESOLVED_TABLE_COLUMNS[no_tables.NO_COMPANY_CONTACTS_TABLE]
        == COMPANY_CONTACTS_COLUMNS
    )


def test_norway_domains_select_matches_migration_lock_step():
    sql = _read_migration("*_corpscout_no_canonical_contacts.up.sql")
    expected = _normalize(_extract_insert_select(sql, "no_company_domains"))
    assert _normalize(no_contacts.build_domains_select()) == expected


def test_finland_contacts_select_matches_migration_lock_step():
    sql = _read_migration("*_corpscout_fi_canonical_contacts.up.sql")
    expected = _normalize(_extract_insert_select(sql, "fi_company_contacts"))
    assert _normalize(fi_contacts.build_contacts_select()) == expected


def test_finland_domains_select_matches_migration_lock_step():
    sql = _read_migration("*_corpscout_fi_canonical_contacts.up.sql")
    expected = _normalize(_extract_insert_select(sql, "fi_company_domains"))
    assert _normalize(fi_contacts.build_domains_select()) == expected


def test_wikidata_contacts_select_matches_migration_lock_step():
    sql = _read_migration("*_corpscout_wikidata_canonical_contacts.up.sql")
    expected = _normalize(_extract_insert_select(sql, "wikidata_company_contacts"))
    assert _normalize(wikidata_contacts.build_contacts_select()) == expected


def test_wikidata_domains_select_matches_migration_lock_step():
    sql = _read_migration("*_corpscout_wikidata_canonical_contacts.up.sql")
    expected = _normalize(_extract_insert_select(sql, "wikidata_company_domains"))
    assert _normalize(wikidata_contacts.build_domains_select()) == expected


def test_contact_derivations_target_canonical_column_tuples():
    # Every source writes the shared canonical column tuples verbatim — no
    # per-source shape (spec: "write the canonical pair, not a per-source shape").
    assert no_contacts.COMPANY_DOMAINS_COLUMNS is COMPANY_DOMAINS_COLUMNS
    assert fi_contacts.COMPANY_CONTACTS_COLUMNS is COMPANY_CONTACTS_COLUMNS
    assert fi_contacts.COMPANY_DOMAINS_COLUMNS is COMPANY_DOMAINS_COLUMNS
    assert wikidata_contacts.COMPANY_CONTACTS_COLUMNS is COMPANY_CONTACTS_COLUMNS
    assert wikidata_contacts.COMPANY_DOMAINS_COLUMNS is COMPANY_DOMAINS_COLUMNS


def test_norway_canonical_contacts_asset_registered_with_both_deps():
    repo = load_project_defs().get_repository_def()
    keys = {k.path[-1] for k in repo.asset_graph.get_all_asset_keys()}
    assert "norway_brreg_clickhouse_canonical_contacts" in keys

    node = repo.asset_graph.get(
        dg.AssetKey("norway_brreg_clickhouse_canonical_contacts")
    )
    assert node.group_name == "norway_brreg"
    assert {k.path[-1] for k in node.parent_keys} == {
        "norway_brreg_entities_snapshot_clickhouse",
        "norway_brreg_entity_updates_clickhouse",
    }


def test_norway_canonical_contacts_asset_in_both_jobs():
    repo = load_project_defs().get_repository_def()
    for job_name in (
        "norway_brreg_entities_full_snapshot_job",
        "norway_brreg_entity_updates_job",
    ):
        keys = {
            k.path[-1] for k in repo.get_job(job_name).asset_layer.executable_asset_keys
        }
        assert "norway_brreg_clickhouse_canonical_contacts" in keys, job_name


def test_finland_canonical_contacts_asset_registered_with_dep():
    repo = load_project_defs().get_repository_def()
    keys = {k.path[-1] for k in repo.asset_graph.get_all_asset_keys()}
    assert "finland_ytj_clickhouse_canonical_contacts" in keys

    node = repo.asset_graph.get(
        dg.AssetKey("finland_ytj_clickhouse_canonical_contacts")
    )
    assert node.group_name == "finland_ytj"
    assert {k.path[-1] for k in node.parent_keys} == {"finland_ytj_resolved_clickhouse"}


def test_finland_canonical_contacts_asset_in_resolved_job():
    repo = load_project_defs().get_repository_def()
    keys = {
        k.path[-1]
        for k in repo.get_job(
            "finland_ytj_resolved_job"
        ).asset_layer.executable_asset_keys
    }
    assert "finland_ytj_clickhouse_canonical_contacts" in keys


def test_wikidata_canonical_contacts_asset_registered_with_dep():
    repo = load_project_defs().get_repository_def()
    keys = {k.path[-1] for k in repo.asset_graph.get_all_asset_keys()}
    assert "wikidata_clickhouse_canonical_contacts" in keys

    node = repo.asset_graph.get(dg.AssetKey("wikidata_clickhouse_canonical_contacts"))
    assert node.group_name == "wikidata"
    assert {k.path[-1] for k in node.parent_keys} == {"wikidata_snapshot_complete"}


def test_wikidata_canonical_contacts_asset_in_publish_job():
    repo = load_project_defs().get_repository_def()
    keys = {
        k.path[-1]
        for k in repo.get_job("wikidata_publish_job").asset_layer.executable_asset_keys
    }
    assert "wikidata_clickhouse_canonical_contacts" in keys
