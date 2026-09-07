"""Curated alias validation and publication contracts."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.technology_catalog import assets, tables
from dagster_v3.defs.technology_catalog.aliases import (
    build_alias_rows,
    load_technology_aliases,
    normalize_technology_name,
)


def alias_entry(**changes) -> dict:
    return {
        "alias": "AWS",
        "technology": "Amazon Web Services",
        "reviewed_by": "test-reviewer",
        "reviewed_at": "2026-09-07",
        "review_note": "Reviewed abbreviation for the catalog cloud platform.",
        "source_references": ["https://aws.amazon.com/what-is-aws/"],
    } | changes


def write_aliases(path: Path, entries: list[dict]) -> None:
    (path / "technology_aliases.json").write_text(
        json.dumps({"schema_version": "1.0", "aliases": entries}), encoding="utf-8"
    )


def test_accepted_alias_has_normalized_key_exact_target_and_source_version(tmp_path):
    write_aliases(tmp_path, [alias_entry(alias="  Aws\u00a0")])
    rows, version = load_technology_aliases(tmp_path, {"Amazon Web Services"})
    assert rows[0].alias_key == "aws"
    assert rows[0].technology == "Amazon Web Services"
    assert rows[0].alias == "  Aws\u00a0"
    assert len(version) == 64
    before = version
    write_aliases(tmp_path, [alias_entry(review_note="Re-reviewed source evidence.")])
    assert load_technology_aliases(tmp_path, {"Amazon Web Services"})[1] != before


def test_missing_or_malformed_file_does_not_mean_empty(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_technology_aliases(tmp_path, set())
    path = tmp_path / "technology_aliases.json"
    for document in ["{", "{}", '{"schema_version":"1.0","aliases":null}', "[]"]:
        path.write_text(document, encoding="utf-8")
        with pytest.raises(ValueError):
            load_technology_aliases(tmp_path, set())
    write_aliases(tmp_path, [])
    assert load_technology_aliases(tmp_path, set())[0] == []


@pytest.mark.parametrize(
    "entry",
    [
        alias_entry(technology="Missing"),
        alias_entry(alias="GIT", technology="git"),
        alias_entry(alias=" Ｇｉｔ ", technology="git"),
        alias_entry(alias="C++", technology="C"),
        alias_entry(alias="C#", technology="C"),
        alias_entry(reviewed_by=" "),
        alias_entry(review_note=""),
        alias_entry(reviewed_at="20260907"),
        alias_entry(reviewed_at="not-a-date"),
        alias_entry(source_references=[]),
        alias_entry(source_references=["not a URL"]),
        alias_entry(source_references=["https://user:password@example.test/"]),
        alias_entry(review_status="candidate"),
    ],
)
def test_invalid_alias_never_loads(tmp_path, entry):
    write_aliases(tmp_path, [entry])
    with pytest.raises(ValueError):
        load_technology_aliases(
            tmp_path, {"Amazon Web Services", "git", "C", "C++", "C#"}
        )


@pytest.mark.parametrize("target", ["Amazon Web Services", "Azure"])
def test_duplicate_or_conflicting_normalized_alias_fails(tmp_path, target):
    write_aliases(
        tmp_path, [alias_entry(), alias_entry(alias=" aws ", technology=target)]
    )
    with pytest.raises(ValueError, match="Duplicate or conflicting"):
        load_technology_aliases(tmp_path, {"Amazon Web Services", "Azure"})


def test_normalization_keeps_punctuation():
    assert (
        normalize_technology_name(" Git ") == normalize_technology_name("GIT") == "git"
    )
    assert (
        len({normalize_technology_name(name) for name in ["C", "C++", "C#", "C/C++"]})
        == 4
    )


def test_alias_contract_matches_migration_in_order(tmp_path):
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse/migrations/000388_corpscout_technology_aliases.up.sql"
    ).read_text(encoding="utf-8")
    declared = [
        line.strip().split()[0]
        for line in migration.splitlines()
        if line.startswith("    ") and not line.strip().startswith("CONSTRAINT")
    ]
    assert tuple(declared) == tables.TECHNOLOGY_ALIASES_COLUMNS
    assert tables.TECHNOLOGY_ALIASES_TABLE in tables.TECHNOLOGY_CATALOG_TABLES
    assert "ENGINE = MergeTree" in migration
    assert "ORDER BY (alias_key, technology)" in migration
    write_aliases(tmp_path, [alias_entry()])
    aliases, version = load_technology_aliases(tmp_path, {"Amazon Web Services"})
    row = build_alias_rows(
        aliases,
        source_version=version,
        source_run_id="run-1",
        updated_at=datetime.now(UTC),
    )[0]
    assert len(row) == len(declared)
    exported = dict(zip(declared, row, strict=True))
    assert exported["alias_key"] == "aws"
    assert exported["review_status"] == "accepted"
    assert exported["source_version"] == version


def test_shipped_aliases_are_explicitly_empty_and_track_asset_version(
    monkeypatch, tmp_path
):
    assert load_technology_aliases(assets.custom_source_dir(), set())[0] == []
    write_aliases(tmp_path, [])
    monkeypatch.setattr(assets, "custom_source_dir", lambda: tmp_path)
    before = assets.custom_definitions_version()
    write_aliases(tmp_path, [alias_entry()])
    assert assets.custom_definitions_version() != before
