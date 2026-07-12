import os
import subprocess
import sys
from pathlib import Path

import pytest

from warc_index_builder.catalog import (
    CATALOG_SCHEMA_VERSION,
    catalog_id,
    prepare_build_directory,
    require_path_within,
    warc_inventory_sha256,
)
from warc_index_builder.selection import (
    SELECTION_POLICY_VERSION,
    selection_policy_sha256,
)


_IDENTITY_HASHES = {
    "selection_policy_sha256": "00" * 32,
    "source_schema_sha256": "11" * 32,
    "warc_manifest_sha256": "22" * 32,
    "index_manifest_sha256": "33" * 32,
    "warc_inventory_sha256": "44" * 32,
}


def _catalog_identity_values() -> dict[str, object]:
    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "crawl_id": "CC-MAIN-2026-25",
        "pages_per_domain": 25,
        "selection_policy_version": SELECTION_POLICY_VERSION,
        **_IDENTITY_HASHES,
    }


def test_require_path_within_rejects_outside_target(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()

    with pytest.raises(ValueError, match="escapes base"):
        require_path_within(base, tmp_path / "outside")


def test_prepare_build_directory_rejects_file(tmp_path: Path) -> None:
    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    catalog_directory.mkdir(parents=True)
    build_path = catalog_directory / ".build"
    build_path.write_text("not a directory")

    with pytest.raises(ValueError, match="not a directory"):
        prepare_build_directory(tmp_path, catalog_directory, rebuild=True)

    assert build_path.read_text() == "not a directory"


def test_warc_inventory_identity_matches_golden_hash() -> None:
    inventory = (
        (
            0,
            "crawl-data/CC-MAIN-2026-25/segments/a/warc/a:alpha.warc.gz",
            2**32 - 1,
        ),
        (
            1,
            "crawl-data/CC-MAIN-2026-25/segments/b/warc/beta-β.warc.gz",
            2**32 + 1,
        ),
    )

    assert warc_inventory_sha256(inventory) == (
        "472da7d2cf3483de1aec457858e72b88769190ec51c4c6f65c277c9c580279da"
    )


@pytest.mark.parametrize(
    "inventory",
    [
        (),
        ((1, "b.warc.gz", 1),),
        ((0, "a.warc.gz", 1), (0, "b.warc.gz", 2)),
        ((1, "b.warc.gz", 2), (0, "a.warc.gz", 1)),
        ((0, " ", 1),),
        ((0, "same.warc.gz", 1), (1, "same.warc.gz", 2)),
        ((0, "zero.warc.gz", 0),),
        ((0, "negative.warc.gz", -1),),
        ((0, "overflow.warc.gz", 2**64),),
    ],
)
def test_warc_inventory_identity_rejects_invalid_rows(
    inventory: tuple[tuple[int, str, int], ...],
) -> None:
    with pytest.raises(ValueError):
        warc_inventory_sha256(inventory)


def test_warc_inventory_identity_is_length_delimited() -> None:
    first = ((0, "a:1.warc.gz", 23),)
    second = ((0, "a.warc.gz", 123),)

    assert warc_inventory_sha256(first) != warc_inventory_sha256(second)


def test_catalog_identity_matches_golden_hash() -> None:
    assert catalog_id(**_catalog_identity_values()) == (
        "41d6768f649157ac34b244e8903662a59674782d9fa941e6e69396db458e04dc"
    )


def test_every_logical_input_changes_catalog_identity() -> None:
    original = _catalog_identity_values()
    changes: dict[str, object] = {
        "schema_version": 2,
        "crawl_id": "CC-MAIN-2016-22",
        "pages_per_domain": 1,
        "selection_policy_version": "page-selection-v2",
        "selection_policy_sha256": "55" * 32,
        "source_schema_sha256": "66" * 32,
        "warc_manifest_sha256": "77" * 32,
        "index_manifest_sha256": "88" * 32,
        "warc_inventory_sha256": "99" * 32,
    }

    original_id = catalog_id(**original)
    for name, value in changes.items():
        changed = {**original, name: value}
        assert catalog_id(**changed) != original_id


@pytest.mark.parametrize("value", ["A" * 64, "0" * 63, "0" * 65, "z" * 64])
def test_catalog_identity_rejects_noncanonical_hashes(value: str) -> None:
    for hash_name in _IDENTITY_HASHES:
        identity = _catalog_identity_values()
        identity[hash_name] = value
        with pytest.raises(ValueError, match=hash_name):
            catalog_id(**identity)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 0),
        ("schema_version", 2**16),
        ("crawl_id", ""),
        ("pages_per_domain", 0),
        ("pages_per_domain", 2**16),
        ("selection_policy_version", ""),
    ],
)
def test_catalog_identity_rejects_invalid_fields(field: str, value: object) -> None:
    identity = _catalog_identity_values()
    identity[field] = value

    with pytest.raises(ValueError):
        catalog_id(**identity)


def test_pages_one_and_pages_twenty_five_share_policy_but_not_catalog_identity() -> None:
    policy_hash = selection_policy_sha256()
    pages_one = _catalog_identity_values()
    pages_twenty_five = _catalog_identity_values()
    pages_one["pages_per_domain"] = 1
    pages_one["selection_policy_sha256"] = policy_hash
    pages_twenty_five["selection_policy_sha256"] = policy_hash

    assert catalog_id(**pages_one) != catalog_id(**pages_twenty_five)


def test_all_catalog_identities_are_stable_across_processes_and_paths(
    tmp_path: Path,
) -> None:
    script = """
from warc_index_builder.catalog import catalog_id, warc_inventory_sha256
from warc_index_builder.manifests import SourceSchema, source_schemas_sha256
from warc_index_builder.selection import SELECTION_POLICY_VERSION, selection_policy_sha256

required = (
    ('url', 'VARCHAR'),
    ('url_host_name', 'VARCHAR'),
    ('url_host_registered_domain', 'VARCHAR'),
    ('url_path', 'VARCHAR'),
    ('content_mime_type', 'VARCHAR'),
    ('warc_filename', 'VARCHAR'),
    ('fetch_status', 'SMALLINT'),
    ('warc_record_offset', 'INTEGER'),
    ('warc_record_length', 'UBIGINT'),
)
current = tuple(
    (
        name,
        {
            'fetch_status': 'USMALLINT',
            'warc_record_offset': 'UINTEGER',
            'warc_record_length': 'BIGINT',
        }.get(name, column_type),
    )
    for name, column_type in required
)
schemas = (
    SourceSchema(0, required),
    SourceSchema(
        1,
        current + (
            ('content_mime_detected', 'VARCHAR'),
            ('content_languages', 'VARCHAR'),
        ),
    ),
)
inventory = (
    (0, 'crawl-data/CC-MAIN-2026-25/segments/a/warc/a:alpha.warc.gz', 2**32 - 1),
    (1, 'crawl-data/CC-MAIN-2026-25/segments/b/warc/beta-β.warc.gz', 2**32 + 1),
)
values = (
    selection_policy_sha256(),
    source_schemas_sha256(schemas),
    warc_inventory_sha256(inventory),
    catalog_id(
        schema_version=1,
        crawl_id='CC-MAIN-2026-25',
        pages_per_domain=25,
        selection_policy_version=SELECTION_POLICY_VERSION,
        selection_policy_sha256='00' * 32,
        source_schema_sha256='11' * 32,
        warc_manifest_sha256='22' * 32,
        index_manifest_sha256='33' * 32,
        warc_inventory_sha256='44' * 32,
    ),
)
print('\\n'.join(values))
"""
    results: list[str] = []
    for name, hash_seed in (("first", "1"), ("second", "8675309")):
        working_directory = tmp_path / name
        temporary_directory = working_directory / "temp"
        temporary_directory.mkdir(parents=True)
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = hash_seed
        environment["TMPDIR"] = str(temporary_directory)
        results.append(
            subprocess.check_output(
                [sys.executable, "-c", script],
                cwd=working_directory,
                env=environment,
                text=True,
            ).strip()
        )

    assert results[0] == results[1]
