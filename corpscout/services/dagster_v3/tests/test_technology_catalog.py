"""Technology catalog: merge/slug/resolution units, icon sync, CH contract.

No live network anywhere — the overlay fetcher and S3 client are injected
fakes, and the extension layer is a fixture mini-bundle on disk.
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.technology_catalog import detection, tables
from dagster_v3.defs.technology_catalog.assets import (
    build_fingerprint_rows,
    build_rows,
    custom_source_dir,
)
from dagster_v3.defs.technology_catalog.fingerprints import (
    extract_dns_fingerprints,
    parse_pattern,
)
from dagster_v3.defs.technology_catalog.catalog import (
    CatalogLayer,
    load_custom_layer,
    load_extension_layer,
    merge_layers,
    slugify,
)
from dagster_v3.defs.technology_catalog.icons import icon_ref, sync_icons
from dagster_v3.defs.technology_catalog.source import overlay_raw_url

MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000350_corpscout_technology_catalog.up.sql"
).read_text()

FINGERPRINTS_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000357_corpscout_technology_fingerprints.up.sql"
).read_text()

OVERLAY_SHA = "b0e1186877307b246769bdeab61f270b597f6886"


def extension_layer() -> CatalogLayer:
    return CatalogLayer(
        technologies={
            "Shared Tech": {
                "cats": [1],
                "description": "extension description",
                "website": "https://extension.example",
                "icon": "Shared Tech.svg",
                "oss": True,
            },
            "Extension Only": {
                "cats": [1, 2],
                "description": "stays from the frozen layer",
                "icon": "Extension Only.png",
                "saas": True,
                "pricing": ["freemium"],
            },
            "No Icon Tech": {"cats": [99]},
        },
        categories={
            1: {"name": "CMS (extension)", "groups": [3]},
            2: {"name": "Message boards", "groups": [3, 4]},
        },
        groups={3: "Content", 4: "Communication"},
        source=tables.EXTENSION_SOURCE,
        source_version=tables.EXTENSION_VERSION,
    )


def overlay_layer() -> CatalogLayer:
    return CatalogLayer(
        technologies={
            "Shared Tech": {
                "cats": [1],
                "description": "overlay description",
                "website": "https://overlay.example",
                "icon": "Shared Tech.svg",
                "saas": True,
            },
            "Overlay Only": {
                "cats": [1],
                "description": "new in the public catalog",
                "icon": "Overlay Only.svg",
            },
        },
        categories={1: {"name": "CMS (overlay)", "groups": [7]}},
        groups={7: "Overlay Group"},
        source=tables.OVERLAY_SOURCE,
        source_version=OVERLAY_SHA,
    )


def merged_by_name():
    merged = merge_layers(extension_layer(), overlay_layer())
    return {technology.technology: technology for technology in merged}


def test_slugify_rules():
    assert slugify("Google Analytics") == "google-analytics"
    assert slugify("1C-Bitrix") == "1c-bitrix"
    assert slugify("Node.js") == "node-js"
    assert slugify("  --Weird__Name!!") == "weird-name"
    assert slugify("ALL CAPS") == "all-caps"
    # Stable: same input, same output.
    assert slugify("Node.js") == slugify("Node.js")


def test_overlay_wins_for_shared_name():
    shared = merged_by_name()["Shared Tech"]
    assert shared.description == "overlay description"
    assert shared.website == "https://overlay.example"
    assert shared.saas is True
    assert shared.oss is False  # the extension's oss flag does not bleed through
    assert shared.source == tables.OVERLAY_SOURCE
    assert shared.source_version == OVERLAY_SHA


def test_extension_only_name_survives():
    extension_only = merged_by_name()["Extension Only"]
    assert extension_only.source == tables.EXTENSION_SOURCE
    assert extension_only.source_version == tables.EXTENSION_VERSION
    assert extension_only.pricing == ("freemium",)
    assert extension_only.saas is True


def test_overlay_only_name_included():
    assert merged_by_name()["Overlay Only"].source == tables.OVERLAY_SOURCE


def test_categories_resolved_via_owning_layer():
    merged = merged_by_name()
    # Category id 1 exists in both layers with different names; each row must
    # resolve through the layer it came from.
    assert merged["Shared Tech"].categories == ("CMS (overlay)",)
    assert merged["Shared Tech"].groups == ("Overlay Group",)
    assert merged["Extension Only"].categories == ("CMS (extension)", "Message boards")
    assert merged["Extension Only"].groups == ("Content", "Communication")


def test_unknown_category_id_keeps_id_without_name():
    no_icon = merged_by_name()["No Icon Tech"]
    assert no_icon.category_ids == (99,)
    assert no_icon.categories == ()
    assert no_icon.groups == ()


def test_merge_is_sorted_by_name():
    names = [t.technology for t in merge_layers(extension_layer(), overlay_layer())]
    assert names == sorted(names)


def test_icon_ref_key_and_content_type():
    svg = icon_ref("shared-tech", "Shared Tech.svg")
    assert svg.object_key == "icons/shared-tech.svg"
    assert svg.content_type == "image/svg+xml"
    png = icon_ref("extension-only", "Extension Only.PNG")
    assert png.object_key == "icons/extension-only.png"
    assert png.content_type == "image/png"


def test_overlay_raw_url_is_pinned_to_commit():
    assert (
        overlay_raw_url(OVERLAY_SHA, "src/categories.json")
        == "https://raw.githubusercontent.com/enthec/webappanalyzer/"
        f"{OVERLAY_SHA}/src/categories.json"
    )


class FakeIconBucket:
    def __init__(self):
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.puts = 0
        self.heads = 0

    def head_object(self, *, Bucket: str, Key: str):
        self.heads += 1
        if Key not in self.objects:
            error = Exception("missing")
            error.response = {"Error": {"Code": "404"}}  # type: ignore[attr-defined]
            raise error
        return {"ContentLength": len(self.objects[Key][0])}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, ContentType: str):
        self.puts += 1
        self.objects[Key] = (Body, ContentType)


@pytest.fixture
def bundle_icons(tmp_path: Path) -> Path:
    icons_dir = tmp_path / "images" / "icons"
    icons_dir.mkdir(parents=True)
    (icons_dir / "Shared Tech.svg").write_bytes(b"<svg>shared</svg>")
    (icons_dir / "Extension Only.png").write_bytes(b"png-bytes")
    return icons_dir


def run_sync(bundle_icons: Path, bucket: FakeIconBucket, fetched: list[str]):
    def fake_fetch(filename: str) -> bytes | None:
        fetched.append(filename)
        if filename == "Overlay Only.svg":
            return b"<svg>overlay</svg>"
        return None

    return sync_icons(
        merge_layers(extension_layer(), overlay_layer()),
        bundle_icons_dir=bundle_icons,
        s3_client=bucket,
        bucket=tables.ICON_BUCKET,
        fetch_overlay_icon=fake_fetch,
    )


def test_sync_icons_uploads_and_sources_correct_layers(bundle_icons: Path):
    bucket = FakeIconBucket()
    fetched: list[str] = []
    result = run_sync(bundle_icons, bucket, fetched)

    # "Shared Tech" wins from the overlay but its icon file exists locally
    # under the identical name, so it must NOT be fetched from GitHub.
    assert fetched == ["Overlay Only.svg"]
    assert result.overlay_fetches == 1
    assert result.uploaded == 3
    assert result.skipped == 0
    assert result.missing == 1  # No Icon Tech carries no icon filename

    assert bucket.objects["icons/shared-tech.svg"] == (
        b"<svg>shared</svg>",
        "image/svg+xml",
    )
    assert bucket.objects["icons/extension-only.png"] == (b"png-bytes", "image/png")
    assert bucket.objects["icons/overlay-only.svg"] == (
        b"<svg>overlay</svg>",
        "image/svg+xml",
    )
    assert result.refs["Extension Only"].object_key == "icons/extension-only.png"
    assert "No Icon Tech" not in result.refs


def test_sync_icons_second_run_skips_existing_same_size(bundle_icons: Path):
    bucket = FakeIconBucket()
    run_sync(bundle_icons, bucket, [])
    first_puts = bucket.puts
    result = run_sync(bundle_icons, bucket, [])
    assert bucket.puts == first_puts  # nothing re-uploaded
    assert result.uploaded == 0
    assert result.skipped == 3
    # The rows still carry the keys even when every upload was skipped.
    assert result.refs["Shared Tech"].object_key == "icons/shared-tech.svg"


def test_sync_icons_reuploads_when_size_differs(bundle_icons: Path):
    bucket = FakeIconBucket()
    run_sync(bundle_icons, bucket, [])
    bucket.objects["icons/shared-tech.svg"] = (b"stale", "image/svg+xml")
    result = run_sync(bundle_icons, bucket, [])
    assert result.uploaded == 1
    assert result.skipped == 2
    assert bucket.objects["icons/shared-tech.svg"][0] == b"<svg>shared</svg>"


def test_sync_icons_missing_everywhere_yields_no_ref(bundle_icons: Path):
    (bundle_icons / "Overlay Only.svg").unlink(missing_ok=True)

    bucket = FakeIconBucket()

    def fetch_nothing(filename: str) -> bytes | None:
        return None

    result = sync_icons(
        merge_layers(extension_layer(), overlay_layer()),
        bundle_icons_dir=bundle_icons,
        s3_client=bucket,
        bucket=tables.ICON_BUCKET,
        fetch_overlay_icon=fetch_nothing,
    )
    assert "Overlay Only" not in result.refs
    assert result.missing == 2  # Overlay Only (404) + No Icon Tech (no filename)


def test_build_rows_match_column_contract(bundle_icons: Path):
    merged = merge_layers(extension_layer(), overlay_layer())
    bucket = FakeIconBucket()
    result = run_sync(bundle_icons, bucket, [])
    rows = build_rows(
        merged,
        result,
        source_run_id="run-1",
        updated_at=datetime.now(UTC).replace(tzinfo=None),
    )
    assert len(rows) == len(merged)
    columns = tables.TECHNOLOGY_CATALOG_COLUMNS
    for row in rows:
        assert len(row) == len(columns)
    by_name = {row[0]: dict(zip(columns, row, strict=True)) for row in rows}
    shared = by_name["Shared Tech"]
    assert shared["slug"] == "shared-tech"
    assert shared["icon_object_key"] == "icons/shared-tech.svg"
    assert shared["icon_content_type"] == "image/svg+xml"
    assert shared["saas"] == 1
    assert shared["oss"] == 0
    assert shared["source"] == tables.OVERLAY_SOURCE
    assert shared["source_version"] == OVERLAY_SHA
    assert shared["source_run_id"] == "run-1"
    no_icon = by_name["No Icon Tech"]
    assert no_icon["icon_object_key"] == ""
    assert no_icon["icon_content_type"] == ""


def test_load_extension_layer_reads_letter_files(tmp_path: Path):
    technologies = tmp_path / "technologies"
    technologies.mkdir()
    for letter in ["_", *"abcdefghijklmnopqrstuvwxyz"]:
        (technologies / f"{letter}.json").write_text("{}")
    (technologies / "s.json").write_text(
        json.dumps({"Some Tech": {"cats": [1], "icon": "Some Tech.svg"}})
    )
    (tmp_path / "categories.json").write_text(
        json.dumps({"1": {"name": "CMS", "groups": [3], "priority": 1}})
    )
    (tmp_path / "groups.json").write_text(json.dumps({"3": {"name": "Content"}}))

    layer = load_extension_layer(tmp_path)
    assert layer.source == tables.EXTENSION_SOURCE
    assert layer.source_version == tables.EXTENSION_VERSION
    assert layer.technologies["Some Tech"]["icon"] == "Some Tech.svg"
    assert layer.categories[1]["name"] == "CMS"
    assert layer.groups[3] == "Content"


def write_custom_files(
    tmp_path: Path,
    technologies: dict,
    categories: dict | None = None,
) -> Path:
    custom_dir = tmp_path / "custom"
    custom_dir.mkdir(parents=True)
    (custom_dir / "technologies.json").write_text(json.dumps(technologies))
    (custom_dir / "categories.json").write_text(json.dumps(categories or {}))
    return custom_dir


def custom_layer_from(tmp_path: Path, technologies: dict, categories: dict | None = None):
    return load_custom_layer(
        write_custom_files(tmp_path, technologies, categories),
        base_categories=overlay_layer().categories,
        base_groups=overlay_layer().groups,
    )


def test_custom_layer_wins_over_both_public_layers(tmp_path: Path):
    custom = custom_layer_from(
        tmp_path,
        {"Shared Tech": {"cats": [1], "description": "curated description"}},
    )
    merged = {
        t.technology: t
        for t in merge_layers(extension_layer(), overlay_layer(), custom)
    }
    shared = merged["Shared Tech"]
    assert shared.description == "curated description"
    assert shared.source == tables.CUSTOM_SOURCE
    assert shared.source_version == custom.source_version
    # Untouched names keep their original winning layers.
    assert merged["Overlay Only"].source == tables.OVERLAY_SOURCE
    assert merged["Extension Only"].source == tables.EXTENSION_SOURCE


def test_custom_layer_resolves_standard_and_custom_categories(tmp_path: Path):
    custom = custom_layer_from(
        tmp_path,
        {"Curated Tech": {"cats": [1, 900], "description": "x"}},
        {"900": {"name": "Email security", "priority": 1}},
    )
    merged = {
        t.technology: t
        for t in merge_layers(extension_layer(), overlay_layer(), custom)
    }
    assert merged["Curated Tech"].categories == ("CMS (overlay)", "Email security")


def test_custom_layer_refuses_low_category_ids(tmp_path: Path):
    with pytest.raises(ValueError, match="below the 900 floor"):
        custom_layer_from(
            tmp_path,
            {},
            {"88": {"name": "Hosting (collides)", "priority": 1}},
        )


def test_custom_layer_refuses_unknown_category_reference(tmp_path: Path):
    with pytest.raises(ValueError, match="unknown category id 999"):
        custom_layer_from(
            tmp_path,
            {"Typo Tech": {"cats": [999], "description": "x"}},
        )


def test_custom_layer_version_is_content_hash(tmp_path: Path):
    technologies = {"Curated Tech": {"cats": [1], "description": "x"}}
    first = custom_layer_from(tmp_path, technologies)
    same = load_custom_layer(
        tmp_path / "custom",
        base_categories=overlay_layer().categories,
        base_groups=overlay_layer().groups,
    )
    assert first.source_version == same.source_version
    assert len(first.source_version) == 40
    changed = custom_layer_from(
        tmp_path / "changed",
        {"Curated Tech": {"cats": [1], "description": "y"}},
    )
    assert changed.source_version != first.source_version


def test_shipped_custom_files_load_against_extension_vocabulary():
    # The real files must reference only category ids the public vocabulary
    # (here: the vendored bundle, a close proxy for the overlay) or our own
    # categories.json can resolve, and every entry needs a description.
    bundle_dir = Path(__file__).resolve().parents[4] / "extensions" / "6.12.5_0"
    extension = load_extension_layer(bundle_dir)
    custom = load_custom_layer(
        custom_source_dir(),
        base_categories=extension.categories,
        base_groups=extension.groups,
    )
    assert len(custom.technologies) >= 10
    for name, entry in custom.technologies.items():
        assert entry.get("description"), f"{name} has no description"
        assert entry.get("cats"), f"{name} has no categories"


def test_load_extension_layer_missing_dir_names_the_override():
    with pytest.raises(FileNotFoundError, match="TECHNOLOGY_CATALOG_EXTENSION_DIR"):
        load_extension_layer(Path("/nonexistent/technology-catalog-bundle"))


# --- Fingerprint extraction (migration 000357) -------------------------------


def test_parse_pattern_plain_and_tails():
    assert parse_pattern("aspmx\\.l\\.google\\.com") == (
        "aspmx\\.l\\.google\\.com",
        100,
        "",
    )
    assert parse_pattern("regex\\;confidence:50") == ("regex", 50, "")
    assert parse_pattern("regex\\;version:\\1\\;confidence:20") == (
        "regex",
        20,
        "\\1",
    )


def test_extract_dns_fingerprints_from_winning_layer(tmp_path: Path):
    custom = custom_layer_from(
        tmp_path,
        {
            "Shared Tech": {
                "cats": [1],
                "description": "curated",
                "dns": {"MX": ["\\.curated\\.example$"], "TXT": "token=\\;confidence:75"},
            }
        },
    )
    fingerprints = extract_dns_fingerprints(extension_layer(), overlay_layer(), custom)
    by_signal = {(f.technology, f.signal_type): f for f in fingerprints}
    mx = by_signal[("Shared Tech", "dns_mx")]
    assert mx.pattern == "\\.curated\\.example$"
    assert mx.confidence == 100
    assert mx.source == tables.CUSTOM_SOURCE
    txt = by_signal[("Shared Tech", "dns_txt")]
    assert txt.pattern == "token="
    assert txt.confidence == 75
    # Layers without dns blocks contribute nothing.
    assert {f.technology for f in fingerprints} == {"Shared Tech"}


def test_shipped_custom_dns_fingerprints_extract():
    bundle_dir = Path(__file__).resolve().parents[4] / "extensions" / "6.12.5_0"
    extension = load_extension_layer(bundle_dir)
    custom = load_custom_layer(
        custom_source_dir(),
        base_categories=extension.categories,
        base_groups=extension.groups,
    )
    fingerprints = extract_dns_fingerprints(extension, custom)
    custom_fingerprints = [
        f for f in fingerprints if f.source == tables.CUSTOM_SOURCE
    ]
    assert len(custom_fingerprints) >= 20
    assert all(f.signal_type.startswith("dns_") for f in custom_fingerprints)
    # The extension bundle's own dns blocks come along (102 patterns counted).
    assert len(fingerprints) >= tables.MIN_TECHNOLOGY_FINGERPRINT_ROWS


def test_build_fingerprint_rows_match_column_contract(tmp_path: Path):
    custom = custom_layer_from(
        tmp_path,
        {
            "Shared Tech": {
                "cats": [1],
                "description": "curated",
                "dns": {"MX": ["\\.curated\\.example$"]},
            }
        },
    )
    fingerprints = extract_dns_fingerprints(custom)
    rows = build_fingerprint_rows(
        fingerprints,
        source_run_id="run-1",
        updated_at=datetime.now(UTC).replace(tzinfo=None),
    )
    columns = tables.TECHNOLOGY_FINGERPRINTS_COLUMNS
    assert rows and all(len(row) == len(columns) for row in rows)
    row = dict(zip(columns, rows[0], strict=True))
    assert row["technology"] == "Shared Tech"
    assert row["signal_type"] == "dns_mx"
    assert row["source_run_id"] == "run-1"


def test_fingerprints_migration_creates_the_table():
    assert (
        f"CREATE TABLE IF NOT EXISTS corpscout.{tables.TECHNOLOGY_FINGERPRINTS_TABLE}"
        in FINGERPRINTS_MIGRATION
    )


def test_fingerprint_columns_match_migration():
    for column in tables.TECHNOLOGY_FINGERPRINTS_COLUMNS:
        assert f"    {column} " in FINGERPRINTS_MIGRATION, (
            f"missing {column} in migration"
        )
    declared = [
        line
        for line in FINGERPRINTS_MIGRATION.splitlines()
        if line.startswith("    ") and not line.lstrip().startswith("--")
    ]
    assert len(declared) == len(tables.TECHNOLOGY_FINGERPRINTS_COLUMNS)


# --- DNS detection (migration 000358) ----------------------------------------

DETECTION_MIGRATION = (
    Path(__file__).resolve().parents[3]
    / "clickhouse"
    / "migrations"
    / "000359_corpscout_domain_signal_technologies_partitioned.up.sql"
).read_text()


def test_group_fingerprints_parallel_arrays_and_skips():
    rows = [
        ("Google Workspace", "dns_mx", "aspmx\\.l\\.google\\.com", 100, "webappanalyzer"),
        ("Loopia", "dns_mx", "\\.loopia\\.se$", 100, "custom"),
        ("Lookaround Tech", "dns_txt", "(?!nope)token", 100, "webappanalyzer"),
        ("Backref Tech", "dns_txt", "(a)\\1", 100, "webappanalyzer"),
        ("Token Tech", "dns_txt", "token=", 75, "custom"),
        ("Future Signal", "spf_include", "ignored", 100, "custom"),
    ]
    signals, skipped = detection.group_fingerprints(rows)
    assert [s.signal_type for s in signals] == ["dns_mx", "dns_txt"]
    mx = signals[0]
    assert mx.technologies == ["Google Workspace", "Loopia"]
    assert mx.patterns == ["aspmx\\.l\\.google\\.com", "\\.loopia\\.se$"]
    txt = signals[1]
    assert txt.technologies == ["Token Tech"]
    assert txt.confidences == [75]
    assert skipped == [
        ("Lookaround Tech", "(?!nope)token"),
        ("Backref Tech", "(a)\\1"),
    ]


def test_vectorscan_safe_accepts_shipped_patterns():
    bundle_dir = Path(__file__).resolve().parents[4] / "extensions" / "6.12.5_0"
    extension = load_extension_layer(bundle_dir)
    custom = load_custom_layer(
        custom_source_dir(),
        base_categories=extension.categories,
        base_groups=extension.groups,
    )
    custom_patterns = [
        f.pattern
        for f in extract_dns_fingerprints(custom)
        if f.source == tables.CUSTOM_SOURCE
    ]
    assert custom_patterns
    assert all(detection.vectorscan_safe(p) for p in custom_patterns)


def test_candidates_insert_is_bucket_pruned_and_covers_every_signal():
    sql = detection.candidates_insert_sql("`db`.`cand`", 19)
    assert sql.count(f"`{'commoncrawl_domain_dns_records'}`") == 1
    # Verbatim record-store partition-key expression (migration 000161) so
    # pruning engages, then the detection bucket's own ownership clause.
    assert "cityHash64(root_domain) % 16 = 3" in sql  # 19 % 16
    assert "cityHash64(root_domain) % 128 = 19" in sql
    for record_type in ("MX", "TXT", "NS", "SOA", "CNAME"):
        assert f"'{record_type}'" in sql
    assert "substringIndex(value, ' ', -1)" in sql  # MX priority prefix
    assert "trim(BOTH '\"' FROM value)" in sql  # TXT quotes
    assert "record_type = 'CNAME' AND name = concat('www.', root_domain)" in sql
    assert "GROUP BY root_domain, record_name, signal_type, candidate" in sql
    # The seen-window is inherited from the matched records, making the
    # detection table a timeline rather than a current-state snapshot.
    assert "min(first_seen) AS first_seen" in sql
    assert "max(last_seen) AS last_seen" in sql


def test_bucket_count_matches_dns_records_partition_key():
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000161_corpscout_dns_records_seen_window.up.sql"
    ).read_text()
    assert (
        f"cityHash64(root_domain) % {detection.DNS_RECORDS_HASH_BUCKETS}"
        in migration
    )


def test_detection_insert_sql_uses_one_vectorscan_pass():
    sql = detection.detection_insert_sql("`db`.`stage`", "`db`.`cand`", "dns_mx")
    assert "multiMatchAllIndices(candidate, %(match_patterns)s)" in sql
    assert "ARRAY JOIN" in sql
    assert "'dns_mx' AS signal_type" in sql
    for column in tables.DOMAIN_SIGNAL_TECHNOLOGIES_COLUMNS:
        assert column in sql


def test_signal_filters_sit_in_subqueries_below_the_alias():
    # The outer SELECT aliases a column literally named signal_type, and
    # ClickHouse resolves an outer WHERE against that alias — the filter must
    # therefore live in a subquery underneath it.
    for sql in (
        detection.detection_insert_sql("`db`.`stage`", "`db`.`cand`", "dns_mx"),
        detection.self_hosted_insert_sql("`db`.`stage`", "`db`.`cand`"),
    ):
        inner = sql.split("FROM (", 1)[1]
        assert "WHERE signal_type = 'dns_mx'" in inner
        alias_pos = sql.index("AS signal_type")
        assert sql.index("WHERE signal_type = 'dns_mx'") > alias_pos
        assert "FROM `db`.`cand`" in inner


def test_match_patterns_are_case_insensitive_but_stored_clean():
    signals, _ = detection.group_fingerprints(
        [("Loopia", "dns_mx", "\\.loopia\\.se$", 100, "custom")]
    )
    assert signals[0].patterns == ["\\.loopia\\.se$"]
    assert signals[0].match_patterns == ["(?i)\\.loopia\\.se$"]


def test_self_hosted_sql_scopes_to_own_domain():
    sql = detection.self_hosted_insert_sql("`db`.`stage`", "`db`.`cand`")
    assert f"'{detection.SELF_HOSTED_TECHNOLOGY}'" in sql
    assert "endsWith(candidate, concat('.', root_domain))" in sql
    assert "candidate = root_domain" in sql
    assert "'~', 'localhost'" in sql


def test_partition_keys_and_bucket_mapping():
    keys = detection.detection_partition_keys()
    assert len(keys) == 128
    assert keys[0] == "hash_000"
    assert keys[127] == "hash_127"
    assert detection.partition_bucket("hash_042") == 42
    with pytest.raises(ValueError):
        detection.partition_bucket("hash_128")


def test_replace_partition_sql_targets_the_bucket():
    sql = detection.replace_partition_sql("`db`.`t`", "`db`.`stage`", 42)
    assert sql == "ALTER TABLE `db`.`t` REPLACE PARTITION 42 FROM `db`.`stage`"


def test_detection_migration_creates_the_table():
    assert (
        "CREATE TABLE IF NOT EXISTS corpscout."
        f"{tables.DOMAIN_SIGNAL_TECHNOLOGIES_TABLE}" in DETECTION_MIGRATION
    )
    # The table's partition key must match the asset's bucket expression, or
    # REPLACE PARTITION would swap the wrong slice.
    assert (
        f"PARTITION BY cityHash64(root_domain) % {detection.DETECTION_PARTITION_COUNT}"
        in DETECTION_MIGRATION
    )


def test_detection_columns_match_migration():
    for column in tables.DOMAIN_SIGNAL_TECHNOLOGIES_COLUMNS:
        assert f"    {column} " in DETECTION_MIGRATION, (
            f"missing {column} in migration"
        )
    declared = [
        line
        for line in DETECTION_MIGRATION.splitlines()
        if line.startswith("    ") and not line.lstrip().startswith("--")
    ]
    assert len(declared) == len(tables.DOMAIN_SIGNAL_TECHNOLOGIES_COLUMNS)


# --- ClickHouse contract: migration 000350 owns the schema -------------------


def test_migration_creates_the_table():
    assert (
        f"CREATE TABLE IF NOT EXISTS corpscout.{tables.TECHNOLOGY_CATALOG_TABLE}"
        in MIGRATION
    )


def test_export_columns_match_migration():
    for column in tables.TECHNOLOGY_CATALOG_COLUMNS:
        assert f"    {column} " in MIGRATION, f"missing {column} in migration"


def test_export_columns_are_unique_and_ordered():
    columns = tables.TECHNOLOGY_CATALOG_COLUMNS
    assert len(columns) == len(set(columns))
    assert columns[0] == "technology"
    assert columns[1] == "slug"
    assert columns[-2:] == ("source_run_id", "updated_at")
    # Every column the migration declares is exported: the count must match
    # the number of column definition lines in the CREATE TABLE.
    declared = [
        line
        for line in MIGRATION.splitlines()
        if line.startswith("    ") and not line.lstrip().startswith("--")
    ]
    assert len(declared) == len(columns)


def test_row_floor_guards_the_extension_baseline():
    # 7,278 technologies ship in the extension bundle alone; the floor must
    # stay high enough to catch a half-broken merge.
    assert tables.MIN_TECHNOLOGY_CATALOG_ROWS == 5_000
