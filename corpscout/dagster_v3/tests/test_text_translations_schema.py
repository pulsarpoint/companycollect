from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "clickhouse" / "migrations"

TEXT_TRANSLATIONS_UP = MIGRATIONS_DIR / "000056_corpscout_text_translations.up.sql"
TEXT_TRANSLATIONS_DOWN = MIGRATIONS_DIR / "000056_corpscout_text_translations.down.sql"

REQUIRED_COLUMNS = (
    "source_slug",
    "field",
    "source_text_hash",
    "source_lang",
    "target_lang",
    "translated_text",
    "provider",
    "model",
    "version",
)


def test_text_translations_up_defines_required_columns():
    sql = TEXT_TRANSLATIONS_UP.read_text(encoding="utf-8")
    for column in REQUIRED_COLUMNS:
        assert column in sql, f"missing column {column!r} in text_translations migration"


def test_text_translations_up_uses_replacing_merge_tree_keyed_on_hash():
    sql = TEXT_TRANSLATIONS_UP.read_text(encoding="utf-8")
    assert "corpscout.text_translations" in sql
    assert "ReplacingMergeTree(version)" in sql
    assert "ORDER BY (source_slug, field, source_text_hash)" in sql
    assert "source_text_hash  UInt64" in sql or "source_text_hash UInt64" in sql


def test_text_translations_down_drops_table():
    sql = TEXT_TRANSLATIONS_DOWN.read_text(encoding="utf-8")
    assert "DROP TABLE IF EXISTS corpscout.text_translations" in sql
