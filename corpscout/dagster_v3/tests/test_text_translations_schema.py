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


VIEW_UP = MIGRATIONS_DIR / "000057_corpscout_norway_companies_translated_view.up.sql"
VIEW_DOWN = MIGRATIONS_DIR / "000057_corpscout_norway_companies_translated_view.down.sql"

FREE_TEXT_FIELDS = (
    ("articles_purpose", "articles_purpose_original", "articles_purpose_en"),
    ("activity_text", "activity_text_original", "activity_text_en"),
    ("company_description", "company_description_original", "company_description_en"),
)


def test_view_up_excludes_free_text_en_and_rejoins_them():
    sql = VIEW_UP.read_text(encoding="utf-8")
    assert "corpscout.norway_companies_translated" in sql
    assert "corpscout.companies" in sql
    assert "corpscout.text_translations" in sql
    # The 3 free-text base _en columns are excluded then re-supplied from the cache.
    for _field, _original, en_col in FREE_TEXT_FIELDS:
        assert en_col in sql, f"view must reference {en_col}"
    assert "EXCEPT (articles_purpose_en, activity_text_en, company_description_en)" in sql
    # Each field joins on the raw-text cityHash64 and selects argMax over version.
    for field, original, _en in FREE_TEXT_FIELDS:
        assert f"field = '{field}'" in sql, f"view must filter field {field!r}"
        assert f"cityHash64(c.{original})" in sql, f"view must join on cityHash64(c.{original})"
    assert "argMax(translated_text, version)" in sql


def test_view_up_passes_through_reference_en_columns():
    sql = VIEW_UP.read_text(encoding="utf-8")
    # Reference _en columns must NOT be in the EXCEPT list (they pass through from base).
    except_clause = sql.split("EXCEPT (", 1)[1].split(")", 1)[0]
    for reference_en in (
        "legal_form_description_en",
        "nace1_description_en",
        "nace2_description_en",
        "nace3_description_en",
    ):
        assert reference_en not in except_clause, (
            f"{reference_en} is reference data and must pass through, not be excluded"
        )


def test_view_down_drops_view():
    sql = VIEW_DOWN.read_text(encoding="utf-8")
    assert "DROP VIEW IF EXISTS corpscout.norway_companies_translated" in sql


DROP_EN_UP = MIGRATIONS_DIR / "000058_corpscout_companies_drop_free_text_en.up.sql"
DROP_EN_DOWN = MIGRATIONS_DIR / "000058_corpscout_companies_drop_free_text_en.down.sql"


def test_drop_free_text_en_up_drops_three_columns_and_rebuilds_view():
    sql = DROP_EN_UP.read_text(encoding="utf-8")
    for col in ("articles_purpose_en", "activity_text_en", "company_description_en"):
        assert f"DROP COLUMN IF EXISTS {col}" in sql
    # View is recreated WITHOUT the EXCEPT clause (base no longer has those columns).
    assert "CREATE OR REPLACE VIEW corpscout.norway_companies_translated" in sql
    assert "EXCEPT (" not in sql
    assert "cityHash64(c.company_description_original)" in sql


def test_drop_free_text_en_down_readds_columns():
    sql = DROP_EN_DOWN.read_text(encoding="utf-8")
    for col in ("articles_purpose_en", "activity_text_en", "company_description_en"):
        assert f"ADD COLUMN IF NOT EXISTS {col} String" in sql


NO_COMPANIES_FREE_TEXT_UP = (
    MIGRATIONS_DIR / "000059_corpscout_no_companies_free_text_columns.up.sql"
)
NO_COMPANIES_FREE_TEXT_DOWN = (
    MIGRATIONS_DIR / "000059_corpscout_no_companies_free_text_columns.down.sql"
)

FREE_TEXT_ORIGINAL_COLUMNS = (
    "company_description_original",
    "articles_purpose_original",
    "activity_text_original",
)


NO_COMPANIES_TRANSLATED_VIEW_UP = (
    MIGRATIONS_DIR / "000060_corpscout_no_companies_translated_view.up.sql"
)
NO_COMPANIES_TRANSLATED_VIEW_DOWN = (
    MIGRATIONS_DIR / "000060_corpscout_no_companies_translated_view.down.sql"
)

NO_COMPANIES_FREE_TEXT_FIELDS = (
    ("articles_purpose", "articles_purpose_original", "articles_purpose_en"),
    ("activity_text", "activity_text_original", "activity_text_en"),
    ("company_description", "company_description_original", "company_description_en"),
)


def test_no_companies_translated_view_up_targets_no_companies_and_joins_cache():
    sql = NO_COMPANIES_TRANSLATED_VIEW_UP.read_text(encoding="utf-8")
    assert "corpscout.no_companies_translated" in sql
    assert "FROM corpscout.no_companies AS c" in sql
    assert "corpscout.text_translations" in sql
    # No EXCEPT clause — no_companies never had base _en columns.
    assert "EXCEPT (" not in sql
    # Each field joins on the raw-text cityHash64 and selects argMax over version.
    for field, original, _en in NO_COMPANIES_FREE_TEXT_FIELDS:
        assert f"field = '{field}'" in sql, f"view must filter field {field!r}"
        assert f"cityHash64(c.{original})" in sql, f"view must join on cityHash64(c.{original})"
    assert "argMax(translated_text, version)" in sql


def test_no_companies_translated_view_up_surfaces_all_three_en_columns():
    sql = NO_COMPANIES_TRANSLATED_VIEW_UP.read_text(encoding="utf-8")
    for _field, _original, en_col in NO_COMPANIES_FREE_TEXT_FIELDS:
        assert en_col in sql, f"view must produce {en_col}"


def test_no_companies_translated_view_down_drops_view():
    sql = NO_COMPANIES_TRANSLATED_VIEW_DOWN.read_text(encoding="utf-8")
    assert "DROP VIEW IF EXISTS corpscout.no_companies_translated" in sql


def test_no_companies_free_text_up_adds_three_nullable_columns():
    sql = NO_COMPANIES_FREE_TEXT_UP.read_text(encoding="utf-8")
    assert "ALTER TABLE corpscout.no_companies" in sql
    for col in FREE_TEXT_ORIGINAL_COLUMNS:
        assert f"ADD COLUMN IF NOT EXISTS {col} Nullable(String)" in sql


def test_no_companies_free_text_down_drops_three_columns():
    sql = NO_COMPANIES_FREE_TEXT_DOWN.read_text(encoding="utf-8")
    assert "ALTER TABLE corpscout.no_companies" in sql
    for col in FREE_TEXT_ORIGINAL_COLUMNS:
        assert f"DROP COLUMN IF EXISTS {col}" in sql
