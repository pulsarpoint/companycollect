from dagster_v3.defs.norway_brreg import tables


def test_free_text_en_columns_are_not_exported():
    for column in ("articles_purpose_en", "activity_text_en"):
        assert column not in tables.COMPANIES_EXPORT_COLUMNS


def test_reference_en_columns_are_still_exported():
    for column in (
        "legal_form_description_en",
        "nace1_description_en",
        "nace2_description_en",
        "nace3_description_en",
    ):
        assert column in tables.COMPANIES_EXPORT_COLUMNS


def test_original_free_text_columns_are_still_exported():
    for column in (
        "articles_purpose_original",
        "activity_text_original",
    ):
        assert column in tables.COMPANIES_EXPORT_COLUMNS
