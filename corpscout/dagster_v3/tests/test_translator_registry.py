import pytest

from translator.registry import FieldConfig, get_source_config


def test_norway_brreg_config_has_three_free_text_fields():
    config = get_source_config("norway_brreg")
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.companies"
    assert config.fields == (
        FieldConfig(field="articles_purpose", original_col="articles_purpose_original"),
        FieldConfig(field="activity_text", original_col="activity_text_original"),
        FieldConfig(field="company_description", original_col="company_description_original"),
    )


def test_unknown_source_raises_key_error():
    with pytest.raises(KeyError):
        get_source_config("atlantis")
