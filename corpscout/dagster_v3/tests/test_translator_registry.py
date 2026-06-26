import pytest

from translator.registry import FieldConfig, get_source_config
from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_norway_brreg_config_has_four_fields_three_dynamic_one_static():
    config = get_source_config("norway_brreg")
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.no_companies"
    assert len(config.fields) == 4

    # The three free-text fields are dynamic (no static map).
    dynamic_fields = config.fields[:3]
    assert dynamic_fields[0] == FieldConfig(field="articles_purpose", original_col="articles_purpose_original")
    assert dynamic_fields[1] == FieldConfig(field="activity_text", original_col="activity_text_original")
    assert dynamic_fields[2] == FieldConfig(field="company_description", original_col="company_description_original")
    for f in dynamic_fields:
        assert f.static_map is None
        assert f.static_key_col is None

    # legal_form_description is a static field resolved from the authoritative code→English map.
    lf = config.fields[3]
    assert lf.field == "legal_form_description"
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None
    assert len(lf.static_map) == len(LEGAL_FORM_DESCRIPTION_EN_BY_CODE)
    assert lf.static_map_dict() == LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_unknown_source_raises_key_error():
    with pytest.raises(KeyError):
        get_source_config("atlantis")
