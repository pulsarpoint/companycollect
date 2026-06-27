import pytest

from translator.registry import FieldConfig, get_source_config
from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_norway_brreg_config_has_three_fields_two_dynamic_one_static():
    config = get_source_config("norway_brreg")
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.no_companies"
    assert len(config.fields) == 3

    dynamic_fields = config.fields[:2]
    assert dynamic_fields[0] == FieldConfig(original_col="articles_purpose_original")
    assert dynamic_fields[1] == FieldConfig(original_col="activity_text_original")
    for f in dynamic_fields:
        assert f.static_map is None
        assert f.static_key_col is None

    lf = config.fields[2]
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None
    assert len(lf.static_map) == len(LEGAL_FORM_DESCRIPTION_EN_BY_CODE)
    assert lf.static_map_dict() == LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_unknown_source_raises_key_error():
    with pytest.raises(KeyError):
        get_source_config("atlantis")
