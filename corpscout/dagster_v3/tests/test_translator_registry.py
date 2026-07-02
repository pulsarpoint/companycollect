"""Translator source config registry tests."""
import pytest

from translator.norway_brreg.config import get_config
from translator.source_registry import get_source_config


def test_norway_brreg_config_has_three_fields_two_dynamic_one_static():
    config = get_config()
    assert config.source_lang == "no"
    assert config.ch_table == "corpscout.no_companies"
    assert len(config.fields) == 3

    for f in config.fields[:2]:
        assert f.static_map is None
        assert f.static_key_col is None

    lf = config.fields[2]
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None


def test_unknown_source_raises_key_error():
    from translator.import_legacy import _get_source_config_by_slug
    with pytest.raises(KeyError):
        _get_source_config_by_slug("atlantis")


def test_latvia_ur_source_config_is_registered():
    config = get_source_config("latvia_ur")
    assert config.source_slug == "latvia_ur"
    assert config.source_lang == "lv"
    assert config.ch_table == "corpscout.lv_companies"
    assert [field.original_col for field in config.fields] == ["activity_text_original"]
