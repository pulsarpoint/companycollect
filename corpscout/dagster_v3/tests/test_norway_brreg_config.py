import pytest

from translator.norway_brreg.config import FieldConfig, SourceConfig, get_config
from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_get_config_returns_source_config():
    cfg = get_config()
    assert isinstance(cfg, SourceConfig)
    assert cfg.source_slug == "norway_brreg"
    assert cfg.source_lang == "no"
    assert cfg.ch_table == "corpscout.no_companies"


def test_config_has_three_fields_two_dynamic_one_static():
    cfg = get_config()
    assert len(cfg.fields) == 3

    assert cfg.fields[0] == FieldConfig(original_col="articles_purpose_original")
    assert cfg.fields[1] == FieldConfig(original_col="activity_text_original")

    lf = cfg.fields[2]
    assert lf.original_col == "legal_form_description_original"
    assert lf.static_key_col == "legal_form_code"
    assert lf.static_map is not None
    assert lf.static_map_dict() == LEGAL_FORM_DESCRIPTION_EN_BY_CODE


def test_dynamic_fields_have_no_static_map():
    cfg = get_config()
    for f in cfg.fields[:2]:
        assert f.static_map is None
        assert f.static_key_col is None


def test_static_map_covers_all_40_legal_form_codes():
    cfg = get_config()
    mapping = cfg.fields[2].static_map_dict() or {}
    for code in ("FLI", "ESEK", "UTLA", "BRL", "KBO", "SAM", "ANNA", "KF", "AS", "ENK"):
        assert mapping.get(code), f"{code} must have an English translation"
    assert len(mapping) >= 40


def test_import_legacy_unknown_source_raises_key_error():
    """_get_source_config_by_slug (import_legacy.py) raises KeyError for unknown slugs."""
    from translator.import_legacy import _get_source_config_by_slug
    with pytest.raises(KeyError):
        _get_source_config_by_slug("atlantis")
