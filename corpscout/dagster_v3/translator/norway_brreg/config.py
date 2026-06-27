"""Per-source translation config for Norway Brreg."""
from __future__ import annotations

from translator.config import FieldConfig, SourceConfig
from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE

__all__ = ["FieldConfig", "SourceConfig", "get_config"]

_NORWAY_BRREG_CONFIG = SourceConfig(
    source_slug="norway_brreg",
    source_lang="no",
    ch_table="corpscout.no_companies",
    fields=(
        FieldConfig(original_col="articles_purpose_original"),
        FieldConfig(original_col="activity_text_original"),
        FieldConfig(
            original_col="legal_form_description_original",
            static_map=tuple(LEGAL_FORM_DESCRIPTION_EN_BY_CODE.items()),
            static_key_col="legal_form_code",
        ),
    ),
)


def get_config() -> SourceConfig:
    """Return the Norway Brreg translation source config."""
    return _NORWAY_BRREG_CONFIG
