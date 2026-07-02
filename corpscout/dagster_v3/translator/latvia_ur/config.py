"""Per-source translation config for Latvia UR."""
from __future__ import annotations

from translator.config import FieldConfig, SourceConfig

__all__ = ["FieldConfig", "SourceConfig", "get_config"]

_LATVIA_UR_CONFIG = SourceConfig(
    source_slug="latvia_ur",
    source_lang="lv",
    ch_table="corpscout.lv_companies",
    fields=(FieldConfig(original_col="activity_text_original"),),
)


def get_config() -> SourceConfig:
    """Return the Latvia UR translation source config."""
    return _LATVIA_UR_CONFIG

