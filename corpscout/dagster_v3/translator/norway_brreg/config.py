"""Per-source translation config for Norway Brreg.

Owns the FieldConfig / SourceConfig dataclasses (shared across the translator
package via imports from this module).
"""
from __future__ import annotations

from dataclasses import dataclass

from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


@dataclass(frozen=True)
class FieldConfig:
    """Config for a single translatable field.

    ``static_map`` is a tuple-of-pairs (hashable) so FieldConfig itself is
    hashable.  Convert to dict at use-time via ``static_map_dict()``.
    ``static_key_col`` is the companion CH column whose value is the map key.
    """

    original_col: str
    static_map: tuple[tuple[str, str], ...] | None = None
    static_key_col: str | None = None

    def static_map_dict(self) -> dict[str, str] | None:
        if self.static_map is None:
            return None
        return dict(self.static_map)


@dataclass(frozen=True)
class SourceConfig:
    source_slug: str
    source_lang: str
    ch_table: str
    fields: tuple[FieldConfig, ...]


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
