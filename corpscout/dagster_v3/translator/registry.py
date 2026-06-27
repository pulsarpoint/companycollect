from __future__ import annotations

from dataclasses import dataclass

from translator.static_maps import LEGAL_FORM_DESCRIPTION_EN_BY_CODE


@dataclass(frozen=True)
class FieldConfig:
    original_col: str
    # Frozen tuple-of-pairs keeps FieldConfig hashable; convert to dict at use via static_map_dict().
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


REGISTRY: dict[str, SourceConfig] = {
    "norway_brreg": SourceConfig(
        source_slug="norway_brreg",
        source_lang="no",
        ch_table="corpscout.no_companies",
        fields=(
            FieldConfig(original_col="articles_purpose_original"),
            FieldConfig(original_col="activity_text_original"),
            FieldConfig(original_col="company_description_original"),
            FieldConfig(
                original_col="legal_form_description_original",
                static_map=tuple(LEGAL_FORM_DESCRIPTION_EN_BY_CODE.items()),
                static_key_col="legal_form_code",
            ),
        ),
    ),
}


def get_source_config(source_slug: str) -> SourceConfig:
    return REGISTRY[source_slug]
