from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FieldConfig:
    field: str
    original_col: str


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
            FieldConfig(field="articles_purpose", original_col="articles_purpose_original"),
            FieldConfig(field="activity_text", original_col="activity_text_original"),
            FieldConfig(field="company_description", original_col="company_description_original"),
            FieldConfig(field="legal_form_description", original_col="legal_form_description_original"),
        ),
    ),
}


def get_source_config(source_slug: str) -> SourceConfig:
    return REGISTRY[source_slug]
