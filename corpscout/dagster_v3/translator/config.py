"""Shared translator dataclasses.

``FieldConfig`` and ``SourceConfig`` live here so shared-core modules
(translator.flush, translator.clickhouse) never import from a per-source
package.  Per-source packages (e.g. translator.norway_brreg.config) import
from here and add source-specific instances / ``get_config()`` helpers.
"""
from __future__ import annotations

from dataclasses import dataclass


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
