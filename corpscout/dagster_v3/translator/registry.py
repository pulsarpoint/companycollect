"""Backward-compat shim — imports FieldConfig / SourceConfig from per-source packages.

New code should import directly from e.g. ``translator.norway_brreg.config``.
This module exists solely so ``translator.import_legacy`` and legacy tests keep
working without change until they are updated in Task 10.
"""
from __future__ import annotations

from translator.norway_brreg.config import FieldConfig, SourceConfig, get_config

_SOURCES = {
    "norway_brreg": get_config(),
}


def get_source_config(source_slug: str) -> SourceConfig:
    return _SOURCES[source_slug]


__all__ = ["FieldConfig", "SourceConfig", "get_source_config"]
