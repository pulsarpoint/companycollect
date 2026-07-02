"""Lookup table for translator source configs.

The Temporal workflow is shared across country sources, while each source owns
its ClickHouse table, source language, and translatable columns.
"""
from __future__ import annotations

from translator.config import SourceConfig


def get_source_config(source_slug: str) -> SourceConfig:
    if source_slug == "norway_brreg":
        from translator.norway_brreg.config import get_config
    elif source_slug == "latvia_ur":
        from translator.latvia_ur.config import get_config
    else:
        raise KeyError(f"Unknown translator source_slug: {source_slug!r}")
    return get_config()

