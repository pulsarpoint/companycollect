"""Shared contract for source-owned Dagster definitions."""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SourceBundle:
    source_name: str
    asset_key_prefix: tuple[str, ...]
    assets: tuple[Any, ...]
    asset_checks: tuple[Any, ...] = ()
    jobs: tuple[Any, ...] = ()
    schedules: tuple[Any, ...] = ()

    def __post_init__(self) -> None:
        if not self.source_name:
            raise ValueError("source_name must not be empty")
        if not self.asset_key_prefix:
            raise ValueError("asset_key_prefix must not be empty")
