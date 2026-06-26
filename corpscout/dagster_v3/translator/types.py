from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SmokeTranslationInput:
    item_id: str
    source_text: str


@dataclass(frozen=True)
class SmokeTranslationResult:
    item_id: str
    translated_text: str
