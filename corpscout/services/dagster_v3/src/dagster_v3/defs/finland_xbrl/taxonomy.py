"""Bundled labels from the official Finnish SBR taxonomy distribution."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

TAXONOMY_VERSION = "SBR-DPM-2022-09-30"
TAXONOMY_SOURCE_URL = (
    "https://www.avoindata.fi/data/dataset/644a8ee5-1de5-4f9d-a7bf-ef5edfcb619a/"
    "resource/73c9a2f2-f440-491b-9098-c3b37b4b0f6e"
)
CATALOG_PATH = Path(__file__).with_name("data") / "finland_sbr_taxonomy_catalog.csv"


@dataclass(frozen=True)
class TaxonomyCode:
    code: str
    code_kind: str
    namespace_hint: str
    label_fi: str
    label_en: str
    label_sv: str
    metric_name_hint: str
    source_artifact: str
    source_url: str
    taxonomy_version: str = TAXONOMY_VERSION


def read_taxonomy_catalog(path: Path = CATALOG_PATH) -> list[TaxonomyCode]:
    with path.open(encoding="utf-8", newline="") as handle:
        return [TaxonomyCode(**row) for row in csv.DictReader(handle)]
