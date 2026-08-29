"""Pure catalog logic: layer loading, merge, slugs, category resolution.

Two layers feed the catalog:

* the vendored Wappalyzer extension bundle (frozen bootstrap, read-only), and
* the maintained public webappanalyzer catalog (the updatable overlay).

The overlay wins entirely for any technology name present in both layers.
Category and group ids are resolved to names via the SAME layer the winning
entry came from — the two layers' category tables have drifted, so resolving
an overlay entry against the extension's categories (or vice versa) would
mislabel it.
"""

import json
import re
import string
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster_v3.defs.technology_catalog import tables

TECHNOLOGY_LETTERS = ("_", *string.ascii_lowercase)

_SLUG_KEEP = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class CatalogLayer:
    """One source layer: technologies plus the vocabulary to label them."""

    technologies: Mapping[str, Mapping[str, Any]]
    categories: Mapping[int, Mapping[str, Any]]
    groups: Mapping[int, str]
    source: str
    source_version: str


@dataclass(frozen=True)
class MergedTechnology:
    technology: str
    slug: str
    description: str
    website: str
    category_ids: tuple[int, ...]
    categories: tuple[str, ...]
    groups: tuple[str, ...]
    icon_filename: str
    saas: bool
    oss: bool
    pricing: tuple[str, ...]
    source: str
    source_version: str


def slugify(name: str) -> str:
    """Stable slug: lowercase, alnum runs joined by single hyphens."""
    return "-".join(_SLUG_KEEP.findall(name.lower()))


def parse_categories(raw: Mapping[str, Any]) -> dict[int, Mapping[str, Any]]:
    return {int(category_id): entry for category_id, entry in raw.items()}


def parse_groups(raw: Mapping[str, Any]) -> dict[int, str]:
    return {int(group_id): str(entry["name"]) for group_id, entry in raw.items()}


def load_extension_layer(bundle_dir: Path) -> CatalogLayer:
    """Read the vendored extension bundle. Never writes into it."""
    if not bundle_dir.is_dir():
        raise FileNotFoundError(
            f"extension bundle not found at {bundle_dir}; set "
            "TECHNOLOGY_CATALOG_EXTENSION_DIR to the vendored 6.12.5_0 directory"
        )
    technologies: dict[str, Mapping[str, Any]] = {}
    for letter in TECHNOLOGY_LETTERS:
        path = bundle_dir / "technologies" / f"{letter}.json"
        technologies.update(json.loads(path.read_text(encoding="utf-8")))
    return CatalogLayer(
        technologies=technologies,
        categories=parse_categories(
            json.loads((bundle_dir / "categories.json").read_text(encoding="utf-8"))
        ),
        groups=parse_groups(
            json.loads((bundle_dir / "groups.json").read_text(encoding="utf-8"))
        ),
        source=tables.EXTENSION_SOURCE,
        source_version=tables.EXTENSION_VERSION,
    )


def merge_layers(
    extension: CatalogLayer, overlay: CatalogLayer
) -> list[MergedTechnology]:
    """Union of both layers' names; the overlay wins where both carry a name.

    Sorted by technology name so every downstream step (icon sync, insert) is
    deterministic run to run.
    """
    merged: list[MergedTechnology] = []
    names = set(extension.technologies) | set(overlay.technologies)
    for name in sorted(names):
        layer = overlay if name in overlay.technologies else extension
        merged.append(_build_entry(name, layer.technologies[name], layer))
    return merged


def _build_entry(
    name: str, entry: Mapping[str, Any], layer: CatalogLayer
) -> MergedTechnology:
    category_ids = tuple(int(category_id) for category_id in entry.get("cats", ()))
    category_names: list[str] = []
    group_ids: list[int] = []
    for category_id in category_ids:
        category = layer.categories.get(category_id)
        if category is None:
            # An id the layer's own vocabulary does not know: keep the id (it
            # is still the source's claim) but there is no name to resolve.
            continue
        category_names.append(str(category["name"]))
        for group_id in category.get("groups", ()):
            if int(group_id) not in group_ids:
                group_ids.append(int(group_id))
    group_names = tuple(
        layer.groups[group_id] for group_id in group_ids if group_id in layer.groups
    )
    pricing = entry.get("pricing", ())
    return MergedTechnology(
        technology=name,
        slug=slugify(name),
        description=str(entry.get("description", "") or ""),
        website=str(entry.get("website", "") or ""),
        category_ids=category_ids,
        categories=tuple(category_names),
        groups=group_names,
        icon_filename=str(entry.get("icon", "") or ""),
        saas=bool(entry.get("saas", False)),
        oss=bool(entry.get("oss", False)),
        pricing=tuple(str(item) for item in pricing),
        source=layer.source,
        source_version=layer.source_version,
    )
