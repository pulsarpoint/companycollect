"""Pure catalog logic: layer loading, merge, slugs, category resolution.

Three layers feed the catalog, later ones winning per technology name:

* the vendored Wappalyzer extension bundle (frozen bootstrap, read-only),
* the maintained public webappanalyzer catalog (the updatable overlay), and
* our curated custom entries (repo-owned, webappanalyzer schema).

Category and group ids are resolved to names via the SAME layer the winning
entry came from — the two public layers' category tables have drifted, so
resolving an overlay entry against the extension's categories (or vice versa)
would mislabel it. The custom layer's vocabulary is the overlay's plus our own
additions at ids 900+, so custom entries may reference standard categories.
"""

import hashlib
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


def load_custom_layer(
    custom_dir: Path,
    *,
    base_categories: Mapping[int, Mapping[str, Any]],
    base_groups: Mapping[int, str],
) -> CatalogLayer:
    """Read the repo-owned custom entries (webappanalyzer schema).

    The layer's vocabulary is the overlay's plus categories.json additions at
    ids >= tables.CUSTOM_CATEGORY_ID_FLOOR. Every entry's category ids must
    resolve — an unresolvable id is a typo, refused at load time rather than
    published as an unlabeled row. source_version is the content hash of the
    two files, so the catalog records exactly which revision published.
    """
    technologies_path = custom_dir / "technologies.json"
    categories_path = custom_dir / "categories.json"
    technologies = json.loads(technologies_path.read_text(encoding="utf-8"))
    custom_categories = parse_categories(
        json.loads(categories_path.read_text(encoding="utf-8"))
    )
    for category_id in custom_categories:
        if category_id < tables.CUSTOM_CATEGORY_ID_FLOOR:
            raise ValueError(
                f"custom category id {category_id} is below the "
                f"{tables.CUSTOM_CATEGORY_ID_FLOOR} floor reserved against "
                "upstream collisions"
            )
    categories = {**base_categories, **custom_categories}
    for name, entry in technologies.items():
        for category_id in entry.get("cats", ()):
            if int(category_id) not in categories:
                raise ValueError(
                    f"custom technology {name!r} references unknown "
                    f"category id {category_id}"
                )
    digest = hashlib.sha256(
        technologies_path.read_bytes() + categories_path.read_bytes()
    ).hexdigest()
    return CatalogLayer(
        technologies=technologies,
        categories=categories,
        groups=dict(base_groups),
        source=tables.CUSTOM_SOURCE,
        source_version=digest[:40],
    )


def merge_layers(*layers: CatalogLayer) -> list[MergedTechnology]:
    """Union of all layers' names; the LAST layer carrying a name wins.

    Callers pass layers in precedence order: extension, overlay, custom.
    Sorted by technology name so every downstream step (icon sync, insert) is
    deterministic run to run.
    """
    merged: list[MergedTechnology] = []
    names = {name for layer in layers for name in layer.technologies}
    for name in sorted(names):
        layer = next(
            layer for layer in reversed(layers) if name in layer.technologies
        )
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
