"""Administrator-approved catalog entries and aliases survive catalog refreshes."""

import hashlib
import json

from dagster_v3.defs.technology_catalog.aliases import (
    normalize_technology_name,
    parse_technology_aliases,
)
from dagster_v3.defs.technology_catalog.catalog import CatalogLayer


def load_reviewed_technologies(client, *, base: CatalogLayer, existing_names: set[str]):
    rows, columns = client.execute(
        "SELECT * FROM corpscout.technology_proposal_latest_reviews "
        "WHERE decision IN ('approve_new', 'map_existing') ORDER BY proposal_id LIMIT 100001",
        with_column_types=True,
    )
    if len(rows) > 100000:
        raise ValueError(
            "Reviewed technology input exceeded its complete snapshot limit"
        )
    reviews = [
        dict(zip([column[0] for column in columns], row, strict=True)) for row in rows
    ]
    return reviewed_catalog_layer(reviews, base=base, existing_names=existing_names)


def reviewed_catalog_layer(
    reviews: list[dict], *, base: CatalogLayer, existing_names: set[str]
):
    technologies = {}
    normalized_names = {
        normalize_technology_name(name): name for name in existing_names
    }
    aliases = []
    for review in reviews:
        if review["decision"] != "approve_new":
            continue
        name = review["technology"]
        key = normalize_technology_name(name)
        if (
            name in technologies
            or key in normalized_names
            and normalized_names[key] != name
        ):
            raise ValueError(
                f"Reviewed technology has a conflicting canonical identity: {name!r}"
            )
        if (
            not review["description"]
            or not review["website"]
            or not review["category_ids"]
        ):
            raise ValueError(f"Reviewed technology has incomplete metadata: {name!r}")
        categories = [
            base.categories.get(identifier) for identifier in review["category_ids"]
        ]
        if (
            any(category is None for category in categories)
            or [category["name"] for category in categories] != review["categories"]
        ):
            raise ValueError(
                f"Reviewed technology categories changed or are ambiguous: {name!r}"
            )
        technologies[name] = {
            "description": review["description"],
            "website": review["website"],
            "cats": review["category_ids"],
            "saas": bool(review["saas"]),
            "oss": bool(review["oss"]),
            "pricing": review["pricing"],
        }
        normalized_names[key] = name
    all_names = existing_names | set(technologies)
    for review in reviews:
        if review["technology"] not in all_names:
            raise ValueError(
                f"Reviewed mapping target is absent: {review['technology']!r}"
            )
        if review["alias"]:
            if normalize_technology_name(review["alias"]) != review["alias_key"]:
                raise ValueError("Reviewed alias key does not match its spelling")
            aliases.append(
                {
                    "alias": review["alias"],
                    "technology": review["technology"],
                    "reviewed_by": review["reviewed_by"],
                    "reviewed_at": review["reviewed_at"].date().isoformat(),
                    "review_note": review["review_note"],
                    "source_references": review["source_references"],
                }
            )
    # More than one proposal can approve the same synonym, but not different targets.
    unique_aliases = {}
    for alias in aliases:
        key = normalize_technology_name(alias["alias"])
        if (
            key in unique_aliases
            and unique_aliases[key]["technology"] != alias["technology"]
        ):
            raise ValueError("Administrators approved conflicting technology aliases")
        unique_aliases[key] = alias
    version = hashlib.sha256(
        json.dumps(reviews, sort_keys=True, default=str).encode()
    ).hexdigest()
    return (
        CatalogLayer(
            technologies, base.categories, base.groups, "admin_review", version
        ),
        parse_technology_aliases(
            {"schema_version": "1.0", "aliases": list(unique_aliases.values())},
            all_names,
        ),
    )
