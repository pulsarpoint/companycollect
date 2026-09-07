"""Replay a reviewed technology audit against a frozen catalog and source artifacts."""

import hashlib
import json
import unicodedata
from collections import Counter
from difflib import SequenceMatcher
from pathlib import Path

import click
from bs4 import BeautifulSoup

from company_research.content import normalize_evidence
from company_research.models import Finding


def alias_key(value: str) -> str:
    """Normalize Unicode, case, and whitespace while preserving punctuation."""
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


def fingerprint(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def validate_aliases(aliases: list[dict], catalog: dict[str, dict]) -> None:
    seen: set[str] = set()
    catalog_keys = {alias_key(name) for name in catalog}
    for item in aliases:
        if (
            item["review_status"] != "accepted"
            or item["match_mode"] != "case_insensitive"
        ):
            raise ValueError(
                "This seed resolver requires reviewed case-insensitive aliases"
            )
        key = alias_key(item["alias"])
        if not key or item["alias_key"] != key or key in seen:
            raise ValueError("Empty, invalid, or conflicting alias key")
        if item["technology"] not in catalog:
            raise ValueError(f"Alias target absent from catalog: {item['technology']}")
        if key in catalog_keys:
            raise ValueError(
                "Normalized canonical names cannot be overridden or duplicated by an alias"
            )
        seen.add(key)


def resolve(label: str, catalog: dict[str, dict], aliases: list[dict]) -> dict:
    if label in catalog:
        return {"status": "matched", "technology": label, "method": "exact_catalog"}
    normalized = alias_key(label)
    catalog_candidates = sorted(
        name for name in catalog if alias_key(name) == normalized
    )
    if len(catalog_candidates) == 1:
        return {
            "status": "matched",
            "technology": catalog_candidates[0],
            "method": "normalized_catalog",
        }
    if catalog_candidates:
        return {
            "status": "ambiguous",
            "technology": None,
            "method": "normalized_catalog_collision",
            "candidates": catalog_candidates,
        }
    candidates = {
        row["technology"]
        for row in aliases
        if row["review_status"] == "accepted"
        and row["match_mode"] == "case_insensitive"
        and row["alias_key"] == normalized
        and row["technology"] in catalog
    }
    if len(candidates) == 1:
        return {
            "status": "matched",
            "technology": next(iter(candidates)),
            "method": "accepted_alias",
        }
    return {
        "status": "ambiguous" if candidates else "unresolved",
        "technology": None,
        "method": None,
    }


def suggest_candidates(label: str, catalog: dict[str, dict]) -> list[dict]:
    """Lexical retrieval only; these scores are never mapping confidence."""
    ranked = sorted(
        (
            (
                SequenceMatcher(None, label.casefold(), name.casefold()).ratio(),
                name,
            )
            for name in catalog
        ),
        key=lambda item: (-item[0], item[1]),
    )
    return [
        {
            "technology": name,
            "retrieval_score": round(score, 4),
            "same_name_ignoring_case": name.casefold() == label.casefold(),
            "automatically_accepted": False,
            "catalog_entry": catalog[name],
        }
        for score, name in ranked[:4]
    ]


def load_corpus(
    root: Path, specifications: list[dict]
) -> tuple[list[dict], list[dict]]:
    observations = []
    inputs = []
    for spec in specifications:
        path = root / spec["path"]
        payload = path.read_bytes()
        result = json.loads(payload)
        if result.get("schema_version") != "1.1":
            raise ValueError(f"Unsupported technology artifact schema: {path.name}")
        if spec["format"] == "extraction_diagnostic":
            if "page" not in result or "input_kind" not in result:
                raise ValueError("Diagnostic requires explicit page and input kind")
        elif spec["format"] == "research_result":
            if "pages" not in result or "input_url" not in result:
                raise ValueError("ResearchResult requires pages and input URL")
        else:
            raise ValueError(f"Unknown artifact format: {spec['format']}")
        findings = result["records"]["technology_signals"]
        inputs.append(
            spec | {"sha256": fingerprint(payload), "finding_count": len(findings)}
        )
        for raw in findings:
            finding = Finding.model_validate(raw)
            if not finding.sources:
                raise ValueError("Observation lacks source evidence")
            sources = []
            for source in finding.sources:
                html_path = path.parent / "html" / f"{source.page_id}.html"
                content = html_path.read_text(encoding="utf-8")
                if fingerprint(content.encode()) != source.html_sha256:
                    raise ValueError(f"Source hash mismatch: {html_path}")
                if not 0 <= source.chunk_start < source.chunk_end <= len(content):
                    raise ValueError("Source window is outside the saved HTML")
                window = content[source.chunk_start : source.chunk_end]
                text = BeautifulSoup(window, "html.parser").get_text(" ", strip=True)
                fragments = [
                    fragment.model_dump()
                    | {
                        "presence_rechecked": bool(normalize_evidence(fragment.text))
                        and (
                            normalize_evidence(fragment.text)
                            in normalize_evidence(text)
                            or normalize_evidence(fragment.text)
                            in normalize_evidence(window)
                        )
                    }
                    for fragment in source.evidence
                ]
                sources.append(
                    source.model_dump()
                    | {
                        "evidence": fragments,
                        "html_artifact": str(html_path.relative_to(root)),
                        "source_hash_verified": True,
                    }
                )
            observations.append(
                {
                    "input_artifact": spec["path"],
                    "record_id": finding.record_id,
                    "data": finding.data,
                    "original_evidence_status": finding.evidence_status,
                    "sources": sources,
                }
            )
    return observations, inputs


def coverage(
    observations: list[dict], catalog: dict[str, dict], aliases: list[dict]
) -> dict:
    labels = {row["data"]["technology"] for row in observations}
    matched_labels = {
        name
        for name in labels
        if resolve(name, catalog, aliases)["status"] == "matched"
    }
    matched_count = sum(
        row["data"]["technology"] in matched_labels for row in observations
    )
    return {
        "observation_count": len(observations),
        "matched_observation_count": matched_count,
        "matched_observation_percent": round(100 * matched_count / len(observations), 2)
        if observations
        else None,
        "distinct_raw_label_count": len(labels),
        "matched_distinct_label_count": len(matched_labels),
        "matched_distinct_label_percent": round(
            100 * len(matched_labels) / len(labels), 2
        )
        if labels
        else None,
        "distinct_job_url_count": len(
            {r["data"]["job_url"] for r in observations if r["data"]["job_url"]}
        ),
        "distinct_source_url_count": len(
            {s["url"] for r in observations for s in r["sources"]}
        ),
        "distinct_source_snapshot_count": len(
            {(s["url"], s["html_sha256"]) for r in observations for s in r["sources"]}
        ),
        "distinct_named_company_count": len(
            {r["data"]["company"] for r in observations if r["data"]["company"]}
        ),
        "original_source_matched_count": sum(
            r["original_evidence_status"] == "source_matched" for r in observations
        ),
    }


def build_audit(root: Path, catalog_path: Path, directory: Path) -> dict:
    catalog_bytes = catalog_path.read_bytes()
    snapshot = json.loads(catalog_bytes)
    catalog = {row["technology"]: row for row in snapshot["technologies"]}
    if len(catalog) != snapshot["row_count"]:
        raise ValueError("Catalog row count or canonical-name uniqueness mismatch")
    manifest = json.loads((directory / "inputs.json").read_text(encoding="utf-8"))
    review_bytes = (directory / "reviewed_decisions.json").read_bytes()
    review = json.loads(review_bytes)
    if review["catalog_snapshot_sha256"] != fingerprint(catalog_bytes):
        raise ValueError(
            "Catalog changed; review decisions against the new snapshot first"
        )
    primary, primary_inputs = load_corpus(root, manifest["primary"])
    historical, historical_inputs = load_corpus(root, manifest["historical"])
    labels = sorted({row["data"]["technology"] for row in primary})
    if set(labels) != set(review["decisions"]):
        raise ValueError(
            "Every primary label needs a reviewed decision; no extra seed labels"
        )
    aliases = []
    for label in labels:
        decision = review["decisions"][label]
        for related in decision["related_catalog_names"]:
            if related not in catalog:
                raise ValueError(f"Reviewed candidate missing from snapshot: {related}")
        if "accepted_alias" in decision:
            aliases.append(
                {
                    "alias": label,
                    "alias_key": alias_key(label),
                    **decision["accepted_alias"],
                    "review_status": "accepted",
                    "reviewed_by": review["reviewed_by"],
                    "reviewed_at": review["reviewed_at"],
                    "reason": decision["reason"],
                    "source_references": decision["references"],
                }
            )
    validate_aliases(aliases, catalog)
    alias_payload = {
        "schema_version": "1.1",
        "mapping_version": fingerprint(json.dumps(aliases, sort_keys=True).encode()),
        "catalog_snapshot_sha256": fingerprint(catalog_bytes),
        "aliases": aliases,
    }
    entries = []
    for label in labels:
        matching = [row for row in primary if row["data"]["technology"] == label]
        decision = review["decisions"][label]
        entries.append(
            {
                "technology_raw": label,
                "observation_frequency": len(matching),
                "distinct_job_urls": sorted(
                    {r["data"]["job_url"] for r in matching if r["data"]["job_url"]}
                ),
                "company_names": sorted(
                    {r["data"]["company"] for r in matching if r["data"]["company"]}
                ),
                "before": resolve(label, catalog, []),
                "after": resolve(label, catalog, aliases),
                "classification": decision["classification"],
                "review": decision,
                "catalog_candidates": suggest_candidates(label, catalog),
                "reviewed_related_entries": [
                    catalog[name] for name in decision["related_catalog_names"]
                ],
                "observations": [
                    row | {"mapping": resolve(label, catalog, aliases)}
                    for row in matching
                ],
            }
        )
    historical_mapped = [
        r | {"mapping": resolve(r["data"]["technology"], catalog, aliases)}
        for r in historical
    ]
    catalog_groups: dict[str, list[str]] = {}
    for name in catalog:
        catalog_groups.setdefault(alias_key(name), []).append(name)
    audit = {
        "schema_version": "1.1",
        "audit_date": review["reviewed_at"],
        "catalog": {
            "source_table": snapshot["source_table"],
            "captured_at": snapshot["captured_at"],
            "row_count": len(catalog),
            "snapshot_path": str(catalog_path.resolve()),
            "snapshot_sha256": fingerprint(catalog_bytes),
            "query": snapshot["query"],
        },
        "review": {
            "reviewed_by": review["reviewed_by"],
            "method": review["review_method"],
            "decisions_sha256": fingerprint(review_bytes),
            "candidate_generation": "Top four case-folded name similarities plus explicitly reviewed related entries. Scores are retrieval signals, not identity confidence.",
            "external_model_calls": 0,
        },
        "matching_policy": {
            "normalization": "Unicode NFKC, casefold, whitespace normalization; punctuation preserved",
            "precedence": [
                "exact_catalog",
                "unique_normalized_catalog",
                "accepted_case_insensitive_alias",
            ],
            "catalog_collisions": [
                {"normalized_key": key, "canonical_names": sorted(names)}
                for key, names in sorted(catalog_groups.items())
                if len(names) > 1
            ],
            "before_coverage": "Catalog matching, including unique normalized matches; no aliases",
            "after_coverage": "The same catalog matching plus accepted aliases",
        },
        "inputs": {
            "primary": primary_inputs,
            "historical": historical_inputs,
            "excluded": manifest["excluded"],
        },
        "limitations": manifest["limitations"],
        "primary_coverage": {
            "before": coverage(primary, catalog, []),
            "after": coverage(primary, catalog, aliases),
        },
        "primary_classifications": dict(
            sorted(Counter(e["classification"] for e in entries).items())
        ),
        "historical_coverage": {
            "before": coverage(historical, catalog, []),
            "after": coverage(historical, catalog, aliases),
        },
        "alias_mapping_version": alias_payload["mapping_version"],
        "technologies": entries,
        "historical_mapped_observations": historical_mapped,
    }
    for name, data in [
        ("technology_aliases.json", alias_payload),
        ("technology_mapping_audit.json", audit),
    ]:
        (directory / name).write_text(
            json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    return audit


@click.command()
@click.option(
    "--catalog",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    required=True,
)
def main(catalog: Path) -> None:
    """Rebuild the audit files from frozen inputs and reviewed mapping decisions."""
    directory = Path(__file__).resolve().parent
    audit = build_audit(directory.parent, catalog, directory)
    click.echo(
        json.dumps(
            {
                "primary_coverage": audit["primary_coverage"],
                "classifications": audit["primary_classifications"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
