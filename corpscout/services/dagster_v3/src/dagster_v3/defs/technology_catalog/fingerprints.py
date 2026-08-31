"""Executable fingerprint extraction from the merged catalog layers.

Wave 1 extracts the Wappalyzer `dns` blocks (MX/TXT/SOA/... record-type
patterns) from whichever layer wins each technology name — the same
winning-entry resolution the catalog rows use, so a fingerprint's
`technology` always joins corpscout.technology_catalog by equality.

Wappalyzer patterns may carry "\\;confidence:N" and "\\;version:V" tails;
those are parsed off into dedicated fields so the stored `pattern` is a
clean regex a SQL pass can hand to match().
"""

import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from dataclasses import dataclass
from typing import Any

from dagster_v3.defs.technology_catalog import tables
from dagster_v3.defs.technology_catalog.catalog import CatalogLayer, winning_entries

DNS_SIGNAL_PREFIX = "dns_"

DEFAULT_CONFIDENCE = 100


@dataclass(frozen=True)
class Fingerprint:
    technology: str
    signal_type: str
    pattern: str
    confidence: int
    version_template: str
    source: str
    source_version: str


def parse_pattern(raw: str) -> tuple[str, int, str]:
    """Split a Wappalyzer pattern into (regex, confidence, version template)."""
    parts = raw.split("\\;")
    pattern = parts[0]
    confidence = DEFAULT_CONFIDENCE
    version_template = ""
    for tail in parts[1:]:
        key, _, value = tail.partition(":")
        if key == "confidence" and value.isdigit():
            confidence = min(int(value), 100)
        elif key == "version":
            version_template = value
    return pattern, confidence, version_template


def _patterns(value: Any) -> list[str]:
    """Wappalyzer allows a single pattern string or a list of them."""
    if isinstance(value, str):
        return [value]
    return [str(item) for item in value]


def load_fingerprint_overrides(
    custom_dir: Path,
) -> tuple[dict[str, Mapping[str, Any]], str]:
    """Pattern-only additions from custom/fingerprints.json.

    Unlike custom/technologies.json entries, these attach extra dns patterns
    to technologies that ALREADY exist in the catalog (usually rich upstream
    entries a custom entry must not shadow). Returns the name → dns-mapping
    dict plus the file's content hash for source_version; a missing file is
    an empty set.
    """
    path = custom_dir / "fingerprints.json"
    if not path.is_file():
        return {}, ""
    overrides = json.loads(path.read_text(encoding="utf-8"))
    for name, dns in overrides.items():
        if not isinstance(dns, Mapping) or not dns:
            raise ValueError(
                f"fingerprint override {name!r} must map record types to "
                "pattern lists"
            )
    return overrides, hashlib.sha256(path.read_bytes()).hexdigest()[:40]


def extract_override_fingerprints(
    overrides: Mapping[str, Mapping[str, Any]],
    known_technologies: set[str],
    source_version: str,
) -> tuple[list[Fingerprint], list[str]]:
    """Override fingerprints for names the merged catalog knows.

    An unknown name (an upstream rename, or a typo) is returned in the second
    element for the caller to log — it must not fail the whole publish.
    """
    fingerprints: list[Fingerprint] = []
    unknown: list[str] = []
    for name in sorted(overrides):
        if name not in known_technologies:
            unknown.append(name)
            continue
        for record_type in sorted(overrides[name], key=str.lower):
            for raw in _patterns(overrides[name][record_type]):
                pattern, confidence, version_template = parse_pattern(raw)
                if not pattern:
                    continue
                fingerprints.append(
                    Fingerprint(
                        technology=name,
                        signal_type=f"{DNS_SIGNAL_PREFIX}{record_type.lower()}",
                        pattern=pattern,
                        confidence=confidence,
                        version_template=version_template,
                        source=tables.CUSTOM_SOURCE,
                        source_version=source_version,
                    )
                )
    return fingerprints, unknown


def extract_dns_fingerprints(*layers: CatalogLayer) -> list[Fingerprint]:
    """Every winning entry's dns block as rows, deterministically ordered."""
    fingerprints: list[Fingerprint] = []
    for name, entry, layer in winning_entries(*layers):
        dns = entry.get("dns")
        if not isinstance(dns, Mapping):
            continue
        for record_type in sorted(dns, key=str.lower):
            for raw in _patterns(dns[record_type]):
                pattern, confidence, version_template = parse_pattern(raw)
                if not pattern:
                    continue
                fingerprints.append(
                    Fingerprint(
                        technology=name,
                        signal_type=f"{DNS_SIGNAL_PREFIX}{record_type.lower()}",
                        pattern=pattern,
                        confidence=confidence,
                        version_template=version_template,
                        source=layer.source,
                        source_version=layer.source_version,
                    )
                )
    return fingerprints
