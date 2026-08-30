"""Executable fingerprint extraction from the merged catalog layers.

Wave 1 extracts the Wappalyzer `dns` blocks (MX/TXT/SOA/... record-type
patterns) from whichever layer wins each technology name — the same
winning-entry resolution the catalog rows use, so a fingerprint's
`technology` always joins corpscout.technology_catalog by equality.

Wappalyzer patterns may carry "\\;confidence:N" and "\\;version:V" tails;
those are parsed off into dedicated fields so the stored `pattern` is a
clean regex a SQL pass can hand to match().
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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
