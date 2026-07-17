"""Build the compact Finland taxonomy label catalog from an extracted SBR ZIP."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote

from lxml import etree

LINK_NS = "http://www.xbrl.org/2003/linkbase"
XLINK_NS = "http://www.w3.org/1999/xlink"
XML_NS = "http://www.w3.org/XML/1998/namespace"
STANDARD_LABEL_ROLE = "http://www.xbrl.org/2003/role/label"
SOURCE_URL = (
    "https://www.avoindata.fi/data/dataset/644a8ee5-1de5-4f9d-a7bf-ef5edfcb619a/"
    "resource/73c9a2f2-f440-491b-9098-c3b37b4b0f6e"
)
METRIC_HINTS = {
    "fi_MC:x673": "revenue",
    "fi_MC:x689": "operating_profit_loss",
    "fi_MC:x740": "profit_loss",
    "fi_MC:x360": "total_assets",
    "fi_MC:x376": "equity",
    "fi_MC:x424": "liabilities",
    "fi_MC:x399": "cash_and_bank",
    "fi_MC:x435": "current_assets",
    "fi_MC:x1768": "current_receivables",
    "fi_MC:x1811": "current_liabilities",
    "fi_MC:x5": "personnel_expenses",
    "fi_MC:x6": "wages_and_salaries",
    "fi_met:ii52": "employees",
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("taxonomy_root", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    rows = build_catalog(args.taxonomy_root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def build_catalog(taxonomy_root: Path) -> list[dict[str, str]]:
    labels: dict[str, dict[str, str]] = defaultdict(dict)
    priorities: dict[tuple[str, str], int] = {}
    artifacts: dict[str, str] = {}
    kinds: dict[str, str] = {}
    namespaces: dict[str, str] = {}
    for path in sorted(taxonomy_root.glob("**/dict/**/*-lab-*.xml")):
        language = _language_for(path)
        if language not in {"fi", "en", "sv"}:
            continue
        for code, label in _labels_from_linkbase(path):
            priority = _label_priority(path)
            if not label or priority >= priorities.get((code, language), 100):
                continue
            labels[code][language] = label
            priorities[(code, language)] = priority
            if language == "fi" or code not in artifacts:
                artifacts[code] = str(path.relative_to(taxonomy_root))
            kinds[code] = _code_kind(path)
            namespaces[code] = _namespace_hint(path)
    rows = []
    for code in sorted(labels):
        localized = labels[code]
        label_fi = localized.get("fi") or localized.get("en") or localized.get("sv") or code
        rows.append(
            {
                "code": code,
                "code_kind": kinds[code],
                "namespace_hint": namespaces[code],
                "label_fi": label_fi,
                "label_en": localized.get("en", ""),
                "label_sv": localized.get("sv", ""),
                "metric_name_hint": METRIC_HINTS.get(code, ""),
                "source_artifact": artifacts[code],
                "source_url": SOURCE_URL,
            }
        )
    if not rows:
        raise ValueError(f"No dictionary labels found below {taxonomy_root}")
    return rows


def _labels_from_linkbase(path: Path) -> list[tuple[str, str]]:
    root = etree.parse(path).getroot()
    pairs: list[tuple[str, str]] = []
    for link in root.findall(f".//{{{LINK_NS}}}labelLink"):
        locators = {
            item.get(f"{{{XLINK_NS}}}label", ""): item.get(f"{{{XLINK_NS}}}href", "")
            for item in link.findall(f"{{{LINK_NS}}}loc")
        }
        resources = {
            item.get(f"{{{XLINK_NS}}}label", ""): " ".join(item.itertext()).strip()
            for item in link.findall(f"{{{LINK_NS}}}label")
            if item.get(f"{{{XLINK_NS}}}role", STANDARD_LABEL_ROLE) == STANDARD_LABEL_ROLE
        }
        for arc in link.findall(f"{{{LINK_NS}}}labelArc"):
            href = locators.get(arc.get(f"{{{XLINK_NS}}}from", ""), "")
            label = resources.get(arc.get(f"{{{XLINK_NS}}}to", ""), "")
            code = _code_for(path, href)
            if code and label:
                pairs.append((code, label))
    return pairs


def _code_for(path: Path, href: str) -> str:
    anchor = unquote(href).partition("#")[2]
    local = anchor.removeprefix("fi_")
    if not local:
        return ""
    lowered = [part.lower() for part in path.parts]
    if "met" in lowered:
        prefix = "fi_met"
    elif "dim" in lowered:
        prefix = "fi_dim"
    elif "dom" in lowered:
        domain = lowered[lowered.index("dom") + 1]
        prefix = f"fi_{domain.upper()}"
    else:
        return ""
    return f"{prefix}:{local}"


def _language_for(path: Path) -> str:
    return path.stem.rsplit("-", maxsplit=1)[-1].lower()


def _label_priority(path: Path) -> int:
    name = path.name.lower()
    if name.startswith(("mem-lab-", "met-lab-", "dim-lab-")):
        return 0
    if "hier-lab-mem-" in name:
        return 2
    return 1


def _code_kind(path: Path) -> str:
    lowered = {part.lower() for part in path.parts}
    if "met" in lowered:
        return "concept"
    if "dim" in lowered:
        return "dimension"
    return "member"


def _namespace_hint(path: Path) -> str:
    lowered = [part.lower() for part in path.parts]
    if "met" in lowered:
        return "http://www.suomi.fi/xbrl/crr/dict/met"
    if "dim" in lowered:
        return "http://www.suomi.fi/xbrl/crr/dict/dim"
    domain = lowered[lowered.index("dom") + 1]
    return f"http://www.suomi.fi/xbrl/crr/dict/dom/{domain.upper()}"


if __name__ == "__main__":
    main()
