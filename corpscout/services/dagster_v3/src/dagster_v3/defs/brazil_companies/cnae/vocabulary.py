"""CNAE 2.0, the classification every Brazilian establishment is filed under.

`br_establishments.primary_cnae_code` is a bare seven digits — `4781400` on
3,687,768 of them — and nothing in the database said what that meant. IBGE
publishes the vocabulary through CONCLA's API: 1,332 subclasses, each nested
under a classe, grupo, divisão and seção, in Portuguese.

The NACE bridge is DIVISION level and deliberately goes no deeper. CNAE 2.0 and
NACE Rev.2 are both ISIC Rev.4 derivatives, so their two-digit divisions agree —
measured: all 87 CNAE divisions exist in NACE, and the ones a reader would check
match (49 land transport, 62 IT services, 47 retail, 86 human health).

Below the division they diverge, and a shared code becomes a false friend:

    CNAE 4781  Comércio varejista de artigos do vestuário e acessórios
    NACE 47.81 Retail sale via stalls and markets of food, beverages and tobacco

61.5% of CNAE classes share a four-digit code with NACE, and that particular
one covers more Brazilian establishments than any other. Mapping on code
equality would file 3.7 million clothing shops as market food stalls, so this
maps what is actually true and leaves the rest to a real correspondence table.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.brazil_companies.cnae import tables

CNAE_VERSION = "CNAE_2_0"
NACE_REVISION = "NACE_REV_2"

# Named in the row so a reader can tell a measured bridge from a guessed one.
DIVISION_MAPPING_SOURCE = "isic_rev4_shared_division"

CNAE_SUBCLASSES_URL = "https://servicodados.ibge.gov.br/api/v2/cnae/subclasses"

SECTION = "section"
DIVISION = "division"
GROUP = "group"
CLASS = "class"
SUBCLASS = "subclass"


@dataclass(frozen=True)
class CnaeCategory:
    level: str
    normalized_code: str
    display_code: str
    description_pt: str
    parent_normalized_code: str
    section_code: str
    division_code: str


@dataclass(frozen=True)
class CnaeNaceEdge:
    cnae_normalized_code: str
    cnae_display_code: str
    cnae_description_pt: str
    nace_normalized_code: str
    nace_description_en: str


def cnae_display_code(normalized_code: str) -> str:
    """How Brazil writes the code, from how the register stores it.

    The register publishes `4781400`; CONCLA, every form and every Brazilian
    reader writes `4781-4/00`. Shorter levels keep their own conventional form.
    """
    digits = "".join(c for c in normalized_code if c.isdigit())
    if len(digits) == 7:
        return f"{digits[:4]}-{digits[4]}/{digits[5:]}"
    if len(digits) == 5:
        return f"{digits[:4]}-{digits[4]}"
    if len(digits) == 3:
        return f"{digits[:2]}.{digits[2]}"
    # A division is two digits and a section is a letter; both stand as they are.
    return normalized_code


def _text(value: Any) -> str:
    return str(value or "").strip()


def nace_division_label(code: str, description: str) -> str:
    """The NACE division's name without the code repeated in front of it.

    `nace_categories.description_en` stores "47 Retail trade, except of motor
    vehicles and motorcycles" — code included. The code is already its own
    column here, so leaving it in renders "NACE 47 47 Retail trade".
    """
    text = description.strip()
    prefix = f"{code} "
    return text[len(prefix) :].strip() if text.startswith(prefix) else text


def parse_cnae_subclasses(payload: Sequence[dict]) -> tuple[CnaeCategory, ...]:
    """Every level of the tree, from IBGE's subclass listing.

    The API returns only subclasses, each carrying its ancestors inline, so the
    coarser levels are recovered by deduplicating what the subclasses nest.
    """
    if not payload:
        raise ValueError("no CNAE subclasses in the vocabulary payload")

    # Keyed on (level, code) so an ancestor shared by 40 subclasses is emitted
    # once. dict rather than set to keep the first description seen and the
    # insertion order stable.
    found: dict[tuple[str, str], CnaeCategory] = {}

    def add(
        level: str,
        code: str,
        description: str,
        parent: str,
        section: str,
        division: str,
    ) -> None:
        if not code:
            return
        found.setdefault(
            (level, code),
            CnaeCategory(
                level=level,
                normalized_code=code,
                display_code=cnae_display_code(code),
                description_pt=description,
                parent_normalized_code=parent,
                section_code=section,
                division_code=division,
            ),
        )

    for entry in payload:
        klass = entry.get("classe") or {}
        group = klass.get("grupo") or {}
        division = group.get("divisao") or {}
        section = division.get("secao") or {}

        section_code = _text(section.get("id"))
        division_code = _text(division.get("id"))

        add(SECTION, section_code, _text(section.get("descricao")), "", section_code, "")
        add(
            DIVISION,
            division_code,
            _text(division.get("descricao")),
            section_code,
            section_code,
            division_code,
        )
        add(
            GROUP,
            _text(group.get("id")),
            _text(group.get("descricao")),
            division_code,
            section_code,
            division_code,
        )
        add(
            CLASS,
            _text(klass.get("id")),
            _text(klass.get("descricao")),
            _text(group.get("id")),
            section_code,
            division_code,
        )
        add(
            SUBCLASS,
            _text(entry.get("id")),
            _text(entry.get("descricao")),
            _text(klass.get("id")),
            section_code,
            division_code,
        )

    return tuple(found.values())


def build_cnae_category_rows(
    *,
    subclasses: Sequence[dict],
    source_run_id: str,
    source_url: str = CNAE_SUBCLASSES_URL,
    retrieved_at: datetime | None = None,
) -> list[tuple]:
    """Rows for `br_cnae_categories`, in the migration's column order."""
    stamped = retrieved_at or datetime.now(UTC).replace(tzinfo=None)
    return [
        (
            CNAE_VERSION,
            category.display_code,
            category.normalized_code,
            category.level,
            category.parent_normalized_code,
            category.section_code,
            category.division_code,
            category.description_pt,
            source_url,
            source_run_id,
            stamped,
        )
        for category in parse_cnae_subclasses(subclasses)
    ]


def nace_division_edges(
    categories: Iterable[CnaeCategory],
    *,
    nace_divisions: dict[str, str],
) -> tuple[CnaeNaceEdge, ...]:
    """One edge per CNAE SUBCLASS, to the NACE division it shares with ISIC.

    Only subclasses, because that is what `br_establishments` publishes. A
    division NACE does not carry is skipped rather than mapped to a guess —
    across the whole vocabulary that is only NACE 98, which has no CNAE
    counterpart.
    """
    edges: list[CnaeNaceEdge] = []
    for category in categories:
        if category.level != SUBCLASS:
            continue
        label = nace_divisions.get(category.division_code)
        if label is None:
            continue
        edges.append(
            CnaeNaceEdge(
                cnae_normalized_code=category.normalized_code,
                cnae_display_code=category.display_code,
                cnae_description_pt=category.description_pt,
                nace_normalized_code=category.division_code,
                nace_description_en=label,
            )
        )
    return tuple(edges)


def build_cnae_to_nace_rows(
    edges: Sequence[CnaeNaceEdge],
    *,
    source_run_id: str,
    source_payload_hash: str,
    source_url: str = CNAE_SUBCLASSES_URL,
    retrieved_at: datetime | None = None,
) -> list[tuple]:
    """Rows for `br_cnae_to_nace`, in that table's existing column order.

    `cnae_description_en` is left empty here and filled by the translation
    view: the English name is machine-translated from the Portuguese and must
    not be mistaken for something IBGE published.
    """
    stamped = retrieved_at or datetime.now(UTC).replace(tzinfo=None)
    return [
        (
            CNAE_VERSION,
            edge.cnae_display_code,
            edge.cnae_normalized_code,
            edge.cnae_description_pt,
            "",
            NACE_REVISION,
            edge.nace_normalized_code,
            edge.nace_normalized_code,
            edge.nace_description_en,
            DIVISION_MAPPING_SOURCE,
            source_url,
            source_payload_hash,
            source_run_id,
            stamped,
        )
        for edge in edges
    ]


__all__ = [
    "CNAE_SUBCLASSES_URL",
    "CNAE_VERSION",
    "CnaeCategory",
    "CnaeNaceEdge",
    "DIVISION_MAPPING_SOURCE",
    "NACE_REVISION",
    "build_cnae_category_rows",
    "build_cnae_to_nace_rows",
    "cnae_display_code",
    "nace_division_edges",
    "parse_cnae_subclasses",
    "tables",
]
