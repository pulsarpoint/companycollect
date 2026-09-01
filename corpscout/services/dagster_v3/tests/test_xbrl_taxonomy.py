"""Tests for the Arelle-once taxonomy dictionary builder.

Uses small fake Arelle model objects (never a real taxonomy load) to verify
`concept_rows_from_model`'s row shapes, structural-namespace filtering, and
presentation/calculation parent resolution. Entry-point enumeration is
tested against a minimal in-memory zip, not a real taxonomy package.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
import zipfile

import pytest

from dagster_v3.defs.xbrl_common.extractor import SourceProfile
from dagster_v3.defs.xbrl_common.tables import TAXONOMY_CONCEPT_COLUMNS, TAXONOMY_LABEL_COLUMNS
from dagster_v3.defs.xbrl_common.taxonomy import (
    concept_rows_from_model,
    package_entrypoints,
)

EX_NS = "http://example.com/taxonomy/2026"
STRUCTURAL_NS = "http://www.xbrl.org/2003/instance"

TEST_PROFILE = SourceProfile(
    source_slug="test_source",
    canonical_prefixes={EX_NS: "ex", STRUCTURAL_NS: "xbrli"},
    reported_concepts={},
)


@dataclass(frozen=True)
class FakeQName:
    """Mirrors arelle.ModelValue.QName: str() is `prefix:localName` (the
    document's own declared prefix), not the SourceProfile's canonical
    prefix -- concept_rows_from_model relies on this directly for
    `item_type` while it re-derives `concept_qname`/`substitution_group`
    via the profile's canonical_prefixes."""

    namespaceURI: str
    localName: str
    prefix: str | None = None

    def __str__(self) -> str:
        if self.prefix:
            return f"{self.prefix}:{self.localName}"
        return self.localName


@dataclass
class FakeConcept:
    qname: FakeQName
    substitutionGroupQname: FakeQName | None = None
    isAbstract: bool = False
    typeQname: FakeQName | None = None
    balance: str | None = None
    periodType: str | None = None


@dataclass
class FakeLabelResource:
    role: str
    xmlLang: str
    stringValue: str


@dataclass
class FakeRelationship:
    fromModelObject: Any
    toModelObject: Any
    order: float = 0.0
    weight: float = 0.0
    linkrole: str = ""


@dataclass
class FakeRelationshipSet:
    modelRelationships: list = field(default_factory=list)


class FakeModelXbrl:
    def __init__(
        self,
        *,
        qname_concepts: dict[FakeQName, FakeConcept],
        relationship_sets: dict[str, FakeRelationshipSet],
    ) -> None:
        self.qnameConcepts = qname_concepts
        self._relationship_sets = relationship_sets

    def relationshipSet(self, arcrole: str) -> FakeRelationshipSet:
        return self._relationship_sets.get(arcrole, FakeRelationshipSet([]))


def _concept(local_name: str, **kwargs: Any) -> FakeConcept:
    return FakeConcept(qname=FakeQName(EX_NS, local_name), **kwargs)


def _rows_by_concept(rows: list[dict], key: str = "concept_qname") -> dict[str, list[dict]]:
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        grouped.setdefault(row[key], []).append(row)
    return grouped


def test_concept_rows_match_taxonomy_concept_columns_shape() -> None:
    revenue = _concept(
        "Revenue",
        substitutionGroupQname=FakeQName(EX_NS, "item"),
        isAbstract=False,
        typeQname=FakeQName(EX_NS, "monetaryItemType", prefix="ex"),
        balance="credit",
        periodType="duration",
    )
    model = FakeModelXbrl(
        qname_concepts={revenue.qname: revenue},
        relationship_sets={},
    )

    concept_rows, label_rows = concept_rows_from_model(
        model,
        taxonomy_version="TEST-2026",
        profile=TEST_PROFILE,
        loaded_at="2026-08-31T00:00:00Z",
    )

    assert len(concept_rows) == 1
    assert set(concept_rows[0]) == set(TAXONOMY_CONCEPT_COLUMNS)
    assert label_rows == []

    row = concept_rows[0]
    assert row["taxonomy_version"] == "TEST-2026"
    assert row["concept_qname"] == "ex:Revenue"
    assert row["concept_namespace"] == EX_NS
    assert row["concept_local_name"] == "Revenue"
    assert row["substitution_group"] == "ex:item"
    assert row["is_abstract"] is False
    assert row["item_type"] == "ex:monetaryItemType"
    assert row["balance"] == "credit"
    assert row["period_type"] == "duration"
    # No relationships supplied: presentation/calculation default to empty.
    assert row["presentation_parent"] == ""
    assert row["presentation_order"] == 0.0
    assert row["presentation_role"] == ""
    assert row["calculation_parent"] == ""
    assert row["calculation_weight"] == 0.0
    assert row["calculation_role"] == ""


def test_label_rows_match_taxonomy_label_columns_shape() -> None:
    revenue = _concept("Revenue")
    label = FakeLabelResource(
        role="http://www.xbrl.org/2003/role/label",
        xmlLang="EN",
        stringValue="Revenue",
    )
    model = FakeModelXbrl(
        qname_concepts={revenue.qname: revenue},
        relationship_sets={
            "http://www.xbrl.org/2003/arcrole/concept-label": FakeRelationshipSet(
                [FakeRelationship(fromModelObject=revenue, toModelObject=label)]
            ),
        },
    )

    _, label_rows = concept_rows_from_model(
        model,
        taxonomy_version="TEST-2026",
        profile=TEST_PROFILE,
        loaded_at="2026-08-31T00:00:00Z",
    )

    assert len(label_rows) == 1
    assert set(label_rows[0]) == set(TAXONOMY_LABEL_COLUMNS)
    row = label_rows[0]
    assert row["taxonomy_version"] == "TEST-2026"
    assert row["concept_qname"] == "ex:Revenue"
    assert row["language"] == "en"
    assert row["label_role"] == "http://www.xbrl.org/2003/role/label"
    assert row["label"] == "Revenue"


def test_structural_namespace_concepts_and_labels_are_skipped() -> None:
    structural_concept = FakeConcept(qname=FakeQName(STRUCTURAL_NS, "context"))
    real_concept = _concept("Revenue")
    label = FakeLabelResource(role="label", xmlLang="en", stringValue="Structural")

    model = FakeModelXbrl(
        qname_concepts={
            structural_concept.qname: structural_concept,
            real_concept.qname: real_concept,
        },
        relationship_sets={
            "http://www.xbrl.org/2003/arcrole/concept-label": FakeRelationshipSet(
                [
                    FakeRelationship(
                        fromModelObject=structural_concept, toModelObject=label
                    )
                ]
            ),
        },
    )

    concept_rows, label_rows = concept_rows_from_model(
        model,
        taxonomy_version="TEST-2026",
        profile=TEST_PROFILE,
        loaded_at="2026-08-31T00:00:00Z",
    )

    assert [row["concept_qname"] for row in concept_rows] == ["ex:Revenue"]
    assert label_rows == []


def test_presentation_and_calculation_parents_are_filled_from_relationships() -> None:
    parent = _concept("RevenueAbstract", isAbstract=True)
    child = _concept("Revenue")
    calc_parent = _concept("TotalIncome")

    model = FakeModelXbrl(
        qname_concepts={parent.qname: parent, child.qname: child, calc_parent.qname: calc_parent},
        relationship_sets={
            "http://www.xbrl.org/2003/arcrole/parent-child": FakeRelationshipSet(
                [
                    FakeRelationship(
                        fromModelObject=parent,
                        toModelObject=child,
                        order=1.0,
                        linkrole="http://example.com/role/statement",
                    )
                ]
            ),
            "http://www.xbrl.org/2003/arcrole/summation-item": FakeRelationshipSet(
                [
                    FakeRelationship(
                        fromModelObject=calc_parent,
                        toModelObject=child,
                        weight=1.0,
                        linkrole="http://example.com/role/calc",
                    )
                ]
            ),
        },
    )

    concept_rows, _ = concept_rows_from_model(
        model,
        taxonomy_version="TEST-2026",
        profile=TEST_PROFILE,
        loaded_at="2026-08-31T00:00:00Z",
    )

    by_concept = _rows_by_concept(concept_rows)
    [child_row] = by_concept["ex:Revenue"]
    assert child_row["presentation_parent"] == "ex:RevenueAbstract"
    assert child_row["presentation_order"] == 1.0
    assert child_row["presentation_role"] == "http://example.com/role/statement"
    assert child_row["calculation_parent"] == "ex:TotalIncome"
    assert child_row["calculation_weight"] == 1.0
    assert child_row["calculation_role"] == "http://example.com/role/calc"

    # Parent/calc-parent concepts themselves have no incoming relationships.
    [parent_row] = by_concept["ex:RevenueAbstract"]
    assert parent_row["presentation_parent"] == ""
    assert parent_row["is_abstract"] is True


def test_concept_in_multiple_presentation_roles_emits_one_row_per_role() -> None:
    parent_a = _concept("SectionA")
    parent_b = _concept("SectionB")
    child = _concept("Revenue")

    model = FakeModelXbrl(
        qname_concepts={
            parent_a.qname: parent_a,
            parent_b.qname: parent_b,
            child.qname: child,
        },
        relationship_sets={
            "http://www.xbrl.org/2003/arcrole/parent-child": FakeRelationshipSet(
                [
                    FakeRelationship(
                        fromModelObject=parent_a,
                        toModelObject=child,
                        order=1.0,
                        linkrole="role-a",
                    ),
                    FakeRelationship(
                        fromModelObject=parent_b,
                        toModelObject=child,
                        order=2.0,
                        linkrole="role-b",
                    ),
                ]
            ),
        },
    )

    concept_rows, _ = concept_rows_from_model(
        model,
        taxonomy_version="TEST-2026",
        profile=TEST_PROFILE,
        loaded_at="2026-08-31T00:00:00Z",
    )

    by_concept = _rows_by_concept(concept_rows)
    child_rows = by_concept["ex:Revenue"]
    assert len(child_rows) == 2
    roles = {row["presentation_role"] for row in child_rows}
    assert roles == {"role-a", "role-b"}
    parents_by_role = {row["presentation_role"]: row["presentation_parent"] for row in child_rows}
    assert parents_by_role == {"role-a": "ex:SectionA", "role-b": "ex:SectionB"}


_TAXONOMY_PACKAGE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<tp:taxonomyPackage
    xmlns:tp="http://xbrl.org/2016/taxonomy-package"
    xmlns:xlink="http://www.w3.org/1999/xlink">
  <tp:entryPoints>
    <tp:entryPoint>
      <tp:name>Test entry point</tp:name>
      <tp:entryPointDocument xlink:type="simple" xlink:href="taxonomy/entry.xsd"/>
    </tp:entryPoint>
  </tp:entryPoints>
</tp:taxonomyPackage>
"""


def _write_taxonomy_package_zip(path: Path, *, xml: str = _TAXONOMY_PACKAGE_XML) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("test-package/META-INF/taxonomyPackage.xml", xml)


def test_package_entrypoints_reads_entry_point_document_hrefs(tmp_path: Path) -> None:
    package_path = tmp_path / "package.zip"
    _write_taxonomy_package_zip(package_path)

    entrypoints = package_entrypoints(package_path)

    assert entrypoints == ["taxonomy/entry.xsd"]


def test_package_entrypoints_raises_without_taxonomy_package_manifest(tmp_path: Path) -> None:
    package_path = tmp_path / "empty.zip"
    with zipfile.ZipFile(package_path, "w") as archive:
        archive.writestr("test-package/README.txt", "no manifest here")

    with pytest.raises(ValueError, match="META-INF/taxonomyPackage.xml"):
        package_entrypoints(package_path)


def test_package_entrypoints_raises_when_no_entry_points_listed(tmp_path: Path) -> None:
    package_path = tmp_path / "no_entrypoints.zip"
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<tp:taxonomyPackage
    xmlns:tp="http://xbrl.org/2016/taxonomy-package"
    xmlns:xlink="http://www.w3.org/1999/xlink">
  <tp:entryPoints/>
</tp:taxonomyPackage>
"""
    _write_taxonomy_package_zip(package_path, xml=xml)

    with pytest.raises(ValueError, match="no entry points"):
        package_entrypoints(package_path)
