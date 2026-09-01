"""Arelle-once taxonomy dictionary builder (offline, per taxonomy version)."""

from __future__ import annotations

import zipfile
from pathlib import Path

from lxml import etree

from dagster_v3.defs.xbrl_common.extractor import SourceProfile

_TP_NS = "http://xbrl.org/2016/taxonomy-package"
_XLINK_NS = "http://www.w3.org/1999/xlink"
_STRUCTURAL_PREFIX = "http://www.xbrl.org/"


def package_entrypoints(package_path: Path) -> list[str]:
    with zipfile.ZipFile(package_path) as archive:
        candidates = [
            name for name in archive.namelist()
            if name.endswith("META-INF/taxonomyPackage.xml")
        ]
        if not candidates:
            raise ValueError(f"no META-INF/taxonomyPackage.xml in {package_path}")
        root = etree.fromstring(archive.read(candidates[0]))
    hrefs = [
        element.get(f"{{{_XLINK_NS}}}href") or ""
        for element in root.iter(f"{{{_TP_NS}}}entryPointDocument")
    ]
    hrefs = [href for href in hrefs if href]
    if not hrefs:
        raise ValueError(f"taxonomy package lists no entry points: {package_path}")
    return hrefs


def load_taxonomy_package(
    *,
    package_path: Path,
    entrypoint_url: str | None = None,
    cache_dir: Path | None = None,
):
    from arelle import Cntlr, ModelManager, PackageManager

    controller = Cntlr.Cntlr(logFileName="logToStdErr")
    if cache_dir is not None:
        controller.webCache.cacheDir = str(cache_dir)
    PackageManager.init(controller)
    PackageManager.addPackage(controller, str(package_path))
    PackageManager.rebuildRemappings(controller)
    manager = ModelManager.initialize(controller)
    entrypoint = entrypoint_url or package_entrypoints(package_path)[0]
    model_xbrl = manager.load(entrypoint)
    if model_xbrl is None or not model_xbrl.qnameConcepts:
        raise ValueError(f"Arelle could not load taxonomy entrypoint: {entrypoint}")
    return model_xbrl, entrypoint


def concept_rows_from_model(
    model_xbrl,
    *,
    taxonomy_version: str,
    profile: SourceProfile,
    loaded_at: str,
) -> tuple[list[dict], list[dict]]:
    from arelle import XbrlConst

    def canonical(qname) -> str:
        if qname is None:
            return ""
        prefix = profile.canonical_prefixes.get(qname.namespaceURI)
        return f"{prefix}:{qname.localName}" if prefix else str(qname)

    presentation: dict[str, list[tuple[str, float, str]]] = {}
    for rel in model_xbrl.relationshipSet(XbrlConst.parentChild).modelRelationships:
        child = canonical(rel.toModelObject.qname)
        presentation.setdefault(child, []).append(
            (canonical(rel.fromModelObject.qname), float(rel.order or 0.0),
             rel.linkrole or "")
        )
    calculation: dict[str, list[tuple[str, float, str]]] = {}
    for rel in model_xbrl.relationshipSet(XbrlConst.summationItem).modelRelationships:
        child = canonical(rel.toModelObject.qname)
        calculation.setdefault(child, []).append(
            (canonical(rel.fromModelObject.qname), float(rel.weight or 0.0),
             rel.linkrole or "")
        )

    concept_rows: list[dict] = []
    for concept in model_xbrl.qnameConcepts.values():
        namespace = concept.qname.namespaceURI or ""
        if namespace.startswith(_STRUCTURAL_PREFIX):
            continue
        qname = canonical(concept.qname)
        pres = presentation.get(qname) or [("", 0.0, "")]
        calc = calculation.get(qname) or [("", 0.0, "")]
        for pres_parent, pres_order, pres_role in pres:
            for calc_parent, calc_weight, calc_role in calc:
                concept_rows.append(
                    {
                        "taxonomy_version": taxonomy_version,
                        "concept_qname": qname,
                        "concept_namespace": namespace,
                        "concept_local_name": concept.qname.localName,
                        "substitution_group": canonical(
                            concept.substitutionGroupQname
                        ),
                        "is_abstract": bool(concept.isAbstract),
                        "item_type": (
                            str(concept.typeQname) if concept.typeQname else ""
                        ),
                        "balance": concept.balance or "",
                        "period_type": concept.periodType or "",
                        "presentation_parent": pres_parent,
                        "presentation_order": pres_order,
                        "presentation_role": pres_role,
                        "calculation_parent": calc_parent,
                        "calculation_weight": calc_weight,
                        "calculation_role": calc_role,
                        "loaded_at": loaded_at,
                    }
                )

    label_rows: list[dict] = []
    for rel in model_xbrl.relationshipSet(XbrlConst.conceptLabel).modelRelationships:
        concept = rel.fromModelObject
        label = rel.toModelObject
        if concept is None or label is None:
            continue
        namespace = concept.qname.namespaceURI or ""
        if namespace.startswith(_STRUCTURAL_PREFIX):
            continue
        label_rows.append(
            {
                "taxonomy_version": taxonomy_version,
                "concept_qname": canonical(concept.qname),
                "language": (label.xmlLang or "").lower(),
                "label_role": label.role or "",
                "label": label.stringValue or "",
                "loaded_at": loaded_at,
            }
        )
    return concept_rows, label_rows
