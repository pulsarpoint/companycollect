"""Freeze already-captured pages and source-read controls before any model calls."""

import json
import shutil
from pathlib import Path

from company_research.storage import content_hash, utc_now, write_json

import company_research
from page_agent_lab.agent import page_inventory


def read(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def prepare(root: Path, corpus: Path) -> dict:
    sources = [
        (
            "team_job",
            "handelsbanken-20260916-reasoning/high",
            "j0027",
            "https://www.handelsbanken.se/",
        ),
        (
            "data_job",
            "handelsbanken-20260916-reasoning/high",
            "j0006",
            "https://www.handelsbanken.se/",
        ),
        (
            "contacts",
            "handelsbanken-20260916-guided/sources",
            "g0010",
            "https://www.handelsbanken.se/",
        ),
        (
            "subsidiaries",
            "handelsbanken-20260916-guided/sources",
            "g0006",
            "https://www.handelsbanken.se/",
        ),
        (
            "careers",
            "handelsbanken-20260916-guided/sources",
            "g0003",
            "https://www.handelsbanken.se/",
        ),
        (
            "quality",
            "novelic-page-statements-v2-final",
            "p0005",
            "https://www.novelic.com/",
        ),
        (
            "report_hub",
            "handelsbanken-20260916-guided/sources",
            "g0012",
            "https://www.handelsbanken.se/",
        ),
    ]
    metadata = []
    for fixture_id, directory, pid, target_url in sources:
        source = corpus / directory
        pages = (
            read(source / "pages.json")
            if (source / "pages.json").exists()
            else read(source / "manifest.json")["pages"]
        )
        page = next(p for p in pages if p["page_id"] == pid).copy()
        html = (source / page["html_file"]).read_text(encoding="utf-8")
        if content_hash(html) != page["html_sha256"]:
            raise ValueError(f"Original snapshot changed: {fixture_id}")
        destination = root / "fixtures" / fixture_id
        destination.mkdir(parents=True, exist_ok=False)
        (destination / "page.html").write_text(html, encoding="utf-8")
        # Metadata from later model processing is not input evidence.
        page = {
            k: page[k]
            for k in [
                "page_id",
                "source_url",
                "requested_url",
                "fetched_at",
                "html_sha256",
                "job_detail",
            ]
            if k in page
        }
        page["page_id"] = fixture_id
        page["html_file"] = "page.html"
        rendered = source / "link-html" / f"{pid}.html"
        if not rendered.exists() and directory.endswith("/high"):
            rendered = (
                corpus / "handelsbanken-20260916-guided/jobs/link-html" / f"{pid}.html"
            )
        link_html = rendered.read_text(encoding="utf-8") if rendered.exists() else html
        kind = "rendered_html" if rendered.exists() else "native_cleaned_html"
        (destination / "link-page.html").write_text(link_html, encoding="utf-8")
        links, _ = page_inventory(link_html, page["source_url"], html_kind=kind)
        _, headings = page_inventory(
            html, page["source_url"], html_kind="native_cleaned_html"
        )
        page["link_html_sha256"] = content_hash(link_html)
        write_json(
            destination / "input.json",
            {
                "page": page,
                "headings": headings,
                "links": links,
                "target_url": target_url,
            },
        )
        metadata.append(
            {
                "fixture_id": fixture_id,
                "original_directory": str(source.resolve()),
                "original_page_id": pid,
                "html_sha256": page["html_sha256"],
                "html_chars": len(html),
                "link_html_sha256": page["link_html_sha256"],
                "link_inventory_source": kind,
                "link_count": len(links),
                "input_sha256": content_hash(
                    (destination / "input.json").read_text(encoding="utf-8")
                ),
            }
        )
    controls = control_set()
    write_json(root / "controls.json", controls)
    frozen_code = root / "implementation"
    frozen_code.mkdir()
    for source in Path(__file__).parent.glob("*.py"):
        shutil.copy2(source, frozen_code / source.name)
    shared = root / "shared_implementation"
    shared.mkdir()
    for source in Path(company_research.__file__).parent.glob("*.py"):
        shutil.copy2(source, shared / source.name)
    return {
        "prepared_at": utc_now(),
        "pages": metadata,
        "code_sha256": {
            str(path.relative_to(root)): content_hash(path.read_text(encoding="utf-8"))
            for directory in [frozen_code, shared]
            for path in sorted(directory.glob("*.py"))
        },
        "controls_sha256": content_hash(
            (root / "controls.json").read_text(encoding="utf-8")
        ),
        "method": "Frozen native cleaned HTML; rendered link inventory when available. No website fetches. Source-read controls are selected checks, not exhaustive ground truth.",
    }


def control_set() -> dict:
    positives = []

    def add(
        page: str,
        objective: str,
        name: str,
        identity: dict,
        qualifiers: dict | None = None,
    ):
        positives.append(
            {
                "id": f"{page}:{name}",
                "page": page,
                "objective": objective,
                "identity": identity,
                "qualifiers": qualifiers or {},
            }
        )

    for tech in ["JavaScript", "React", "TypeScript", "git", "npm", "Azure DevOps"]:
        add(
            "team_job",
            "technology_signals",
            tech,
            {"technology": [tech]},
            {"signal": ["required_experience"], "scope": ["role"]},
        )
    add(
        "team_job",
        "technology_signals",
        "Azure Pipelines",
        {"technology": ["Azure Pipelines"]},
        {"signal": ["advertised_expertise"], "scope": ["team"]},
    )
    add(
        "team_job",
        "jobs",
        "opening",
        {"title": ["Frontendutvecklare fokus infrastruktur"]},
    )
    add(
        "team_job",
        "company_contacts",
        "Olle email",
        {"value": ["olle.fagerlin@handelsbanken.se"]},
        {"owner": ["Olle Fagerlin"]},
    )
    add(
        "team_job",
        "company_contacts",
        "Olle phone",
        {"value": ["0732474522", "+46732474522"]},
        {"owner": ["Olle Fagerlin"]},
    )
    add("team_job", "people", "Olle", {"name": ["Olle Fagerlin"]})
    for tech, aliases in [
        ("OneLake", ["OneLake"]),
        ("Power BI", ["Power BI", "Microsoft Power BI"]),
        ("Data Factory", ["Data Factory", "Microsoft Fabric Data Factory"]),
    ]:
        add(
            "data_job",
            "technology_signals",
            tech,
            {"technology": aliases},
            {"signal": ["required_experience"], "scope": ["role"]},
        )
    for tech in ["Microsoft Fabric", "Azure Databricks"]:
        add(
            "data_job",
            "technology_signals",
            tech,
            {"technology": [tech]},
            {"signal": ["required_experience", "planned_adoption", "develops"]},
        )
    add(
        "data_job",
        "company_contacts",
        "Claudia",
        {"value": ["claudia.vag@handelsbanken.se"]},
        {"owner": ["Claudia Våg"]},
    )
    add("contacts", "company_profile", "registration", {"value": ["502007-7862"]})
    add("contacts", "company_profile", "LEI", {"value": ["NHBDILHZTYCNBV5UYZ31"]})
    for name, email in [
        ("Peter Grabe", "peter.grabe@handelsbanken.se"),
        ("Andreas Skogelid", "andreas.skogelid@handelsbanken.se"),
        ("Susanna Överby", "susanna.overby@handelsbanken.se"),
    ]:
        add(
            "contacts", "company_contacts", email, {"value": [email]}, {"owner": [name]}
        )
        add("contacts", "people", name, {"name": [name]})
    add("contacts", "company_contacts", "switchboard", {"value": ["+4687011000"]})
    add("contacts", "company_contacts", "press", {"value": ["press@handelsbanken.se"]})
    add(
        "contacts",
        "locations",
        "headquarters",
        {"address_contains": ["Kungsträdgårdsgatan 2"]},
        {"kind": ["headquarters"]},
    )
    for subsidiary in [
        "Handelsbanken Fonder AB",
        "Handelsbanken Liv Försäkringsaktiebolag",
        "Handelsbanken plc",
        "Stadshypotek AB",
        "EFN Ekonomikanalen AB",
    ]:
        # The ownership paragraph uses the short name below the legal-name heading.
        source_name = (
            "Handelsbanken Liv"
            if subsidiary == "Handelsbanken Liv Försäkringsaktiebolag"
            else subsidiary
        )
        add(
            "subsidiaries",
            "company_relationships",
            subsidiary,
            {"ownership_pair": [source_name]},
            {"ownership_direction": [source_name]},
        )
    add(
        "subsidiaries",
        "company_contacts",
        "UK phone",
        {"value": ["+442075788000"]},
        {"owner": ["Handelsbanken plc"]},
    )
    for name in ["ISO 9001", "ISO 14001", "IATF 16949"]:
        add(
            "quality",
            "certifications_compliance",
            name,
            {"standard_name_contains": [name]},
            {
                "claim_type": ["working_toward"]
                if name.startswith("IATF")
                else ["certification"]
            },
        )
    for title in [
        "SAS-utvecklare inom Financial Crime Prevention",
        "Frontendutvecklare fokus infrastruktur",
        "Data engineer/Data Warehouse Specialist inom Data Provisioning till Handelsbanken",
    ]:
        add("careers", "jobs", title, {"title": [title]})
    negatives = [
        {
            "id": "team_job:exclude generic names",
            "page": "team_job",
            "objective": "technology_signals",
            "forbidden": {
                "technology": [
                    "WCAG",
                    "HTML",
                    "CSS",
                    "CI/CD",
                    "VPN",
                    "JSON",
                    "XML",
                    "DORA",
                ]
            },
        },
        {
            "id": "quality:no held IATF",
            "page": "quality",
            "objective": "certifications_compliance",
            "forbidden": {
                "standard_name_contains": ["IATF 16949"],
                "claim_type": ["certification"],
            },
        },
        {
            "id": "team_job:no bank map stack",
            "page": "team_job",
            "objective": "technology_signals",
            "forbidden": {
                "technology_contains": ["MapTiler", "OpenStreetMap"],
                "company_contains": ["Handelsbanken"],
            },
        },
        {
            "id": "data_job:no bank map stack",
            "page": "data_job",
            "objective": "technology_signals",
            "forbidden": {
                "technology_contains": ["MapTiler", "OpenStreetMap"],
                "company_contains": ["Handelsbanken"],
            },
        },
    ]
    # Source-reviewed failures from the pilot; only future runs use these controls.
    for page, objective, label, forbidden in [
        (
            "report_hub",
            "technology_signals",
            "no DOM technology",
            {"technology": ["vp292.alertir.com", "shb-iframe", "Linkedin"]},
        ),
        (
            "careers",
            "locations",
            "markets are not addresses",
            {
                "address": [
                    "Sverige",
                    "Nederländerna",
                    "Storbritannien",
                    "Norge",
                    "Luxemburg",
                    "USA",
                ]
            },
        ),
        (
            "contacts",
            "company_profile",
            "people are not companies",
            {
                "company": [
                    "Peter Grabe",
                    "Andreas Skogelid",
                    "Lars Kenneth Dahlqvist",
                    "Susanna Överby",
                ]
            },
        ),
        (
            "report_hub",
            "company_relationships",
            "markets are not counterparties",
            {
                "object_contains": [
                    "Sverige",
                    "Norge",
                    "Nederländerna",
                    "Storbritannien",
                ]
            },
        ),
        (
            "data_job",
            "technology_signals",
            "capture date is not claim date",
            {"as_of": ["2026-09-16"]},
        ),
        (
            "data_job",
            "technology_signals",
            "target architecture is not deployment",
            {
                "technology": ["Microsoft Fabric", "Azure Databricks"],
                "signal": ["stated_use"],
            },
        ),
        (
            "quality",
            "technology_signals",
            "AURIX service is not product sales",
            {"technology": ["AURIX"], "signal": ["offers"]},
        ),
        (
            "report_hub",
            "products_services",
            "report ordering is not a service",
            {"name_contains": ["Beställ"]},
        ),
        (
            "quality",
            "document_links",
            "purchase conditions are not reports",
            {"document_url_contains": ["GeneralPurchaseConditions"]},
        ),
        (
            "report_hub",
            "document_links",
            "order forms are not documents",
            {"document_url_contains": ["/bestalla-arsredovisning", "/bestall-solvens"]},
        ),
        (
            "contacts",
            "document_links",
            "archive hub is not a document",
            {
                "document_url": [
                    "https://www.handelsbanken.com/sv/investor-relations/rapporter-och-presentationer"
                ]
            },
        ),
    ]:
        negatives.append(
            {
                "id": f"{page}:{label}",
                "page": page,
                "objective": objective,
                "forbidden": forbidden,
            }
        )
    for page in ["data_job", "team_job"]:
        negatives.append(
            {
                "id": f"{page}:hosting is not an explicit supplier claim",
                "page": page,
                "objective": "company_relationships",
                "forbidden": {"subject": ["Jobylon"], "relationship": ["supplier_of"]},
            }
        )
    for page in ["team_job", "subsidiaries"]:
        negatives.append(
            {
                "id": f"{page}:relationship capture date is not claim date",
                "page": page,
                "objective": "company_relationships",
                "forbidden": {"as_of": ["2026-09-16"]},
            }
        )
    return {
        "version": "page-agent-controls/0.2",
        "positive": positives,
        "negative": negatives,
        "link_controls": [
            {
                "page": "careers",
                "label": "next_page",
                "kind": "control",
                "anchor": "Nästa",
            },
            {
                "page": "careers",
                "label": "job_detail",
                "url_contains": "emp.jobylon.com/jobs/",
            },
            {
                "page": "report_hub",
                "label": "embedded_archive",
                "kind": "iframe",
                "url_contains": "vp292.alertir.com",
            },
            {"page": "quality", "label": "careers", "url_contains": "/careers"},
        ],
    }
