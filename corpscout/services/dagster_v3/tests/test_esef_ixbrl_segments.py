import json
import zipfile
from dataclasses import replace
from datetime import timedelta
from hashlib import sha256
from pathlib import Path

import pytest
from dagster import AssetKey

from dagster_v3.defs.common.duckdb_resources import duckdb_resource
from dagster_v3.defs.esef_filings import assets
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_ARELLE_POOL,
    ESEF_DOCUMENT_BUCKET,
    ESEF_PROCESSED_WEEK_PARTITIONS,
    document_manifest_object_key,
    document_result_object_key,
    esef_document_artifacts_s3,
    esef_document_extraction_duckdb,
    esef_document_extraction_manifest_s3,
    load_esef_document_result,
    report_package_object_key,
    run_esef_document_artifacts_partition,
    run_esef_document_extraction_partition,
)
from dagster_v3.defs.esef_filings.segment_cli import main as segment_cli_main
from dagster_v3.defs.esef_filings.fact_parity import (
    build_fact_parity_report,
    iter_artifact_legacy_facts,
)
from dagster_v3.defs.esef_filings.contact_candidates import (
    TaggedContactValue,
    extract_contact_candidates,
)
from dagster_v3.defs.esef_filings.segment_parser import (
    EsefArtifactSource,
    EsefSegmentArtifact,
    artifact_json_bytes,
    artifact_object_key,
    compare_artifact_to_oim,
    parse_esef_report_package,
)
from dagster_v3.defs.esef_filings.visible_sections import extract_visible_sections
from dagster_v3.defs.esef_filings.website_candidates import (
    TaggedWebsiteValue,
    extract_website_candidates,
    registrable_domain_for_host,
)


def test_parse_report_package_resolves_continuations_and_selects_segments(
    tmp_path: Path,
) -> None:
    package_path = _write_sample_report_package(tmp_path)

    artifact = parse_esef_report_package(
        package_path,
        source=EsefArtifactSource(
            fxo_id="SAMPLE-2024",
            country="SE",
            source_url="https://example.test/sample.zip",
            object_key="raw/sample.zip",
        ),
        validate_esef=False,
    )

    facts_by_concept = {fact.concept_qname: fact for fact in artifact.facts.values()}
    description = facts_by_concept[
        "sample:DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities"
    ]
    revenue = facts_by_concept["sample:Revenue"]

    assert description.canonical_value == "Makes better oils."
    assert description.language == "en"
    assert revenue.canonical_value == "1234500.0"
    assert revenue.unit == {
        "numerators": ["iso4217:EUR"],
        "denominators": [],
    }
    assert artifact.document.entities == [
        {
            "scheme": "http://standards.iso.org/iso/17442",
            "identifier": "549300SAMPLE000000001",
        }
    ]
    assert artifact.document.languages == ["en", "sv"]
    assert artifact.schema_version == 5
    assert artifact.parser.candidate_extractor_versions.keys() == {
        "corpscout-contact-candidates",
        "corpscout-visible-sections",
        "email-validator",
        "lxml",
        "phonenumbers",
        "tldextract",
    }
    assert artifact.quality.fact_count == 4
    revenue_note = facts_by_concept["xbrl:note"]
    assert revenue_note.canonical_value == "Revenue footnote."
    assert revenue.links[0].target_fact_key == revenue_note.fact_key
    assert (
        artifact.segments["identity"][0].fact_key
        == facts_by_concept[
            "sample:NameOfReportingEntityOrOtherMeansOfIdentification"
        ].fact_key
    )
    assert artifact.segments["business_profile"][0].fact_key == description.fact_key
    assert artifact.segments["financial_highlights"][0].fact_key == revenue.fact_key
    assert artifact.concepts[description.concept_qname].labels == {
        "en": "Description of operations",
        "sv": "Beskrivning av verksamheten",
    }
    contacts = {
        (candidate.kind, candidate.normalized_value): candidate
        for candidate in artifact.contact_candidates
    }
    assert set(contacts) == {
        ("email", "audit@audit-firm.se"),
        ("email", "investor.relations@sample-oils.se"),
        ("phone", "+46401234567"),
    }
    assert contacts[("email", "investor.relations@sample-oils.se")].suggested_roles == [
        "investor_relations"
    ]
    assert contacts[("email", "audit@audit-firm.se")].suggested_roles == ["auditor"]
    assert contacts[("phone", "+46401234567")].suggested_roles == ["general"]
    websites = {
        candidate.registrable_domain: candidate
        for candidate in artifact.website_candidates
    }
    assert set(websites) == {
        "audit-firm.se",
        "linkedin.com",
        "sample-oils.se",
    }
    assert websites["sample-oils.se"].hosts == [
        "investors.sample-oils.se",
        "www.sample-oils.se",
    ]
    assert websites["sample-oils.se"].suggested_roles == [
        "company_website",
        "investor_relations",
    ]
    board_section = next(
        section
        for section in artifact.visible_sections
        if section.section_type == "board_composition"
    )
    assert board_section.heading == "Board of Directors"
    assert "Anna Andersson — Chair" in board_section.text
    assert board_section.extraction_method == "semantic_section"


def test_visible_sections_reconstruct_positioned_page_order_and_provenance(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "positioned.xhtml"
    report_path.write_text(_POSITIONED_VISIBLE_SECTIONS_XHTML, encoding="utf-8")

    sections = extract_visible_sections({"reports/positioned.xhtml": report_path})

    management = next(
        section
        for section in sections
        if section.section_type == "executive_management"
    )
    assert management.page_id == "pf_management"
    assert management.printed_page_number == "72"
    assert management.heading == "FÖRETAGSLEDNING"
    assert management.extraction_method == "positioned_page"
    assert management.language == "sv"
    assert management.text.index("FÖRETAGSLEDNING") < management.text.index(
        "BJÖRN GARAT"
    )
    assert management.text.index("BJÖRN GARAT") < management.text.index(
        "Finanschef och vice VD sedan 2012."
    )
    assert "Ignore Hidden Person" not in management.text
    assert management.included_character_count == len(management.text)
    assert management.text_sha256 == sha256(management.text.encode()).hexdigest()
    assert {section.section_type for section in sections} >= {
        "executive_management",
        "person_profiles",
        "company_contact",
    }


def test_visible_sections_stop_semantic_excerpt_at_next_heading(tmp_path: Path) -> None:
    report_path = tmp_path / "semantic.xhtml"
    report_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en"><body>
  <h2>Board of Directors</h2>
  <p>Anna Andersson — Chair and board member.</p>
  <p>Bo Berg — Board member.</p>
  <h2>Revenue note</h2>
  <p>Revenue was SEK 10 million.</p>
</body></html>
""",
        encoding="utf-8",
    )

    sections = extract_visible_sections({"reports/semantic.xhtml": report_path})

    board = next(
        section for section in sections if section.section_type == "board_composition"
    )
    assert board.heading == "Board of Directors"
    assert "Anna Andersson" in board.text
    assert "Revenue was" not in board.text
    assert board.extraction_method == "semantic_section"


def test_website_candidates_are_normalized_deduplicated_and_auditable(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(_WEBSITE_REPORT_XHTML, encoding="utf-8")

    candidates = extract_website_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[
            TaggedWebsiteValue(
                report_member="reports/report.xhtml",
                concept_local_name="WebsitesOfLegalEntity",
                value="www.sample-oils.se/contact?utm_source=esef#office",
            ),
            TaggedWebsiteValue(
                report_member="reports/report.xhtml",
                concept_local_name="Revenue",
                value="https://financial-noise.example.se/",
            ),
        ],
        known_email_domains=["sample-oils.se"],
    )

    by_domain = {candidate.registrable_domain: candidate for candidate in candidates}
    assert set(by_domain) == {
        "audit-firm.se",
        "corporate-governanceboard.se",
        "linkedin.com",
        "sample-oils.se",
    }

    company = by_domain["sample-oils.se"]
    assert company.hosts == [
        "investors.sample-oils.se",
        "www.sample-oils.se",
    ]
    assert company.normalized_urls == [
        "https://investors.sample-oils.se/reports/",
        "https://www.sample-oils.se/",
        "https://www.sample-oils.se/contact",
    ]
    assert company.suggested_roles == [
        "company_website",
        "investor_relations",
    ]
    assert {evidence.extraction_method for evidence in company.evidence} == {
        "tagged_fact",
        "visible_link",
        "visible_text",
    }
    assert all(
        evidence.report_member == "reports/report.xhtml"
        for evidence in company.evidence
    )
    assert all(
        evidence.xpath != ""
        for evidence in company.evidence
        if evidence.extraction_method != "tagged_fact"
    )

    assert by_domain["audit-firm.se"].suggested_roles == ["auditor"]
    assert by_domain["linkedin.com"].suggested_roles == ["social_media"]
    assert {
        evidence.extraction_method
        for evidence in by_domain["corporate-governanceboard.se"].evidence
    } == {"visible_text_reconstructed"}
    assert "example.se" not in by_domain
    assert "xbrl.org" not in by_domain
    assert "do-not-use.se" not in by_domain
    assert "design-vendor.se" not in by_domain
    assert "i.property" not in by_domain
    assert "m.in" not in by_domain


def test_report_production_credits_are_not_company_contacts(tmp_path: Path) -> None:
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(_REPORT_PRODUCTION_CREDITS_XHTML, encoding="utf-8")

    contacts = extract_contact_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[],
        default_region="SE",
    )
    websites = extract_website_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[],
        known_email_domains=(
            candidate.normalized_value.rpartition("@")[2]
            for candidate in contacts
            if candidate.kind == "email"
        ),
    )

    assert {(candidate.kind, candidate.normalized_value) for candidate in contacts} == {
        ("email", "info@sample-oils.se"),
        ("phone", "+46401234567"),
    }
    assert {candidate.registrable_domain for candidate in websites} == {
        "sample-oils.se"
    }


def test_registrable_domain_validator_requires_a_public_suffix() -> None:
    assert registrable_domain_for_host("investors.example.co.uk") == "example.co.uk"
    assert registrable_domain_for_host("RÄKSMÖRGÅS.SE") == "xn--rksmrgs-5wao1o.se"
    assert registrable_domain_for_host("localhost") is None
    assert registrable_domain_for_host("10.0.0.1") is None
    assert registrable_domain_for_host("company.not-a-real-tld") is None
    assert registrable_domain_for_host("filing.zip") is None
    assert registrable_domain_for_host("invalid-.se") is None


def test_contact_candidates_are_normalized_deduplicated_and_auditable(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(_CONTACT_REPORT_XHTML, encoding="utf-8")

    candidates = extract_contact_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[
            TaggedContactValue(
                report_member="reports/report.xhtml",
                concept_local_name="EMailAddress",
                value="accounts@sample-oils.se",
            ),
            TaggedContactValue(
                report_member="reports/report.xhtml",
                concept_local_name="Revenue",
                value="46401234567",
            ),
        ],
        default_region="SE",
    )

    by_value = {candidate.normalized_value: candidate for candidate in candidates}
    assert set(by_value) == {
        "+46401234567",
        "accounts@sample-oils.se",
        "audit@audit-firm.se",
        "investor.relations@sample-oils.se",
    }
    investor_relations = by_value["investor.relations@sample-oils.se"]
    assert investor_relations.kind == "email"
    assert investor_relations.country_code == ""
    assert investor_relations.suggested_roles == ["investor_relations"]
    assert {evidence.extraction_method for evidence in investor_relations.evidence} == {
        "mailto",
        "visible_text",
    }
    assert all(
        evidence.report_member == "reports/report.xhtml"
        for evidence in investor_relations.evidence
    )
    assert all(evidence.xpath != "" for evidence in investor_relations.evidence)

    phone = by_value["+46401234567"]
    assert phone.country_code == "SE"
    assert phone.suggested_roles == ["general"]
    assert {evidence.extraction_method for evidence in phone.evidence} == {
        "tel",
        "visible_text",
    }

    tagged = by_value["accounts@sample-oils.se"]
    assert tagged.evidence[0].extraction_method == "tagged_fact"
    assert tagged.evidence[0].source_concept == "EMailAddress"
    assert "hidden@do-not-use.se" not in by_value
    assert "+46409999999" not in by_value


def test_contact_candidates_normalize_local_french_phone_numbers(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body>
  <p>Tél. : 01 43 23 04 31</p>
  <p>N° d’appel gratuit : 0 800 000 777</p>
</body></html>
""",
        encoding="utf-8",
    )

    candidates = extract_contact_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[],
        default_region="FR",
    )

    assert {candidate.normalized_value for candidate in candidates} == {
        "+33143230431",
        "+33800000777",
    }
    assert all(candidate.suggested_roles == ["general"] for candidate in candidates)


def test_contact_candidates_join_text_split_by_empty_layout_spans(
    tmp_path: Path,
) -> None:
    report_path = tmp_path / "report.xhtml"
    report_path.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"><body>
  <div>Investor Relations:</div>
  <div>Johan Bartler +<span/>46 739 02 21 9<span/>3</div>
  <div>E-mail: inves<span/>torrela<span/>tions@vo<span/>lvo.com</div>
  <div>Head office:</div>
  <div>T<span/>el +4<span/>6 31 66 0<span/>0 00</div>
</body></html>
""",
        encoding="utf-8",
    )

    candidates = extract_contact_candidates(
        {"reports/report.xhtml": report_path},
        tagged_values=[],
        default_region="SE",
    )
    by_value = {candidate.normalized_value: candidate for candidate in candidates}

    assert set(by_value) == {
        "+4631660000",
        "+46739022193",
        "investorrelations@volvo.com",
    }
    assert by_value["+46739022193"].suggested_roles == ["investor_relations"]
    assert by_value["investorrelations@volvo.com"].suggested_roles == [
        "investor_relations"
    ]
    assert by_value["+4631660000"].suggested_roles == ["general"]


def test_artifact_json_is_deterministic(tmp_path: Path) -> None:
    package_path = _write_sample_report_package(tmp_path)
    source = EsefArtifactSource(fxo_id="SAMPLE-2024")

    first = parse_esef_report_package(
        package_path,
        source=source,
        validate_esef=False,
    )
    second = parse_esef_report_package(
        package_path,
        source=source,
        validate_esef=False,
    )

    assert artifact_json_bytes(first) == artifact_json_bytes(second)


def test_esef_validation_loads_the_report_package_entrypoint(tmp_path: Path) -> None:
    artifact = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=True,
    )

    assert artifact.quality.fact_count == 4
    assert artifact.parser.validation_profile == "ESEF"
    assert artifact.document.report_members == ["sample/reports/sample.xhtml"]


def test_parse_report_package_rejects_unsafe_zip_member(tmp_path: Path) -> None:
    package_path = tmp_path / "unsafe.zip"
    with zipfile.ZipFile(package_path, "w") as package:
        package.writestr("../outside.xhtml", "<html />")

    with pytest.raises(ValueError, match="unsafe path"):
        parse_esef_report_package(
            package_path,
            source=EsefArtifactSource(fxo_id="UNSAFE"),
            validate_esef=False,
        )


def test_artifact_object_key_is_content_and_parser_versioned() -> None:
    assert artifact_object_key("a" * 64) == (
        "esef_filings/ixbrl_segments/schema=v5/parser=arelle-2.43.1/"
        "candidates=v3/"
        f"package_sha256={'a' * 64}/artifact.json"
    )


def test_compare_artifact_to_oim_reports_exact_fact_agreement(tmp_path: Path) -> None:
    artifact = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=False,
    )
    reference = {
        "facts": {
            fact.source_fact_id: {
                "value": (
                    "1234500"
                    if fact.source_fact_id == "revenue"
                    else fact.canonical_value
                ),
                "decimals": fact.decimals,
                "dimensions": {
                    **fact.oim_dimensions,
                    **(
                        {"noteId": "reference-generated-note-id"}
                        if fact.concept_qname == "xbrl:note"
                        else {}
                    ),
                },
            }
            for fact in artifact.facts.values()
        }
    }

    result = compare_artifact_to_oim(artifact, reference)

    assert result == {
        "artifact_fact_count": 4,
        "reference_fact_count": 4,
        "matched_fact_count": 4,
        "artifact_only_count": 0,
        "reference_only_count": 0,
    }


def test_fact_parity_report_checks_semantic_and_legacy_row_contracts(
    tmp_path: Path,
) -> None:
    artifact = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=False,
    )
    reference = _reference_oim_from_artifact(artifact)

    report = build_fact_parity_report(
        artifact,
        reference,
        lei="549300SAMPLE000000001",
        fxo_id="SAMPLE-2024",
        period_end="2024-12-31",
    )

    assert report["semantic_facts"] == {
        "artifact_fact_count": 4,
        "reference_fact_count": 4,
        "matched_fact_count": 4,
        "artifact_only_count": 0,
        "reference_only_count": 0,
        "parity": True,
    }
    assert report["legacy_rows"]["parity"] is True
    assert report["legacy_rows"]["exact_row_parity"] is True
    assert report["metrics"]["parity"] is True
    assert report["ready_for_fact_cutover"] is True
    assert set(report["field_mismatch_counts"]) == {
        "concept",
        "value",
        "decimals",
        "entity",
        "period",
        "unit",
        "language",
        "dimensions",
    }
    assert not any(report["field_mismatch_counts"].values())


def test_fact_parity_report_resolves_equivalent_qname_prefixes(
    tmp_path: Path,
) -> None:
    artifact = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=False,
    )
    reference = _reference_oim_from_artifact(artifact)
    namespaces = reference["documentInfo"]["namespaces"]
    namespaces["alternate"] = namespaces.pop("sample")
    namespaces["money"] = namespaces.pop("iso4217")
    namespaces["lei"] = namespaces.pop("scheme")
    for entry in reference["facts"].values():
        dimensions = entry["dimensions"]
        if dimensions["concept"].startswith("sample:"):
            dimensions["concept"] = dimensions["concept"].replace(
                "sample:", "alternate:", 1
            )
        if dimensions.get("unit", "").startswith("iso4217:"):
            dimensions["unit"] = dimensions["unit"].replace("iso4217:", "money:", 1)
        if dimensions.get("entity", "").startswith("scheme:"):
            dimensions["entity"] = dimensions["entity"].replace("scheme:", "lei:", 1)

    report = build_fact_parity_report(
        artifact,
        reference,
        lei="549300SAMPLE000000001",
        fxo_id="SAMPLE-2024",
        period_end="2024-12-31",
    )

    assert report["semantic_facts"]["parity"] is True
    assert report["legacy_rows"]["parity"] is False
    assert report["ready_for_fact_cutover"] is False


def test_fact_parity_report_identifies_value_decimal_and_metric_changes(
    tmp_path: Path,
) -> None:
    parsed = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=False,
    )
    revenue_fact_key = next(
        fact_key
        for fact_key, fact in parsed.facts.items()
        if fact.concept_qname == "sample:Revenue"
    )
    sample_revenue = parsed.facts[revenue_fact_key]
    ifrs_namespace = "https://xbrl.ifrs.org/taxonomy/2024-03-27/ifrs-full"
    artifact = replace(
        parsed,
        document=replace(
            parsed.document,
            namespaces={**parsed.document.namespaces, "ifrs-full": ifrs_namespace},
        ),
        concepts={
            **{
                qname: concept
                for qname, concept in parsed.concepts.items()
                if qname != "sample:Revenue"
            },
            "ifrs-full:Revenue": replace(
                parsed.concepts["sample:Revenue"],
                qname="ifrs-full:Revenue",
                namespace=ifrs_namespace,
                is_extension=False,
            ),
        },
        facts={
            **parsed.facts,
            revenue_fact_key: replace(
                sample_revenue,
                concept_qname="ifrs-full:Revenue",
                oim_dimensions={
                    **sample_revenue.oim_dimensions,
                    "concept": "ifrs-full:Revenue",
                },
            ),
        },
    )
    reference = _reference_oim_from_artifact(artifact)
    reference_revenue = reference["facts"][sample_revenue.source_fact_id]
    reference_revenue["value"] = "9999999"
    reference_revenue["decimals"] = -2

    report = build_fact_parity_report(
        artifact,
        reference,
        lei="549300SAMPLE000000001",
        fxo_id="SAMPLE-2024",
        period_end="2024-12-31",
    )

    assert report["semantic_facts"]["parity"] is False
    assert report["field_mismatch_counts"]["value"] == 1
    assert report["field_mismatch_counts"]["decimals"] == 1
    assert report["metrics"]["parity"] is False
    assert report["metrics"]["mismatched_metrics"] == ["revenue"]
    assert report["ready_for_fact_cutover"] is False


def test_artifact_legacy_fact_ids_remain_unique_when_source_ids_repeat(
    tmp_path: Path,
) -> None:
    parsed = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=False,
    )
    first_key, second_key = list(parsed.facts)[:2]
    artifact = replace(
        parsed,
        facts={
            **parsed.facts,
            first_key: replace(parsed.facts[first_key], source_fact_id="repeated"),
            second_key: replace(parsed.facts[second_key], source_fact_id="repeated"),
        },
    )

    rows = list(
        iter_artifact_legacy_facts(
            artifact,
            lei="549300SAMPLE000000001",
            fxo_id="SAMPLE-2024",
            period_end="2024-12-31",
        )
    )

    assert len(rows) == 4
    assert len({row.fact_id for row in rows}) == 4
    assert "repeated" in {row.fact_id for row in rows}
    assert any(row.fact_id.startswith("repeated#") for row in rows)


def test_fact_parity_allows_deterministic_fact_id_changes_when_rows_still_match(
    tmp_path: Path,
) -> None:
    artifact = parse_esef_report_package(
        _write_sample_report_package(tmp_path),
        source=EsefArtifactSource(fxo_id="SAMPLE-2024"),
        validate_esef=False,
    )
    reference = _reference_oim_from_artifact(artifact)
    reference["facts"] = {
        f"generated-{index}": entry
        for index, entry in enumerate(reference["facts"].values(), start=1)
    }

    report = build_fact_parity_report(
        artifact,
        reference,
        lei="549300SAMPLE000000001",
        fxo_id="SAMPLE-2024",
        period_end="2024-12-31",
    )

    assert report["semantic_facts"]["parity"] is True
    assert report["legacy_rows"]["parity"] is True
    assert report["legacy_rows"]["exact_row_parity"] is False
    assert report["legacy_rows"]["fact_id_inventory_match"] is False
    assert report["ready_for_fact_cutover"] is True


def _reference_oim_from_artifact(
    artifact: EsefSegmentArtifact,
) -> dict[str, object]:
    return {
        "documentInfo": {
            "namespaces": dict(artifact.document.namespaces),
        },
        "facts": {
            fact.source_fact_id: {
                "value": fact.canonical_value,
                "decimals": fact.decimals,
                "dimensions": dict(fact.oim_dimensions),
            }
            for fact in artifact.facts.values()
        },
    }


def test_cli_writes_reviewable_artifact(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    package_path = _write_sample_report_package(tmp_path)
    output_path = tmp_path / "output" / "artifact.json"

    exit_code = segment_cli_main(
        [
            str(package_path),
            str(output_path),
            "--fxo-id",
            "SAMPLE-2024",
            "--company-id",
            "556600-0000",
            "--country",
            "SE",
            "--skip-esef-validation",
        ]
    )

    assert exit_code == 0
    output = json.loads(output_path.read_text(encoding="utf-8"))
    assert output["source"]["fxo_id"] == "SAMPLE-2024"
    assert output["source"]["company_id"] == "556600-0000"
    assert output["source"]["country"] == "SE"
    assert output["quality"]["fact_count"] == 4
    summary = json.loads(capsys.readouterr().out)
    assert summary["fact_count"] == 4
    assert summary["contact_candidate_counts"] == {"email": 2, "phone": 1}
    assert summary["website_domain_candidate_count"] == 3
    assert summary["segment_fact_counts"]["business_profile"] == 1


def test_cli_emits_detailed_fact_parity_report(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    package_path = _write_sample_report_package(tmp_path)
    artifact = parse_esef_report_package(
        package_path,
        source=EsefArtifactSource(fxo_id="SAMPLE-2024-12-31-ESEF-SE-0"),
        validate_esef=False,
    )
    reference_path = tmp_path / "reference.json"
    reference_path.write_text(
        json.dumps(_reference_oim_from_artifact(artifact)),
        encoding="utf-8",
    )

    exit_code = segment_cli_main(
        [
            str(package_path),
            str(tmp_path / "artifact.json"),
            "--fxo-id",
            "SAMPLE-2024-12-31-ESEF-SE-0",
            "--lei",
            "549300SAMPLE000000001",
            "--reference-oim",
            str(reference_path),
            "--skip-esef-validation",
        ]
    )

    assert exit_code == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["fact_parity"]["ready_for_fact_cutover"] is True
    assert summary["fact_parity"]["semantic_facts"]["matched_fact_count"] == 4


def test_document_asset_archives_parses_and_stores_source_linked_rows(
    tmp_path: Path,
) -> None:
    package_body = _write_sample_report_package(tmp_path).read_bytes()
    package_sha256 = sha256(package_body).hexdigest()
    database = duckdb_resource(tmp_path / "esef.duckdb")
    _seed_filing_index(database, package_sha256=package_sha256)
    object_store = _FakeObjectStore({})
    client = _FakeDownloadClient(package_body)

    first = run_esef_document_extraction_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        client=client,
        company_links={"549300SAMPLE000000001": ("SE", "5566000000")},
        partition_key="2025-03-30",
        source_run_id="parse-run",
        source_document_ids=["SAMPLE-2024"],
        max_documents=None,
        refresh_existing=False,
        validate_esef=False,
        log_info=lambda *_args: None,
    )
    second = run_esef_document_extraction_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        client=client,
        company_links={"549300SAMPLE000000001": ("SE", "5566000000")},
        partition_key="2025-03-30",
        source_run_id="parse-run-2",
        source_document_ids=["SAMPLE-2024"],
        max_documents=None,
        refresh_existing=False,
        validate_esef=False,
        log_info=lambda *_args: None,
    )

    assert first["parsed_package_count"] == 1
    assert first["downloaded_package_count"] == 1
    assert first["email_candidate_count"] == 2
    assert first["phone_candidate_count"] == 1
    assert first["website_candidate_count"] == 3
    assert first["concept_label_row_count"] == 2
    assert second["reused_artifact_count"] == 1
    assert second["reused_package_count"] == 1
    assert client.calls == ["https://example.test/sample.zip"]
    artifact_key = artifact_object_key(package_sha256)
    artifact = json.loads(object_store.objects[(ESEF_DOCUMENT_BUCKET, artifact_key)])
    assert artifact["source"]["company_id"] == "5566000000"
    assert artifact["source"]["source_run_id"] == "parse-run"
    assert (
        ESEF_DOCUMENT_BUCKET,
        report_package_object_key(package_sha256),
    ) in object_store.objects

    with database.get_connection() as connection:
        document = connection.execute(
            f"select source_document_id, country_iso2, company_id, fact_count, "
            f"parsed_artifact_object_key from {tables.DLT_DATASET_NAME}."
            f"{tables.ESEF_SOURCE_DOCUMENTS_TABLE}"
        ).fetchone()
        candidates = connection.execute(
            f"select candidate_kind, normalized_value, evidence_json from "
            f"{tables.DLT_DATASET_NAME}."
            f"{tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE} "
            "order by candidate_kind, normalized_value"
        ).fetchall()
        concept_labels = connection.execute(
            f"select concept_qname, label_role, language, label, "
            f"is_report_language from {tables.DLT_DATASET_NAME}."
            f"{tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE} "
            "order by concept_qname, label_role, language"
        ).fetchall()
    assert document == (
        "SAMPLE-2024",
        "SE",
        "5566000000",
        4,
        artifact_key,
    )
    assert {kind for kind, _value, _evidence in candidates} == {
        "email",
        "phone",
        "website",
    }
    assert all(json.loads(evidence) for _kind, _value, evidence in candidates)
    assert concept_labels == [
        (
            "sample:DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities",
            "standard",
            "en",
            "Description of operations",
            True,
        ),
        (
            "sample:DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities",
            "standard",
            "sv",
            "Beskrivning av verksamheten",
            True,
        ),
    ]


def test_document_artifact_stage_accepts_schema_two_manifest() -> None:
    partition_key = "2025-03-30"
    object_store = _FakeObjectStore(
        {
            (
                ESEF_DOCUMENT_BUCKET,
                document_manifest_object_key(partition_key),
            ): json.dumps(
                {
                    "schema_version": 2,
                    "processed_week": partition_key,
                    "documents": [],
                }
            ).encode("utf-8")
        }
    )

    metadata = run_esef_document_artifacts_partition(
        object_store=object_store,
        client=_FakeDownloadClient(b""),
        partition_key=partition_key,
        source_run_id="compatibility-run",
        refresh_existing=False,
        validate_esef=False,
        parse_workers=1,
        log_info=lambda *_args: None,
    )

    result = json.loads(
        object_store.objects[
            (ESEF_DOCUMENT_BUCKET, document_result_object_key(partition_key))
        ]
    )
    assert metadata["selected_document_count"] == 0
    assert result["schema_version"] == 3
    assert result["concept_label_rows"] == []


def test_document_result_rejects_schema_two() -> None:
    partition_key = "2025-03-30"
    object_store = _FakeObjectStore(
        {
            (
                ESEF_DOCUMENT_BUCKET,
                document_result_object_key(partition_key),
            ): json.dumps(
                {
                    "schema_version": 2,
                    "processed_week": partition_key,
                }
            ).encode("utf-8")
        }
    )

    with pytest.raises(
        ValueError,
        match=r"result.*expected=\[3\] actual=2",
    ):
        load_esef_document_result(object_store, partition_key=partition_key)


def test_facts_cutover_reads_arelle_artifact_and_replaces_legacy_checkpoint(
    tmp_path: Path,
) -> None:
    package_body = _write_sample_report_package(tmp_path).read_bytes()
    package_sha256 = sha256(package_body).hexdigest()
    database = duckdb_resource(tmp_path / "esef.duckdb")
    _seed_filing_index(database, package_sha256=package_sha256)
    object_store = _FakeObjectStore({})

    run_esef_document_extraction_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        client=_FakeDownloadClient(package_body),
        company_links={"549300SAMPLE000000001": ("SE", "5566000000")},
        partition_key="2025-03-30",
        source_run_id="artifact-run",
        source_document_ids=["SAMPLE-2024"],
        max_documents=None,
        refresh_existing=False,
        validate_esef=False,
        log_info=lambda *_args: None,
    )
    with database.get_connection() as connection:
        connection.execute(
            f"create table {assets.QUALIFIED_FACTS_INGESTION_STATE_TABLE} ("
            "fxo_id varchar, fact_count bigint, source_run_id varchar, "
            "completed_at timestamp)"
        )
        connection.execute(
            f"insert into {assets.QUALIFIED_FACTS_INGESTION_STATE_TABLE} values "
            "('SAMPLE-2024', 999, 'legacy-oim-run', current_timestamp)"
        )

    first = assets.run_esef_artifact_facts_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        partition_key="2025-03-30",
        source_run_id="facts-run",
        log_info=lambda *_args: None,
        log_warning=lambda *_args: None,
    )
    second = assets.run_esef_artifact_facts_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        partition_key="2025-03-30",
        source_run_id="facts-run-2",
        log_info=lambda *_args: None,
        log_warning=lambda *_args: None,
    )

    assert first["inserted_fact_row_count"] == 4
    assert first["checkpointed_filing_count"] == 0
    assert first["unique_artifact_s3_read_count"] == 1
    assert second["inserted_fact_row_count"] == 0
    assert second["checkpointed_filing_count"] == 1
    assert second["unique_artifact_s3_read_count"] == 0
    with database.get_connection() as connection:
        facts_rows = connection.execute(
            f"select fact_id, concept_qname, source_run_id from "
            f"{tables.QUALIFIED_FACTS_TABLE} order by fact_id"
        ).fetchall()
        checkpoint = connection.execute(
            f"select fact_count, source_run_id, parser_contract from "
            f"{assets.QUALIFIED_FACTS_INGESTION_STATE_TABLE} "
            "where fxo_id = 'SAMPLE-2024'"
        ).fetchone()
    assert len(facts_rows) == 4
    assert {row[2] for row in facts_rows} == {"facts-run"}
    assert checkpoint == (
        4,
        "facts-run",
        assets.ARELLE_ARTIFACT_FACTS_PARSER_CONTRACT,
    )


def test_document_asset_parses_identical_packages_once_per_partition(
    tmp_path: Path,
) -> None:
    package_body = _write_sample_report_package(tmp_path).read_bytes()
    package_sha256 = sha256(package_body).hexdigest()
    database = duckdb_resource(tmp_path / "esef.duckdb")
    _seed_filing_index(database, package_sha256=package_sha256)
    with database.get_connection() as connection:
        connection.execute(
            f"insert into {tables.QUALIFIED_FILINGS_INDEX_TABLE} values "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "SAMPLE-2024-COPY",
                "549300SAMPLE000000001",
                "Sample AB",
                "SE",
                "2024-12-31",
                "https://mirror.example.test/sample.zip",
                "https://mirror.example.test/report.xhtml",
                "https://mirror.example.test/viewer",
                package_sha256,
                "2025-04-02T00:00:00",
            ],
        )
    object_store = _FakeObjectStore({})
    client = _FakeDownloadClient(package_body)

    metadata = run_esef_document_extraction_partition(
        esef_filings_duckdb=database,
        object_store=object_store,
        client=client,
        company_links={"549300SAMPLE000000001": ("SE", "5566000000")},
        partition_key="2025-03-30",
        source_run_id="deduplicated-run",
        source_document_ids=[],
        max_documents=None,
        refresh_existing=False,
        validate_esef=False,
        log_info=lambda *_args: None,
    )

    assert metadata["selected_document_count"] == 2
    assert metadata["unique_package_count"] == 1
    assert metadata["parsed_package_count"] == 1
    assert client.calls == ["https://example.test/sample.zip"]
    with database.get_connection() as connection:
        [(document_count,)] = connection.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}."
            f"{tables.ESEF_SOURCE_DOCUMENTS_TABLE}"
        ).fetchall()
    assert document_count == 2


def test_document_assets_use_processed_week_partitions_and_share_source_deps() -> None:
    partition_keys = ESEF_PROCESSED_WEEK_PARTITIONS.get_partition_keys()
    manifest_dependencies = {
        AssetKey("esef_filings_index_duckdb"),
        AssetKey("esef_entity_registry_map_clickhouse"),
    }
    artifact_dependency = {AssetKey("esef_document_artifacts_s3")}

    assert partition_keys[0] == "2023-01-01"
    window = ESEF_PROCESSED_WEEK_PARTITIONS.time_window_for_partition_key("2025-03-30")
    assert window.end - window.start == timedelta(days=7)
    assert (
        esef_document_extraction_manifest_s3.asset_deps[
            AssetKey("esef_document_extraction_manifest_s3")
        ]
        == manifest_dependencies
    )
    assert esef_document_extraction_manifest_s3.op.pool == "esef_filings_duckdb"
    assert esef_document_artifacts_s3.op.pool == ESEF_ARELLE_POOL
    assert {
        dep.asset_key
        for spec in assets.esef_filing_facts_duckdb.specs
        for dep in spec.deps
    } == artifact_dependency
    assert (
        esef_document_extraction_duckdb.asset_deps[
            AssetKey("esef_source_documents_duckdb")
        ]
        == artifact_dependency
    )
    assert (
        esef_document_extraction_duckdb.asset_deps[
            AssetKey("esef_document_contact_candidates_duckdb")
        ]
        == artifact_dependency
    )
    assert (
        esef_document_extraction_duckdb.asset_deps[
            AssetKey("esef_document_concept_labels_duckdb")
        ]
        == artifact_dependency
    )
    assert esef_document_extraction_duckdb.op.pool == "esef_filings_duckdb"


def _seed_filing_index(database: object, *, package_sha256: str) -> None:
    with database.get_connection() as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"create table {tables.QUALIFIED_FILINGS_INDEX_TABLE} ("
            "fxo_id varchar, lei varchar, entity_name varchar, country varchar, "
            "period_end varchar, package_url varchar, report_url varchar, "
            "viewer_url varchar, package_sha256 varchar, processed_at varchar)"
        )
        connection.execute(
            f"insert into {tables.QUALIFIED_FILINGS_INDEX_TABLE} values "
            "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                "SAMPLE-2024",
                "549300SAMPLE000000001",
                "Sample AB",
                "SE",
                "2024-12-31",
                "https://example.test/sample.zip",
                "https://example.test/report.xhtml",
                "https://example.test/viewer",
                package_sha256,
                "2025-04-01T00:00:00",
            ],
        )


class _FakeDownloadClient:
    def __init__(self, package_body: bytes) -> None:
        self.package_body = package_body
        self.calls: list[str] = []

    def download_json_facts(self, url: str, target: Path) -> None:
        self.calls.append(url)
        target.write_bytes(self.package_body)


class _FakeObjectStore:
    def __init__(self, objects: dict[tuple[str, str], bytes]) -> None:
        self.objects = objects
        self.created_buckets: list[str] = []

    def list_keys(self, prefix: str, bucket: str | None = None) -> list[str]:
        return sorted(
            key
            for object_bucket, key in self.objects
            if object_bucket == bucket and key.startswith(prefix)
        )

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]

    def write_bytes(
        self,
        key: str,
        body: bytes,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body

    def upload_file(
        self,
        key: str,
        source_path: Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = source_path.read_bytes()

    def download_file(
        self,
        key: str,
        target_path: Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        target_path.write_bytes(self.objects[(bucket, key)])

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)


def _write_sample_report_package(tmp_path: Path) -> Path:
    package_path = tmp_path / "sample-report-package.zip"
    package_root = "sample"
    with zipfile.ZipFile(package_path, "w") as package:
        package.writestr(
            f"{package_root}/META-INF/reportPackage.json",
            json.dumps(
                {
                    "documentInfo": {
                        "documentType": "https://xbrl.org/report-package/2023"
                    }
                }
            ),
        )
        package.writestr(
            f"{package_root}/META-INF/catalog.xml",
            _CATALOG_XML,
        )
        package.writestr(
            f"{package_root}/META-INF/taxonomyPackage.xml",
            _TAXONOMY_PACKAGE_XML,
        )
        package.writestr(
            f"{package_root}/taxonomy/sample.xsd",
            _TAXONOMY_XSD,
        )
        package.writestr(
            f"{package_root}/taxonomy/sample-labels.xml",
            _LABEL_LINKBASE_XML,
        )
        package.writestr(
            f"{package_root}/reports/sample.xhtml",
            _REPORT_XHTML,
        )
    return package_path


_CATALOG_XML = """<?xml version="1.0" encoding="UTF-8"?>
<catalog xmlns="urn:oasis:names:tc:entity:xmlns:xml:catalog">
  <rewriteURI uriStartString="http://example.test/taxonomy/" rewritePrefix="../taxonomy/"/>
</catalog>
"""

_TAXONOMY_PACKAGE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<tp:taxonomyPackage xmlns:tp="http://xbrl.org/2016/taxonomy-package">
  <tp:identifier>http://example.test/sample</tp:identifier>
  <tp:name>Sample ESEF taxonomy</tp:name>
  <tp:version>1</tp:version>
  <tp:entryPoints>
    <tp:entryPoint>
      <tp:name>Sample</tp:name>
      <tp:entryPointDocument href="http://example.test/taxonomy/sample.xsd"/>
      <tp:languages><tp:language>en</tp:language></tp:languages>
    </tp:entryPoint>
  </tp:entryPoints>
</tp:taxonomyPackage>
"""

_TAXONOMY_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:xbrli="http://www.xbrl.org/2003/instance"
    xmlns:link="http://www.xbrl.org/2003/linkbase"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:sample="http://example.test/taxonomy"
    targetNamespace="http://example.test/taxonomy">
  <xs:import namespace="http://www.xbrl.org/2003/instance"
             schemaLocation="http://www.xbrl.org/2003/xbrl-instance-2003-12-31.xsd"/>
  <xs:annotation><xs:appinfo>
    <link:linkbaseRef xlink:type="simple"
      xlink:arcrole="http://www.w3.org/1999/xlink/properties/linkbase"
      xlink:role="http://www.xbrl.org/2003/role/labelLinkbaseRef"
      xlink:href="sample-labels.xml"/>
  </xs:appinfo></xs:annotation>
  <xs:element id="sample_Name" name="NameOfReportingEntityOrOtherMeansOfIdentification"
    substitutionGroup="xbrli:item" type="xbrli:stringItemType"
    xbrli:periodType="duration" nillable="true"/>
  <xs:element id="sample_Description" name="DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities"
    substitutionGroup="xbrli:item" type="xbrli:stringItemType"
    xbrli:periodType="duration" nillable="true"/>
  <xs:element id="sample_Revenue" name="Revenue"
    substitutionGroup="xbrli:item" type="xbrli:monetaryItemType"
    xbrli:periodType="duration" xbrli:balance="credit" nillable="true"/>
</xs:schema>
"""

_LABEL_LINKBASE_XML = """<?xml version="1.0" encoding="UTF-8"?>
<link:linkbase xmlns:link="http://www.xbrl.org/2003/linkbase"
               xmlns:xlink="http://www.w3.org/1999/xlink">
  <link:labelLink xlink:type="extended" xlink:role="http://www.xbrl.org/2003/role/link">
    <link:loc xlink:type="locator" xlink:href="sample.xsd#sample_Description" xlink:label="description"/>
    <link:label xlink:type="resource" xlink:label="description_label"
      xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="en">Description of operations</link:label>
    <link:label xlink:type="resource" xlink:label="description_label_sv"
      xlink:role="http://www.xbrl.org/2003/role/label" xml:lang="sv">Beskrivning av verksamheten</link:label>
    <link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label"
      xlink:from="description" xlink:to="description_label"/>
    <link:labelArc xlink:type="arc" xlink:arcrole="http://www.xbrl.org/2003/arcrole/concept-label"
      xlink:from="description" xlink:to="description_label_sv"/>
  </link:labelLink>
</link:linkbase>
"""

_REPORT_XHTML = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml"
      xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
      xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2020-02-12"
      xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
      xmlns:link="http://www.xbrl.org/2003/linkbase"
      xmlns:sample="http://example.test/taxonomy"
      xmlns:xbrli="http://www.xbrl.org/2003/instance"
      xmlns:xlink="http://www.w3.org/1999/xlink"
      xml:lang="sv">
  <head><title>Sample report</title></head>
  <body>
    <ix:header>
      <ix:references>
        <link:schemaRef xlink:type="simple" xlink:href="http://example.test/taxonomy/sample.xsd"/>
      </ix:references>
      <ix:resources>
        <xbrli:context id="duration">
          <xbrli:entity>
            <xbrli:identifier scheme="http://standards.iso.org/iso/17442">549300SAMPLE000000001</xbrli:identifier>
          </xbrli:entity>
          <xbrli:period>
            <xbrli:startDate>2024-01-01</xbrli:startDate>
            <xbrli:endDate>2024-12-31</xbrli:endDate>
          </xbrli:period>
        </xbrli:context>
        <xbrli:unit id="EUR"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
      </ix:resources>
      <ix:relationship fromRefs="revenue" toRefs="revenue-note"/>
    </ix:header>
    <p><ix:nonNumeric id="name" name="sample:NameOfReportingEntityOrOtherMeansOfIdentification"
      contextRef="duration" xml:lang="en">Sample Oils AB</ix:nonNumeric></p>
    <p><ix:nonNumeric id="description" name="sample:DescriptionOfNatureOfEntitysOperationsAndPrincipalActivities"
      contextRef="duration" continuedAt="description-2" xml:lang="en">Makes </ix:nonNumeric></p>
    <ix:continuation id="description-2" continuedAt="description-3" xml:lang="en">better </ix:continuation>
    <ix:continuation id="description-3" xml:lang="en">oils.</ix:continuation>
    <p><ix:nonFraction id="revenue" name="sample:Revenue" contextRef="duration"
      unitRef="EUR" decimals="-3" scale="3" format="ixt:num-comma-decimal">1.234,5</ix:nonFraction></p>
    <ix:footnote id="revenue-note" xml:lang="en">Revenue footnote.</ix:footnote>
    <section id="contacts">
      <p>Investor relations:
        <a href="mailto:investor.relations@sample-oils.se">Investor.Relations@Sample-Oils.se</a>
      </p>
      <p>Switchboard: <a href="tel:+46401234567">040-123 45 67</a></p>
      <p>Auditor: audit@audit-firm.se</p>
      <p>Website:
        <a href="https://www.sample-oils.se/?utm_source=report#contact">www.Sample-Oils.se</a>
      </p>
      <p>Investor relations website: investors.sample-oils.se/reports/</p>
      <p>Auditor website:
        <a href="https://audit-firm.se/">audit-firm.se</a>
      </p>
      <p>Follow us:
        <a href="https://www.linkedin.com/company/sample-oils/">LinkedIn</a>
      </p>
      <script>hidden@do-not-use.se +46 40 999 99 99</script>
    </section>
    <section id="board">
      <h2>Board of Directors</h2>
      <p>Anna Andersson — Chair and board member.</p>
      <p>Bo Berg — Board member.</p>
    </section>
  </body>
</html>
"""

_POSITIONED_VISIBLE_SECTIONS_XHTML = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml" xml:lang="sv">
  <head><style>
    .pf { position: relative; } .t { position: absolute; }
    .w0 { width: 800px; } .h0 { height: 1100px; }
    .x1 { left: 50px; } .x2 { left: 210px; }
    .y1 { bottom: 1000px; } .y2 { bottom: 850px; }
    .y3 { bottom: 820px; } .y4 { bottom: 790px; }
    .y5 { bottom: 760px; } .y6 { bottom: 70px; }
    .y7 { bottom: 50px; } .y8 { bottom: 30px; }
    .hidden { display: none; }
  </style></head>
  <body>
    <div id="pf_management" class="pf w0 h0"><div>
      <div class="t x2 y3">Finanschef och vice VD sedan 2012.</div>
      <div class="t x1 y1">FÖRETAGSLEDNING</div>
      <div class="t x2 y2">BJÖRN GARAT</div>
      <div class="t x2 y4">Utbildning: Civilekonom.</div>
      <div class="t x2 y5">Arbetslivserfarenhet: Finansanalytiker.</div>
      <div class="t x1 y6">Sample AB, Storgatan 1, 111 22 Stockholm</div>
      <div class="t x1 y7">Org. nr. 556000-0000 · Telefon 08-123 45 67 · www.sample.se</div>
      <div class="t x1 y8">72</div>
      <div class="t x2 y2 hidden">Ignore Hidden Person — CEO</div>
    </div></div>
  </body>
</html>
"""

_CONTACT_REPORT_XHTML = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <title>Sample report</title>
    <style>.hidden { display: none; }</style>
  </head>
  <body>
    <section id="contacts">
      <p>Investor relations:
        <a href="mailto:investor.relations@sample-oils.se">Investor.Relations@Sample-Oils.se</a>
      </p>
      <p>Switchboard: <a href="tel:+46401234567">040-123 45 67</a></p>
      <p>Auditor: audit@audit-firm.se</p>
      <p>Revenue for 2024 was SEK 4 640 123 456 789.</p>
      <script>hidden@do-not-use.se +46 40 999 99 99</script>
      <svg xmlns="http://www.w3.org/2000/svg">
        <text>image-contact@do-not-use.se</text>
      </svg>
    </section>
  </body>
</html>
"""

_WEBSITE_REPORT_XHTML = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Sample report</title></head>
  <body>
    <section id="websites">
      <p>Website:
        <a href="https://www.sample-oils.se:443/?utm_source=report#contact">www.Sample-Oils.se</a>
      </p>
      <p>Investor relations website: investors.sample-oils.se/reports/</p>
      <p>Auditor website:
        <a href="https://audit-firm.se/">audit-firm.se</a>
      </p>
      <p>Follow us:
        <a href="https://www.linkedin.com/company/sample-oils/">LinkedIn</a>
      </p>
      <p>Email only: person@email-only.se</p>
      <p>Legal form: Sample Holdings Co.Ltd</p>
      <p>Filing package: sample-oils-2024.zip</p>
      <p>Broken line: www.corporate- governanceboard.se</p>
      <p>Accounting prose: i.Property, plant and equipment; changes occur m.in. notes.</p>
      <p>Graphic design by Example Studio www.design-vendor.se</p>
      <p>Standards: <a href="https://www.xbrl.org/">XBRL</a></p>
      <p><a href="https://cdn.sample-oils.se/logo.svg">Logo</a></p>
      <script>https://hidden.do-not-use.se/</script>
      <svg xmlns="http://www.w3.org/2000/svg">
        <text>https://image.do-not-use.se/</text>
      </svg>
    </section>
  </body>
</html>
"""

_REPORT_PRODUCTION_CREDITS_XHTML = """<?xml version="1.0" encoding="UTF-8"?>
<html xmlns="http://www.w3.org/1999/xhtml">
  <head><title>Sample report</title></head>
  <body>
    <footer>
      <p>Sample Oils AB · Switchboard: 040-123 45 67 · info@sample-oils.se · sample-oils.se</p>
      <p>Grafik och original: Design Vendor AB, www.design-vendor.se,
        studio@design-vendor.se, Telefon: 070-123 45 67.</p>
    </footer>
  </body>
</html>
"""
