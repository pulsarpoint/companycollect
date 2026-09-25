"""Deterministic crawl projections retain evidence and never infer company ownership."""

import json
from datetime import UTC, datetime
from uuid import UUID

import pytest

from dagster_v3.defs.website_crawl.normalization.parser import (
    parse_attempt,
    decode_archive,
)

NORM = UUID("00000000-0000-0000-0000-000000000001")


@pytest.fixture
def source():
    return dict(
        domain="example.se",
        crawl_type="full",
        request_id="request-1",
        attempt=1,
        input_revision=1,
        work_key="work",
        run_id="crawl-run",
        state="completed",
        crawl_status="finished",
        successful=True,
        started_at=datetime(2026, 9, 20, tzinfo=UTC),
        finished_at=datetime(2026, 9, 20, 0, 1, tzinfo=UTC),
        ingested_at=datetime(2026, 9, 20, 0, 2, tzinfo=UTC),
        website_url="https://example.se/",
        s3_path="crawls/company-crawls/request-1/attempts/0001/result.json.gz",
        s3_state="uploaded",
        site_info=None,
        pages="[]",
        page_observations="[]",
        error="",
    )


@pytest.fixture
def payload():
    profile = dict(
        crawl_decision="continue_crawling",
        purpose="Present engineering services",
        site_types=["company"],
        research_profiles=["service_provider"],
        operator_name="Example AB",
        site_description="Engineering company.",
        business_activities=["Engineering", "Consulting"],
    )
    observations = dict(
        schema_version="company-page-observations/1.1",
        page_id="p0001",
        source_url="https://www.example.se/",
        status="collected",
        errors=[],
        metadata={
            "title": "Example",
            "description": "unused",
            "language": "sv",
            "meta": {"description": "Company page"},
            "canonical_url": "https://example.se/",
            "links": [],
            "alternate_languages": [],
        },
        contacts=[
            dict(
                type="email",
                value="info@example.se",
                raw_value="info@example.se",
                source="jsonld",
                locator={"script_index": 0, "entity_path": "$", "property": "email"},
            )
        ],
        identifiers=[
            dict(
                type="vat",
                value="SE123",
                raw_value="SE 123",
                source="jsonld",
                locator={"property": "vatID"},
            )
        ],
        structured_data={
            "jsonld_blocks": [],
            "jsonld_entities": [
                {
                    "script_index": 0,
                    "entity_path": "$",
                    "types": ["JobPosting"],
                    "id": "job-1",
                    "data": {
                        "@type": "JobPosting",
                        "@id": "job-1",
                        "title": "Engineer",
                        "hiringOrganization": {"name": "Actual employer"},
                        "jobLocation": {
                            "address": {
                                "addressLocality": "Stockholm",
                                "addressCountry": "SE",
                            }
                        },
                        "datePosted": "2026-09-01",
                        "validThrough": "unknown",
                        "employmentType": ["FULL_TIME"],
                        "description": "Design systems.",
                        "identifier": {"value": "J-1"},
                        "url": "/jobs/1",
                        "nullable": None,
                        "active": True,
                        "salary": 12.5,
                        "nested": {"a/b": [0, False]},
                    },
                }
            ],
            "microdata": [],
        },
        document_links=[
            {
                "url": "https://example.se/report.pdf",
                "text": "Report",
                "anchor_index": 2,
            }
        ],
        financial_links=[
            {
                "url": "https://example.se/report.pdf",
                "text": "Report",
                "anchor_index": 2,
            }
        ],
    )
    return {
        "schema_version": "company-crawl-result/1.2",
        "crawl": {
            "schema_version": "company-crawl/1.0",
            "input_url": "https://example.se/",
            "site_url": "https://www.example.se/",
            "status": "finished",
            "full_crawl_all": False,
            "pages": [
                {
                    "page_id": "p0001",
                    "requested_url": "https://example.se/",
                    "source_url": "https://www.example.se/",
                    "status_code": 200,
                    "fetch_status": "fetched",
                }
            ],
            "site_gate": {
                "decision": "continue_crawling",
                "scope": "first_page_only",
                "profile": {
                    "record_id": "class-1",
                    "data": profile,
                    "evidence_status": "source_matched",
                    "sources": [
                        {
                            "page_id": "p0001",
                            "url": "https://www.example.se/",
                            "evidence": [
                                {
                                    "text": "Engineering and consulting",
                                    "matched_in": "text",
                                }
                            ],
                        }
                    ],
                },
            },
        },
        "documents": [
            {
                "page_id": "p0001",
                "url": "https://www.example.se/",
                "input": {"observations": observations},
            }
        ],
    }


def parse(source, payload):
    return parse_attempt(source, payload, NORM, 1, "normalization-run")


def test_classification_activities_and_page_provenance(source, payload):
    rows = parse(source, payload)
    (profile,) = rows["site_profiles"]
    assert profile["purpose_original"] == "Present engineering services"
    assert profile["research_profiles"] == ["service_provider"]
    assert profile["site_types"] == ["company"]
    assert profile["evidence"] == ["Engineering and consulting"]
    assert [r["activity_original"] for r in rows["business_activities"]] == [
        "Engineering",
        "Consulting",
    ]
    assert (
        rows["business_activities"][1]["classification_evidence"] == profile["evidence"]
    )
    (page,) = rows["pages"]
    assert page["description_original"] == "Company page"
    (contact,) = rows["contacts"]
    assert contact["source_url"] == "https://www.example.se/"
    assert contact["domain"] == "example.se" and contact["request_id"] == "request-1"
    assert contact["normalization_id"] == NORM
    assert "email" in contact["source_locator"]
    assert len(rows["links"]) == 3  # canonical plus two observed link categories


def test_structured_jobs_are_postings_not_employer_domain_inferences(source, payload):
    rows = parse(source, payload)
    (job,) = rows["jobs"]
    assert job["employer"] == "Actual employer"
    assert job["title_original"] == "Engineer" and job["source_job_id"] == "J-1"
    assert job["job_url"] == "https://www.example.se/jobs/1"
    assert job["posted_at"] == datetime(2026, 9, 1, tzinfo=UTC)
    assert job["expires_at"] is None and job["expires_at_original"] == "unknown"
    assert "Stockholm" in job["location_original"]
    assert job["extraction_source"] == "jsonld"
    assert rows["scans"][0]["structured_jobs_status"] == "completed"


def test_scalar_number_text_paths_and_explicit_null_are_retained(source, payload):
    payload = decode_archive(json.dumps(payload).replace("12.5", "9007199254740993"))
    values = {r["property_path"]: r for r in parse(source, payload)["structured_data"]}
    assert values["/salary"]["value_number_original"] == "9007199254740993"
    assert values["/nullable"]["value_type"] == "null"
    assert values["/active"]["value_boolean"] is True
    assert values["/nested/a~1b/1"]["value_boolean"] is False


def test_missing_sections_are_unavailable_not_no_vacancies(source, payload):
    payload["documents"][0]["input"]["observations"]["structured_data"] = None
    rows = parse(source, payload)
    assert rows["jobs"] == []
    assert rows["scans"][0]["structured_jobs_status"] == "not_available"


def test_basic_profile_keeps_research_profiles_from_gate(source, payload):
    source["crawl_type"] = "site_info"
    payload["crawl"]["site_info"] = {
        **payload["crawl"]["site_gate"]["profile"]["data"],
        "source_url": "https://www.example.se/",
        "evidence_status": "source_matched",
        "evidence": ["Engineering and consulting"],
    }
    del payload["crawl"]["site_info"]["research_profiles"]
    assert parse(source, payload)["site_profiles"][0]["research_profiles"] == [
        "service_provider"
    ]


def test_failed_attempt_with_no_archive_keeps_diagnostics(source):
    source.update(
        state="failed",
        crawl_status="failed",
        successful=False,
        error="Browser failed",
        s3_path="",
        s3_state="failed",
    )
    rows = parse(source, None)
    assert rows["scans"][0]["error"] == "Browser failed"
    assert rows["pages"] == [] and rows["site_profiles"] == []
    assert rows["scans"][0]["successful"] is False


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p.update(schema_version="company-crawl-result/99"),
        lambda p: p["crawl"].update(input_url="https://different.se/"),
        lambda p: p.update(request_id="wrong"),
        lambda p: p["documents"][0]["input"]["observations"].update(
            schema_version="company-page-observations/99"
        ),
        lambda p: p["documents"][0]["input"]["observations"].update(
            contacts={"unexpected": "object"}
        ),
        lambda p: p["documents"][0]["input"]["observations"].update(page_id="wrong"),
    ],
)
def test_invalid_sources_fail_before_publication(source, payload, change):
    change(payload)
    with pytest.raises(ValueError):
        parse(source, payload)


def test_unknown_override_stays_unknown_for_historical_results(source, payload):
    del payload["crawl"]["full_crawl_all"]
    p = parse(source, payload)["site_profiles"][0]
    assert p["full_crawl_all"] is None and p["classification_overridden"] is None


def test_supported_legacy_jobs_keep_source_evidence(source, payload):
    payload["records"] = {
        "jobs": [
            {
                "record_id": "legacy-1",
                "data": {
                    "title": "Engineer",
                    "employer": "Other AB",
                    "job_url": "https://example.se/jobs/2",
                },
                "sources": [
                    {
                        "page_id": "p0001",
                        "url": "https://www.example.se/",
                        "evidence": [
                            {"text": "Hiring engineers", "matched_in": "text"}
                        ],
                    }
                ],
                "evidence_status": "source_matched",
            }
        ]
    }
    jobs = parse(source, payload)["jobs"]
    legacy = [j for j in jobs if j["extraction_source"] == "legacy_record"]
    assert len(legacy) == 1 and legacy[0]["evidence"] == ["Hiring engineers"]


def test_recovered_errors_do_not_reclassify_a_success(source, payload):
    payload["crawl"]["errors"] = [{"error": "Transient issue, recovered"}]
    (scan,) = parse(source, payload)["scans"]
    assert scan["successful"] is True and scan["error"] is None


def test_unsuccessful_model_reason_remains_queryable(source, payload):
    source.update(successful=False, crawl_status="needs_review")
    payload["crawl"]["usage"] = {
        "by_call": [{"provider_error": {"message": "Model is unavailable"}}]
    }
    assert parse(source, payload)["scans"][0]["error"] == "Model is unavailable"


def test_jsonld_parse_error_is_partial_not_empty_completed(source, payload):
    payload["documents"][0]["input"]["observations"]["structured_data"][
        "jsonld_blocks"
    ] = [{"status": "invalid_json"}]
    assert parse(source, payload)["scans"][0]["structured_jobs_status"] == "partial"
