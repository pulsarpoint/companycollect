import unittest

from test_package import page

from crawler_service.analytics import (
    summarize_entities,
    summarize_technologies,
    technology_submission_records,
)
from crawler_service.models import RECORD_TYPES, Finding, Findings, Source


def record(
    record_id: str, data: dict, url: str = "https://example.test/team"
) -> Finding:
    return Finding(
        record_id=record_id,
        data=data,
        evidence_status="source_matched",
        sources=[
            Source(
                url=url,
                page_id=record_id,
                fetched_at="2026-09-08T00:00:00Z",
                html_sha256="a" * 64,
                chunk_start=0,
                chunk_end=100,
                evidence=[{"text": "Fixture evidence", "matched_in": "text"}],
                evidence_status="source_matched",
                issues=[],
            )
        ],
    )


class EntityTests(unittest.TestCase):
    def setUp(self):
        self.records = Findings(**{objective: [] for objective in RECORD_TYPES})

    def test_person_role_variants_merge_but_companies_and_conflicting_profiles_do_not(
        self,
    ):
        data = {
            "name": "Ada Example",
            "company": "DemoWorks",
            "role": "VP Radar",
            "profile_url": None,
        }
        self.records.people = [
            record("1", data),
            record(
                "2",
                data | {"role": "Vice President of Radar"},
                "https://example.test/about",
            ),
            record("3", data | {"company": "Other"}),
        ]
        groups = summarize_entities(self.records, []).people
        self.assertEqual(len(groups), 2)
        self.assertEqual(groups[0].record_ids, ["1", "2"])
        self.assertEqual(len(groups[0].source_urls), 2)
        self.assertEqual(
            groups[0].field_values["role"], ["VP Radar", "Vice President of Radar"]
        )
        self.records.people[0].data["profile_url"] = "https://example.test/ada-one"
        self.records.people[1].data["profile_url"] = "https://example.test/ada-two"
        self.assertEqual(len(summarize_entities(self.records, []).people), 3)

    def test_job_listing_and_linked_external_detail_join_without_fuzzy_titles(self):
        internal = "https://example.test/career/dsp"
        external = "https://ats.test/jobs/123"
        data = {"employer": "DemoWorks", "title": "DSP", "job_url": internal}
        self.records.jobs = [
            record("1", data, "https://example.test/careers"),
            record("2", data | {"job_url": external}, internal),
            record(
                "3", data | {"title": "Internship - DSP", "job_url": external}, external
            ),
            record("4", data | {"job_url": "https://ats.test/jobs/456"}),
            record("5", data | {"employer": "Other"}),
        ]
        groups = summarize_entities(self.records, []).jobs
        self.assertEqual(len(groups), 3)
        self.assertEqual(groups[0].record_ids, ["1", "2", "3"])
        self.assertEqual(groups[0].field_values["title"], ["DSP", "Internship - DSP"])

    def test_job_redirects_join_but_missing_urls_do_not_guess_identity(self):
        url = "https://example.test/career/python"
        redirected = "https://ats.test/jobs/42"
        data = {"employer": "DemoWorks", "title": "Engineer", "job_url": url}
        self.records.jobs = [
            record("1", data),
            record("2", data | {"job_url": redirected}),
            record("3", data | {"job_url": None}),
        ]
        fetched = page("", redirected)
        fetched.requested_url = url
        groups = summarize_entities(self.records, [fetched]).jobs
        self.assertEqual(len(groups), 2)

    def test_technology_identity_dedup_retains_signals_dates_and_rejects_invalid_claims(
        self,
    ):
        data = {
            "company": "DemoWorks",
            "technology": "Python",
            "category": "programming_language",
            "signal": "required_experience",
            "scope": "role",
            "as_of": None,
            "alternative_group": None,
            "job_url": None,
            "catalog_match": {"status": "matched", "canonical_technology": "Python"},
        }
        self.records.technology_signals = [
            record("1", data),
            record("2", data | {"signal": "past_use", "as_of": "2020"}),
            record("3", data | {"interpretation_review": {"supported": False}}),
            record("4", data | {"catalog_error": "Resolution failed"}),
            record("5", data),
        ]
        self.records.technology_signals[-1].evidence_status = "needs_review"
        groups = summarize_entities(self.records, []).technologies
        self.assertEqual(len(groups), 1)
        self.assertEqual(groups[0].record_ids, ["1", "2"])
        self.assertEqual(
            groups[0].field_values["signal"], ["required_experience", "past_use"]
        )
        self.assertEqual(
            len(summarize_technologies(self.records.technology_signals)), 2
        )
        self.assertEqual(
            [
                r["record_id"]
                for r in technology_submission_records(self.records.technology_signals)
            ],
            ["1", "2"],
        )
        self.assertEqual(len(self.records.technology_signals), 5)

    def test_submission_keeps_only_verified_sources(self):
        finding = record(
            "1", {"company": "DemoWorks", "catalog_match": {"status": "proposed"}}
        )
        invalid = finding.sources[0].model_copy(deep=True)
        invalid.evidence_status = "needs_review"
        finding.sources.append(invalid)
        self.assertEqual(len(technology_submission_records([finding])[0]["sources"]), 1)
        self.assertEqual(len(finding.sources), 2)
