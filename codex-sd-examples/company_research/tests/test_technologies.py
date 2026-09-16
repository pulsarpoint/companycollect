import unittest

from pydantic import ValidationError
from test_package import assessment, page

from company_research.analytics import accepted_finding, summarize_technologies
from company_research.content import HtmlWindow, merge_finding, source_finding
from company_research.discovery import CrawlQueue
from company_research.models import (
    OBJECTIVES,
    Finding,
    ModelTechnologyMatch,
    ProposedTechnology,
    ResearchConfig,
    TechnologySignal,
)


def signal(**changes) -> dict:
    return {
        "company": "DemoWorks",
        "technology": "Python",
        "category": "programming_language",
        "signal": "stated_use",
        "scope": "team",
        "job_employer": "DemoWorks",
        "job_title": "Engineer",
        "job_url": "/jobs/engineer",
        "alternative_group": None,
        "context": "The team develops tools in Python.",
        "as_of": None,
        "evidence": ["DemoWorks", "Engineer", "Our team develops tools in Python."],
    } | changes


def finding(record: dict, html: str, url: str = "https://example.test/jobs/engineer"):
    parsed = TechnologySignal.model_validate(record).model_dump()
    return source_finding(
        "technology_signals",
        parsed,
        page=page(html, url).model_dump(),
        window=HtmlWindow(0, len(html), html),
    )


class TechnologyTests(unittest.TestCase):
    def test_saved_generic_technology_cannot_enter_accepted_summaries(self):
        record = finding(
            signal(
                company=None,
                job_employer=None,
                job_title=None,
                job_url=None,
                evidence=["Python"],
            ),
            "<p>Python</p>",
        )
        self.assertTrue(accepted_finding(record))
        # Finding.data intentionally accepts historical payloads without re-running
        # TechnologySignal validation. The export boundary must enforce it too.
        record.data["technology"] = "CMOS"
        self.assertFalse(accepted_finding(record))
        self.assertEqual(summarize_technologies([record]), [])

    def test_credential_document_type_without_document_reference_is_held(self):
        source = finding(
            signal(
                company=None,
                job_employer=None,
                job_title=None,
                job_url=None,
                evidence=["Python"],
            ),
            "<p>Python</p>",
        ).sources[0]
        record = Finding(
            record_id="certificate",
            sources=[source],
            evidence_status="source_matched",
            data={
                "standard_name": "ISO 9001",
                "document_type": "certificate",
                "document_url": None,
            },
        )
        self.assertFalse(accepted_finding(record))
        record.data["document_type"] = None
        self.assertTrue(accepted_finding(record))

    def test_formats_and_capabilities_cannot_be_technology_identities(self):
        for name in (
            "XML",
            " json ",
            "ＣＳＶ",
            "CVAT XML",
            "Radar",
            "FPGA",
            "AI",
            "RISC-V",
            "CMOS",
            "BiCMOS",
            "SiGe",
            "Child Presence Detection",
            "Seat Occupancy Detection",
            "Intrusion & Proximity Alert",
        ):
            with self.subTest(name=name):
                with self.assertRaises(ValidationError):
                    TechnologySignal.model_validate(signal(technology=name))
                with self.assertRaises(ValidationError):
                    ProposedTechnology.model_validate(
                        {
                            "name": name,
                            "description": "Not a specific tool",
                            "website": None,
                            "category_ids": [],
                            "category_suggestion": None,
                            "saas": None,
                            "oss": None,
                            "pricing": [],
                        }
                    )
                with self.assertRaises(ValidationError):
                    ModelTechnologyMatch.model_validate(
                        {
                            "status": "matched",
                            "canonical_technology": name,
                            "proposed_technology": None,
                            "reason": "Present in catalog",
                        }
                    )

    def test_scope_guard_does_not_reject_specific_tools_containing_generic_words(self):
        for name in (
            "Ansys HFSS",
            "CST Studio Suite",
            "AURIX",
            "JSONata",
            "WIPL-D Pro CAD",
            "Python",
            "PostgreSQL",
        ):
            with self.subTest(name=name):
                self.assertEqual(
                    TechnologySignal.model_validate(signal(technology=name)).technology,
                    name,
                )

    def test_inline_markup_spacing_does_not_reject_a_supported_clause(self):
        html = "<h1>DemoWorks Engineer</h1><p>Familiarity with <b>Yocto</b>.</p>"
        observation = finding(
            signal(
                technology="Yocto",
                signal="required_experience",
                evidence=["DemoWorks", "Engineer", "Familiarity with Yocto."],
            ),
            html,
        )
        self.assertEqual(observation.evidence_status, "source_matched")

    def test_matching_checks_technology_identity_attribution_and_url(self):
        html = "<h1>DemoWorks Engineer</h1><p>Our team develops tools in Python.</p>"
        accepted = finding(signal(), html)
        self.assertEqual(accepted.evidence_status, "source_matched")
        for changes, issue in [
            ({"technology": "R"}, "technology_not_in_evidence"),
            ({"company": "Invented Parent"}, "company_not_in_evidence"),
            ({"job_url": "/invented"}, "job_url_absent"),
            (
                {"alternative_group": "Python or Go"},
                "alternative_group_not_in_evidence",
            ),
        ]:
            reviewed = finding(signal(**changes), html)
            self.assertEqual(reviewed.evidence_status, "needs_review")
            self.assertIn(issue, reviewed.sources[0].issues)
            self.assertEqual(summarize_technologies([reviewed]), [])

    def test_c_does_not_match_cplusplus_or_csharp(self):
        for technology in ("C++", "C#"):
            html = f"<h1>DemoWorks Engineer</h1><p>We develop in {technology}.</p>"
            record = signal(
                technology="C",
                evidence=["DemoWorks", "Engineer", f"We develop in {technology}."],
            )
            self.assertEqual(finding(record, html).evidence_status, "needs_review")
            self.assertEqual(
                finding(record | {"technology": technology}, html).evidence_status,
                "source_matched",
            )

    def test_summary_preserves_signal_scope_dates_alternatives_and_distinct_job_urls(
        self,
    ):
        html = "<h1>DemoWorks Engineer</h1><p>Our team develops tools in Python.</p><p>Python or Go experience is required.</p>"
        records = []
        for url, changes in [
            ("https://example.test/jobs/engineer", {}),
            ("https://example.test/jobs/engineer?utm_source=other", {}),
            ("https://example.test/jobs/second", {}),
            (
                "https://example.test/jobs/second",
                {
                    "signal": "required_experience",
                    "scope": "role",
                    "alternative_group": "Python or Go",
                    "evidence": [
                        "DemoWorks",
                        "Engineer",
                        "Python or Go experience is required.",
                    ],
                },
            ),
            ("https://example.test/jobs/second", {"as_of": "2022"}),
        ]:
            record = signal(job_url=url, **changes)
            merge_finding(records, finding(record, html, url))
        # Repeated windows do not create additional analytical job counts.
        merge_finding(records, finding(signal(), html))
        summaries = summarize_technologies(records)
        self.assertEqual(len(summaries), 3)
        current = next(
            s for s in summaries if s.signal == "stated_use" and s.as_of is None
        )
        self.assertEqual(current.distinct_job_url_count, 2)
        required = next(s for s in summaries if s.signal == "required_experience")
        self.assertEqual(required.alternative_group, "Python or Go")
        self.assertEqual(required.scope, "role")
        self.assertEqual(required.distinct_job_url_count, 1)

    def test_unknown_client_is_retained_but_not_assigned_to_recruiter(self):
        html = "<h1>HireCo Engineer</h1><p>Maintain our client's SAP system.</p>"
        record = signal(
            company=None,
            technology="SAP",
            category="business_software",
            scope="client",
            job_employer="HireCo",
            evidence=["HireCo", "Engineer", "Maintain our client's SAP system."],
        )
        observation = finding(record, html)
        self.assertEqual(observation.evidence_status, "source_matched")
        self.assertEqual(summarize_technologies([observation]), [])

    def test_known_jobs_do_not_stop_selection_of_technology_descriptions(self):
        queue = CrawlQueue("https://example.test", ResearchConfig())
        queue.add(
            "/jobs/engineer", source=queue.site_url, label="Engineer job description"
        )
        candidate = next(iter(queue.candidates.values()))
        candidate.assessment = assessment(
            candidate.candidate_id, jobs="high", technology_signals="high"
        )
        counts = dict.fromkeys(OBJECTIVES, 1)
        counts["jobs"] = 55
        counts["technology_signals"] = 0
        selected = queue.pick(counts)
        assert selected is not None
        self.assertEqual(selected[1], "technology_signals")


if __name__ == "__main__":
    unittest.main()
