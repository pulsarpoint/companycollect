"""Regressions for target-specific navigation and misleading source-matched claims."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture, proposal
from test_package import assessment, page, response
from test_technologies import finding, signal

from company_research.analytics import accepted_finding, technology_submission_records
from company_research.content import HtmlWindow, merge_finding, source_finding
from company_research.discovery import CrawlQueue, navigation_objectives
from company_research.llm import ModelClient
from company_research.models import (
    OBJECTIVES,
    ResearchConfig,
    validate_specific_technology_name,
)
from company_research.profiles import review_overview, validate_summary
from company_research.review import review_claims, review_proposals


class TargetQueueTests(unittest.TestCase):
    def test_partner_profile_does_not_authorize_partner_investors(self):
        queue = CrawlQueue("https://target.example/", ResearchConfig())
        url = "https://partner.example/partners/target"
        queue.add(url, source=queue.site_url)
        candidate = queue.candidates[url]
        candidate.assessment = assessment(
            candidate.candidate_id, company_relationships="high"
        )
        self.assertEqual(queue.pick(dict.fromkeys(OBJECTIVES, 0))[0], candidate)
        queue.add("https://investor.partner.example/", source=url)
        queue.add("https://partner.example/careers", source=url)
        self.assertEqual(len(queue.candidates), 1)
        self.assertEqual(queue.excluded["outside_approved_scope"], 2)

    def test_related_company_general_page_is_not_useful_for_target(self):
        queue = CrawlQueue("https://target.example/", ResearchConfig())
        queue.add("https://partner.example/careers", source=queue.site_url)
        candidate = next(iter(queue.candidates.values()))
        candidate.assessment = assessment(candidate.candidate_id, jobs="high")
        candidate.assessment.target_relevance = "related_company"
        self.assertIsNone(queue.pick(dict.fromkeys(OBJECTIVES, 0)))

    def test_observed_job_list_schedules_external_detail_even_after_exploration(self):
        queue = CrawlQueue(
            "https://target.example/", ResearchConfig(job_detail_reserve=1)
        )
        queue.site_profile = {"operator_name": "Target"}
        url = "https://target.ats.example/job/engineer"
        html = f'<h1>Target Careers</h1><a href="{url}">Engineer</a>'
        source_page = page(html, "https://target.example/careers")
        source_page.selected_for = "exploration:jobs"
        queue.add(url, source=source_page.source_url, label="Engineer")
        job = source_finding(
            "jobs",
            {
                "employer": "Target",
                "title": "Engineer",
                "job_url": url,
                "evidence": ["Target", "Engineer", url],
            },
            page=source_page.model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        queue.observe_findings(source_page, [job], [])
        self.assertEqual(queue.navigation_visits["jobs"], 1)
        chosen, focus = queue.pick(dict.fromkeys(OBJECTIVES, 1))
        self.assertEqual((chosen.url, focus), (url, "job_detail_followup"))
        queue.visited.add(url)
        self.assertEqual(queue.snapshot()["job_coverage"]["unvisited_job_urls"], [])
        self.assertIn("jobs", navigation_objectives(url, []))
        self.assertFalse(queue.is_target("Target India Private Ltd"))

    def test_employer_navigation_requires_observed_target_relevance(self):
        queue = CrawlQueue("https://target.example/", ResearchConfig())
        board = "https://ats.example/target"
        queue.add(board, source=queue.site_url)
        candidate = queue.candidates[board]
        candidate.assessment = assessment(candidate.candidate_id, jobs="high")
        candidate.assessment.follow_scope = "target_navigation"
        queue.add("/target/job/one", source=board)
        self.assertEqual(len(queue.candidates), 1)
        candidate.observed_relevance = "target"
        queue.add("/target/job/one", source=board)
        self.assertEqual(len(queue.candidates), 2)


class MeaningReviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_matching_quotes_do_not_override_wrong_holder_identity_or_document(
        self,
    ):
        cases = [
            (
                "certifications_compliance",
                {
                    "subject_name": "Target",
                    "subject_kind": "company",
                    "standard_name": "ISO 26262",
                    "claim_type": "certification",
                },
                {
                    "source_subject": "Target",
                    "source_subject_kind": "person",
                    "source_value": "ISO 26262",
                    "source_claim_type": "certification",
                },
            ),
            (
                "company_profile",
                {
                    "company": "Target",
                    "field": "legal_name",
                    "value": "Target India Private Ltd",
                },
                {
                    "source_subject": "Target India Private Ltd",
                    "source_value": "Target India Private Ltd",
                },
            ),
            (
                "document_links",
                {
                    "company": "Target",
                    "document_url": "https://target.example/quality",
                    "document_type": "certificate",
                    "reporting_period": None,
                },
                {
                    "source_subject": "Target",
                    "source_value": "https://target.example/quality",
                    "source_claim_type": "navigation",
                },
            ),
        ]
        html = '<p>Target has ISO 26262 certified experts. Target India Private Ltd opened an office.</p><a href="https://target.example/quality">Quality</a>'
        for objective, data, reconstructed in cases:
            with self.subTest(objective=objective), TemporaryDirectory() as directory:
                root = Path(directory)
                record = source_finding(
                    objective,
                    data
                    | {
                        "evidence": [
                            "Target",
                            "ISO 26262",
                            "Target India Private Ltd",
                            "https://target.example/quality",
                        ]
                    },
                    page=page(html).model_dump(),
                    window=HtmlWindow(0, len(html), html),
                )
                self.assertEqual(record.evidence_status, "source_matched")
                duplicate = record.model_copy(deep=True)
                review = {
                    "record_id": record.record_id,
                    "supported": True,
                    "reason": "Reconstructed source meaning",
                    "source_subject": None,
                    "source_object": None,
                    "specific_technology": None,
                    "source_signal": None,
                } | reconstructed
                async with httpx.AsyncClient(
                    base_url="https://test.invalid/",
                    transport=httpx.MockTransport(
                        lambda _, review=review: httpx.Response(
                            200, json=response({"reviews": [review]})
                        )
                    ),
                ) as client:
                    await review_claims(
                        [record, duplicate],
                        ModelClient(client, "test", ResearchConfig(), root),
                        root,
                        "meaning",
                    )
                self.assertEqual(record.evidence_status, "needs_review")
                self.assertFalse(accepted_finding(duplicate))
                self.assertEqual(
                    duplicate.data["interpretation_review"],
                    record.data["interpretation_review"],
                )
                self.assertEqual(
                    record.data["interpretation_review"]["status"], "rejected"
                )
                self.assertEqual(record.sources[0].evidence_status, "source_matched")

    async def test_temporary_review_failure_retries_without_semantic_rejection(self):
        html = "<p>Target was founded in 2000.</p>"
        record = source_finding(
            "company_profile",
            {
                "company": "Target",
                "field": "founded",
                "value": "2000",
                "evidence": ["Target was founded in 2000."],
            },
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        calls = []

        def handle(request):
            calls.append(json.loads(request.content))
            if len(calls) == 1:
                return httpx.Response(503)
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": record.record_id,
                                "supported": True,
                                "reason": "Explicit founding date",
                                "source_subject": "Target",
                                "source_value": "2000",
                                "source_object": None,
                                "specific_technology": None,
                                "source_signal": None,
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            duplicate = record.model_copy(deep=True)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_claims(
                    [record, duplicate],
                    ModelClient(
                        client, "test", ResearchConfig(max_http_attempts=1), root
                    ),
                    root,
                    "retry",
                )
        self.assertEqual(len(calls), 2)
        self.assertEqual(record.data["interpretation_review"]["status"], "accepted")
        self.assertEqual(record.evidence_status, "source_matched")
        self.assertEqual(
            duplicate.data["interpretation_review"],
            record.data["interpretation_review"],
        )
        self.assertTrue(accepted_finding(duplicate))

    def test_summary_short_citations_expand_to_canonical_ids(self):
        fact = finding(signal(), "DemoWorks Engineer Python")
        overview = {
            "site_description": None,
            "company_name": {"text": "DemoWorks", "record_ids": ["r1"]},
            "company_description": None,
            "products_services": [],
            "industries": [],
            "certifications_compliance": [],
            "company_relationships": [],
        }
        result = validate_summary(overview, {"r1": fact})
        self.assertEqual(result["company_name"]["record_ids"], [fact.record_id])
        overview["company_name"]["record_ids"] = ["r99"]
        with self.assertRaisesRegex(ValueError, "absent"):
            validate_summary(overview, {"r1": fact})

    async def test_changed_certificate_number_is_rejected_even_if_model_approves(self):
        html = "Target obtained ISO 14001:2015 certification."
        fact = source_finding(
            "certifications_compliance",
            {
                "subject_name": "Target",
                "standard_name": "ISO 14001:2015",
                "evidence": [html],
            },
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        overview = {
            "site_description": None,
            "company_name": None,
            "company_description": None,
            "products_services": [],
            "industries": [],
            "company_relationships": [],
            "certifications_compliance": [
                {
                    "text": "Target obtained ISO 14000:2015 certification.",
                    "record_ids": [fact.record_id],
                }
            ],
        }
        with self.assertRaisesRegex(ValueError, "standard name exactly"):
            validate_summary(overview, {fact.record_id: fact})
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200,
                        json=response(
                            {
                                "reviews": [
                                    {
                                        "statement_id": "certifications_compliance:0",
                                        "supported": True,
                                        "reason": "Matches",
                                    }
                                ]
                            }
                        ),
                    )
                ),
            ) as client:
                checked = await review_overview(
                    overview,
                    {fact.record_id: fact},
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
        self.assertEqual(checked["certifications_compliance"], [])
        self.assertIn(
            "standard name exactly",
            checked["validation"]["excluded_statements"][0]["reason"],
        )

    def test_missing_required_reviews_are_not_accepted(self):
        record = finding(signal(), "DemoWorks Engineer Python")
        record.evidence_status = "source_matched"
        record.sources[0].evidence_status = "source_matched"
        self.assertTrue(accepted_finding(record))
        record.data["required_reviews"] = ["source_meaning"]
        self.assertFalse(accepted_finding(record))
        record.data["interpretation_review"] = {"supported": True, "status": "accepted"}
        self.assertFalse(accepted_finding(record))
        record.data["interpretation_review"]["source_interpretation"] = {
            "source_subject": record.data["company"],
            "source_subject_kind": "company",
            "source_signal": record.data["signal"],
            "source_scope": record.data["scope"],
        }
        self.assertTrue(accepted_finding(record))
        record.data["interpretation_review"].pop("status")
        self.assertFalse(accepted_finding(record))
        record.data["interpretation_review"]["status"] = "accepted"
        record.data["required_reviews"].append("proposal_metadata")
        self.assertFalse(accepted_finding(record))
        record.data["proposal_review"] = {"status": "accepted"}
        self.assertTrue(accepted_finding(record))

    def test_delivery_method_is_not_a_specific_tool(self):
        for name in (
            "CI/CD",
            "ci cd",
            "Continuous Integration",
            "continuous delivery",
            "continuous deployment",
        ):
            with self.subTest(name=name), self.assertRaises(ValueError):
                validate_specific_technology_name(name)
        for name in ("GitLab CI", "GitHub Actions", "Jenkins"):
            self.assertEqual(validate_specific_technology_name(name), name)

    def test_overview_cannot_use_legal_name_as_relationship_evidence(self):
        html = "Target India Private Ltd"
        fact = source_finding(
            "company_profile",
            {"company": html, "field": "legal_name", "value": html, "evidence": [html]},
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        overview = {
            "site_description": None,
            "company_name": None,
            "company_description": None,
            "products_services": [],
            "industries": [],
            "certifications_compliance": [],
            "company_relationships": [
                {
                    "text": "Target owns Target India Private Ltd",
                    "record_ids": [fact.record_id],
                }
            ],
        }
        with self.assertRaisesRegex(ValueError, "accepted fact of that type"):
            validate_summary(overview, {fact.record_id: fact})


class ProposalBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_category_cannot_enter_technology_submission(self):

        html = "<h1>DemoWorks Engineer</h1><p>Our team uses Yocto.</p>"
        record = finding(
            signal(
                technology="Yocto",
                evidence=["DemoWorks", "Engineer", "Our team uses Yocto."],
            ),
            html,
        )
        record.data["catalog_match"] = proposal()
        record.data["catalog_match"]["proposed_technology"]["category_suggestion"] = (
            "Electronic design automation / RF circuit simulation"
        )
        with TemporaryDirectory() as directory:
            root = Path(directory)
            duplicate = record.model_copy(deep=True)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200,
                        json=response(
                            {
                                "reviews": [
                                    {
                                        "record_id": record.record_id,
                                        "identity_supported": True,
                                        "description_supported": True,
                                        "category_supported": False,
                                        "reason": "Embedded Linux build tools are not RF circuit simulators",
                                    }
                                ]
                            }
                        ),
                    )
                ),
            ) as client:
                await review_proposals(
                    [record, duplicate],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "category",
                    catalog_fixture().category_options(),
                )
        self.assertEqual(record.data["proposal_review"]["status"], "rejected")
        self.assertEqual(
            duplicate.data["proposal_review"], record.data["proposal_review"]
        )
        self.assertFalse(accepted_finding(duplicate))
        self.assertEqual(technology_submission_records([record]), [])
        record.evidence_status = "source_matched"
        self.assertEqual(technology_submission_records([record]), [])

    def test_duplicate_unreviewed_source_cannot_resurrect_a_rejected_claim(self):

        html = "<p>Target India Private Ltd</p>"
        original = source_finding(
            "company_profile",
            {
                "company": "Target",
                "field": "legal_name",
                "value": "Target India Private Ltd",
                "evidence": ["Target India Private Ltd"],
            },
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        original.evidence_status = "needs_review"
        original.data["interpretation_review"] = {
            "supported": False,
            "status": "rejected",
        }
        duplicate = original.model_copy(deep=True)
        duplicate.data.pop("interpretation_review")
        duplicate.evidence_status = "source_matched"
        duplicate.sources[0].page_id = "p0002"
        records = [original]
        merge_finding(records, duplicate)
        self.assertEqual(original.evidence_status, "needs_review")
        self.assertFalse(original.data["interpretation_review"]["supported"])
        self.assertEqual(len(original.sources), 2)
        for review_key in ("interpretation_review", "proposal_review"):
            for reverse in (False, True):
                with self.subTest(review=review_key, reverse=reverse):
                    rejected = original.model_copy(deep=True)
                    rejected.data = {
                        review_key: {"status": "rejected", "supported": False}
                    }
                    unreviewed = duplicate.model_copy(deep=True)
                    unreviewed.data = {}
                    values = (
                        [unreviewed, rejected] if reverse else [rejected, unreviewed]
                    )
                    merged = [values[0]]
                    merge_finding(merged, values[1])
                    self.assertEqual(merged[0].data[review_key]["status"], "rejected")
                    self.assertFalse(accepted_finding(merged[0]))
