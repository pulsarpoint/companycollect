import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from pydantic import ValidationError
from test_package import assessment, page, person, response
from test_technologies import finding, signal

from crawler_service.analytics import summarize_technologies
from crawler_service.content import HtmlWindow, merge_finding, source_finding
from crawler_service.discovery import CrawlQueue, normalize_url
from crawler_service.llm import ModelClient, parse_model_json
from crawler_service.models import (
    OBJECTIVES,
    RECORD_TYPES,
    DocumentLink,
    Relationship,
    ResearchConfig,
)
from crawler_service.research import assess_links, extract_window
from crawler_service.review import correct_reviewed_claims, review_claims


class DiscoveryRecoveryTests(unittest.TestCase):
    def test_found_job_does_not_starve_careers_navigation(self):
        queue = CrawlQueue("https://example.test/", ResearchConfig())
        for route, objective, role in (
            ("careers", "jobs", "navigation"),
            ("job/engineer", "jobs", "direct"),
            ("team", "people", "direct"),
        ):
            queue.add(f"/{route}", source=queue.site_url, label=route)
            candidate = queue.candidates[f"https://example.test/{route}"]
            candidate.assessment = assessment(
                candidate.candidate_id, **{objective: "high"}
            )
            getattr(candidate.assessment.objectives, objective).role = role
        queue.focus_visits.update(dict.fromkeys(OBJECTIVES, 3))
        queue.focus_visits["jobs"] = 1
        counts = dict.fromkeys(OBJECTIVES, 1)
        counts["people"] = 0
        chosen = queue.pick(counts)
        self.assertIsNotNone(chosen)
        assert chosen is not None
        self.assertEqual(chosen[0].url, "https://example.test/careers")

    def test_composite_role_from_html_line_break_matches_without_reconstructed_quote(
        self,
    ):
        html = "<h1>DemoWorks</h1><h2>Ada Example</h2><p>Chief Executive Officer <br> Co-Founder</p>"
        data = person() | {
            "company": "DemoWorks",
            "role": "Chief Executive Officer, Co-Founder",
            "evidence": [
                "DemoWorks",
                "Ada Example",
                "Chief Executive Officer <br> Co-Founder",
            ],
        }
        result = source_finding(
            "people",
            data,
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(result.evidence_status, "source_matched")

    def test_large_navigation_cannot_hide_careers_and_contact_from_first_batch(self):
        queue = CrawlQueue(
            "https://example.test/", ResearchConfig(selection_batch_size=10)
        )
        for index in range(80):
            queue.add(
                f"/a-product-{index}", source=queue.site_url, label=f"Product {index}"
            )
        for route in ("careers", "contact", "about", "quality", "investors"):
            queue.add(f"/{route}", source=queue.site_url, label=route.title())
        urls = {c.url for c in queue.assessment_batch()}
        self.assertIn("https://example.test/careers", urls)
        self.assertIn("https://example.test/contact", urls)
        self.assertIn("https://example.test/investors", urls)

    def test_failed_careers_assessment_can_be_explored_before_more_known_service_pages(
        self,
    ):
        queue = CrawlQueue("https://example.test/", ResearchConfig())
        queue.add("/careers", source=queue.site_url, label="Careers")
        careers = queue.candidates["https://example.test/careers"]
        careers.assessment_attempts = 1
        queue.add("/another-service", source=queue.site_url, label="Another service")
        service = queue.candidates["https://example.test/another-service"]
        service.assessment = assessment(service.candidate_id, products_services="high")
        counts = dict.fromkeys(OBJECTIVES, 1)
        counts["jobs"] = 0
        selected = queue.pick(counts)
        assert selected is not None
        self.assertEqual(selected[0].url, careers.url)
        self.assertEqual(selected[1], "exploration:jobs")
        self.assertEqual(queue.explored, 1)

    def test_social_feeds_are_not_crawled_but_individual_job_urls_remain_distinct(self):
        queue = CrawlQueue("https://example.test/", ResearchConfig())
        for url in (
            "https://www.linkedin.com/company/example/",
            "https://rs.linkedin.com/company/example/?trk=feed",
            "https://www.facebook.com/example",
        ):
            queue.add(url, source=queue.site_url)
        self.assertEqual(len(queue.candidates), 0)
        for url in (
            "https://rs.linkedin.com/jobs/view/123?trk=footer",
            "https://www.linkedin.com/jobs/view/123?trackingId=abc",
            "https://www.linkedin.com/jobs/view/456",
        ):
            queue.add(url, source=queue.site_url)
        self.assertEqual(len(queue.candidates), 2)
        self.assertNotEqual(
            normalize_url("https://board.test/jobs?id=123"),
            normalize_url("https://board.test/jobs?id=456"),
        )

    def test_document_inventory_preserves_links_outside_the_html_queue(self):
        queue = CrawlQueue("https://example.test/", ResearchConfig())
        queue.add(
            "https://cdn.example.test/accounts.PDF?download=1",
            source="https://example.test/investors",
            label="Annual accounts",
        )
        queue.add(
            "https://cdn.example.test/accounts.PDF?download=1",
            source="https://example.test/about",
            label="Accounts",
        )
        self.assertFalse(queue.candidates)
        document = queue.snapshot()["document_candidates"][0]
        self.assertEqual(len(document["source_urls"]), 2)
        self.assertFalse(document["content_examined"])
        self.assertNotIn("reporting_period", document)


class AttributionTests(unittest.TestCase):
    def test_only_identical_repeated_json_can_be_unwrapped(self):
        self.assertEqual(parse_model_json('{"x":1}{"x":1}'), ({"x": 1}, True))
        with self.assertRaises(ValueError):
            parse_model_json('{"x":1}{"x":2}')

    def test_document_metadata_is_grounded_in_link_context_without_claiming_content(
        self,
    ):
        html = '<p>DemoWorks accounts for year ended 31 March 2025: <a href="/reports/2026.pdf">Financial statements</a></p>'
        record = DocumentLink(
            document_url="/reports/2026.pdf",
            label="Financial statements",
            document_type="financial_statement",
            company="DemoWorks",
            reporting_period="year ended 31 March 2025",
            evidence=[
                "DemoWorks accounts for year ended 31 March 2025:",
                "Financial statements",
            ],
        ).model_dump()
        source = page(html).model_dump()
        window = HtmlWindow(0, len(html), html)
        result = source_finding("document_links", record, page=source, window=window)
        self.assertEqual(result.evidence_status, "source_matched")
        self.assertFalse(result.data["content_examined"])
        self.assertEqual(
            result.data["document_url"], "https://example.test/reports/2026.pdf"
        )
        fabricated = source_finding(
            "document_links",
            record | {"reporting_period": "2026"},
            page=source,
            window=window,
        )
        self.assertEqual(fabricated.evidence_status, "needs_review")

    def test_ownership_percentage_requires_evidence_and_cannot_attach_to_partnership(
        self,
    ):
        html = "<p>In 2025 Example Holdings owns 54% of DemoWorks directly.</p>"
        record = Relationship(
            subject="Example Holdings",
            object="DemoWorks",
            relationship="shareholder_of",
            subject_kind="company",
            object_kind="company",
            ownership_percentage=54,
            ownership_scope="direct",
            as_of="2025",
            evidence=["In 2025 Example Holdings owns 54% of DemoWorks directly."],
        ).model_dump()
        window = HtmlWindow(0, len(html), html)
        accepted = source_finding(
            "company_relationships", record, page=page(html).model_dump(), window=window
        )
        self.assertEqual(accepted.evidence_status, "source_matched")
        wrong = source_finding(
            "company_relationships",
            record | {"ownership_percentage": 100},
            page=page(html).model_dump(),
            window=window,
        )
        self.assertIn("ownership_percentage_not_in_evidence", wrong.sources[0].issues)
        with self.assertRaises(ValidationError):
            Relationship.model_validate(record | {"relationship": "partner_of"})

    def test_company_switchboard_cannot_be_assigned_to_person_without_attribution_evidence(
        self,
    ):
        html = "<p>DemoWorks switchboard: +45 1234.</p>"
        record = {
            "owner": "Ada Example",
            "owner_kind": "person",
            "type": "phone",
            "value": "+45 1234",
            "purpose": None,
            "evidence": ["DemoWorks switchboard: +45 1234."],
        }
        result = source_finding(
            "company_contacts",
            record,
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertIn("owner_not_in_evidence", result.sources[0].issues)

    def test_phone_value_and_named_owner_use_independent_evidence_checks(self):
        html = "<h2>Peter Grabe</h2><p>Mobil: +46 (0)70 559 11 67</p>"
        record = {
            "owner": "Peter Grabe",
            "owner_kind": "person",
            "type": "phone",
            "value": "+46 (0)70 559 11 67",
            "purpose": "Mobil",
            "evidence": ["Peter Grabe", "Mobil: +46 (0)70 559 11 67"],
        }
        source = page(html).model_dump()
        window = HtmlWindow(0, len(html), html)
        result = source_finding("company_contacts", record, page=source, window=window)
        self.assertEqual(result.evidence_status, "source_matched")
        self.assertEqual(result.data["owner"], "Peter Grabe")
        self.assertEqual(result.data["value"], record["value"])
        self.assertEqual(
            [fragment.text for fragment in result.sources[0].evidence],
            record["evidence"],
        )

        unquoted_owner = source_finding(
            "company_contacts",
            record | {"evidence": ["Mobil: +46 (0)70 559 11 67"]},
            page=source,
            window=window,
        )
        self.assertIn("owner_not_in_evidence", unquoted_owner.sources[0].issues)

        wrong_number = source_finding(
            "company_contacts",
            record | {"value": "+46 8 999 99 99"},
            page=source,
            window=window,
        )
        self.assertEqual(wrong_number.evidence_status, "needs_review")
        self.assertIn("value_not_in_evidence", wrong_number.sources[0].issues)
        self.assertNotIn("owner_not_in_evidence", wrong_number.sources[0].issues)

    def test_digits_in_phone_do_not_establish_a_company_owner(self):
        html = "<p>DemoWorks switchboard: +45 1234.</p>"
        record = {
            "owner": "Studio 12",
            "owner_kind": "company",
            "type": "phone",
            "value": "+45 1234",
            "purpose": None,
            "evidence": ["DemoWorks switchboard: +45 1234."],
        }
        result = source_finding(
            "company_contacts",
            record,
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(result.evidence_status, "needs_review")
        self.assertEqual(result.sources[0].issues, ["owner_not_in_evidence"])

    def test_catalog_failure_does_not_enter_technology_summary(self):
        html = "<h1>DemoWorks Engineer</h1><p>Our team develops tools in Python.</p>"
        record = finding(signal(), html)
        record.data["catalog_error"] = "Search failed"
        self.assertEqual(summarize_technologies([record]), [])


class RecoveryHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_signal_correction_keeps_original_and_rechecks_new_claim(self):
        html = "<h1>DemoWorks Engineer</h1><p>Python is an advantage</p>"
        original = finding(
            signal(
                context="Python is an advantage",
                evidence=["DemoWorks", "Engineer", "Python is an advantage"],
            ),
            html,
        )
        original.evidence_status = "needs_review"
        original.data["interpretation_review"] = {
            "supported": False,
            "source_interpretation": {
                "specific_technology": True,
                "source_signal": "preferred_experience",
                "source_subject": None,
                "source_object": None,
            },
        }
        requests = []

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n", 1
                )[1]
            )
            requests.append(data)
            self.assertEqual(
                data["claims"][0]["data"]["signal"], "preferred_experience"
            )
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": data["claims"][0]["record_id"],
                                "supported": True,
                                "reason": "Optional candidate experience",
                                "source_subject": "DemoWorks",
                                "source_subject_kind": "company",
                                "source_scope": "team",
                                "source_object": None,
                                "specific_technology": True,
                                "source_signal": "preferred_experience",
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "html").mkdir()
            (root / "html/p0001.html").write_text(html)
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(client, "test-key", ResearchConfig(), root)
                corrected = await correct_reviewed_claims([original], llm, root, "test")
                self.assertEqual(
                    await correct_reviewed_claims(
                        [original, *corrected], llm, root, "again"
                    ),
                    [],
                )
        self.assertEqual(len(requests), 1)
        self.assertEqual(original.data["signal"], "stated_use")
        self.assertEqual(original.evidence_status, "needs_review")
        self.assertEqual(corrected[0].data["signal"], "preferred_experience")
        self.assertEqual(corrected[0].data["correction_of"], original.record_id)
        self.assertEqual(corrected[0].evidence_status, "source_matched")

    async def test_structured_review_checks_direction_and_allows_symmetric_partnership(
        self,
    ):
        for predicate, expected in (
            ("customer_of", "needs_review"),
            ("partner_of", "source_matched"),
        ):
            with self.subTest(predicate=predicate), TemporaryDirectory() as directory:
                html = "<h1>DemoWorks</h1><p>DemoWorks client Acme</p><p>Acme DemoWorks partner</p>"
                data = {
                    "subject": "DemoWorks",
                    "object": "Acme",
                    "relationship": predicate,
                    "subject_kind": "company",
                    "object_kind": "company",
                    "ownership_percentage": None,
                    "ownership_scope": None,
                    "as_of": None,
                    "evidence": ["DemoWorks client Acme", "Acme DemoWorks partner"],
                }
                record = source_finding(
                    "company_relationships",
                    data,
                    page=page(html).model_dump(),
                    window=HtmlWindow(0, len(html), html),
                )
                document = {
                    "reviews": [
                        {
                            "record_id": record.record_id,
                            "supported": True,
                            "reason": "Acme is the client/partner of DemoWorks",
                            "source_subject": "Acme",
                            "source_object": "DemoWorks",
                            "specific_technology": None,
                            "source_signal": None,
                        }
                    ]
                }
                async with httpx.AsyncClient(
                    base_url="https://openrouter.test/v1/",
                    transport=httpx.MockTransport(
                        lambda _, document=document: httpx.Response(
                            200, json=response(document)
                        )
                    ),
                ) as client:
                    await review_claims(
                        [record],
                        ModelClient(
                            client, "test-key", ResearchConfig(), Path(directory)
                        ),
                        Path(directory),
                        "direction",
                    )
                self.assertEqual(record.evidence_status, expected)

    async def test_semantic_rejection_prevents_source_matched_claim_entering_summary(
        self,
    ):
        html = "<h1>DemoWorks Engineer</h1><p>Our team develops tools in Python.</p>"
        record = finding(signal(signal="explicitly_not_used"), html)
        self.assertEqual(record.evidence_status, "source_matched")

        def handle(request):
            payload = json.loads(request.content)
            self.assertIn(
                "explicitly_not_used needs explicit denial",
                payload["messages"][1]["content"],
            )
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": record.record_id,
                                "supported": False,
                                "reason": "The source states usage, not non-use.",
                                "source_subject": None,
                                "source_object": None,
                                "specific_technology": True,
                                "source_signal": "stated_use",
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(client, "test-key", ResearchConfig(), Path(directory))
                issues = await review_claims([record], llm, Path(directory), "test")
        self.assertTrue(issues)
        self.assertEqual(record.sources[0].evidence_status, "source_matched")
        self.assertEqual(record.evidence_status, "needs_review")
        self.assertEqual(summarize_technologies([record]), [])

    async def test_unavailable_review_is_not_accepted_as_support(self):
        html = "<h1>DemoWorks Engineer</h1><p>Our team develops tools in Python.</p>"
        record = finding(signal(), html)
        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(lambda _: httpx.Response(503)),
            ) as client:
                llm = ModelClient(
                    client,
                    "test-key",
                    ResearchConfig(max_http_attempts=1),
                    Path(directory),
                )
                await review_claims([record], llm, Path(directory), "unavailable")
        self.assertEqual(record.evidence_status, "needs_review")
        self.assertFalse(record.data["interpretation_review"]["supported"])

    async def test_empty_json_with_published_contact_is_not_reported_complete(self):
        html = '<a href="mailto:hello@example.test">Email our team</a>'
        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(
                        200, json=response(dict.fromkeys(RECORD_TYPES, []))
                    )
                ),
            ) as client:
                llm = ModelClient(client, "test-key", ResearchConfig(), Path(directory))
                _, complete, errors, assessed = await extract_window(
                    HtmlWindow(0, len(html), html), page(html), llm, Path(directory), 0
                )
                self.assertEqual(len(llm.calls), 2)
        self.assertFalse(complete)
        self.assertFalse(assessed)
        self.assertTrue(any("Empty extraction" in error for error in errors))

    async def test_failed_link_assessment_is_retried_and_recovers(self):
        with TemporaryDirectory() as directory:
            config = ResearchConfig(max_http_attempts=1, max_model_calls=10)
            queue = CrawlQueue("https://example.test/", config)
            queue.add("/careers", source=queue.site_url, label="Careers")
            candidate = next(iter(queue.candidates.values()))
            requests = []

            def handle(request):
                requests.append(request)
                if len(requests) == 1:
                    return httpx.Response(503)
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "assessments": [
                                assessment(
                                    candidate.candidate_id, jobs="high"
                                ).model_dump()
                            ]
                        }
                    ),
                )

            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(client, "test-key", config, Path(directory))
                await assess_links(queue, llm, Path(directory))
                self.assertFalse(candidate.assessed)
                self.assertEqual(queue.snapshot()["assessment_failed_count"], 1)
                await assess_links(queue, llm, Path(directory))
                self.assertTrue(candidate.assessed)
                self.assertIsNotNone(candidate.assessment)
                self.assertEqual(len(requests), 2)

    async def test_evidence_repair_preserves_separated_source_fragments(self):
        html = "<h2>Group President &amp; CEO</h2><p>File title: Ada Example</p>"
        requests = []

        def handle(request):
            requests.append(json.loads(request.content))
            record = person()
            if len(requests) == 1:
                record["evidence"] = ["Group President & CEO Ada Example"]
            else:
                data = json.loads(
                    requests[-1]["messages"][1]["content"].split("INPUT DATA:\n", 1)[1]
                )
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "repairs": [
                                {
                                    "record_id": data["records"][0]["record_id"],
                                    "evidence": person()["evidence"],
                                }
                            ]
                        }
                    ),
                )
            return httpx.Response(
                200,
                json=response(dict.fromkeys(RECORD_TYPES, []) | {"people": [record]}),
            )

        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client,
                    "test-key",
                    ResearchConfig(reasoning_effort="none"),
                    Path(directory),
                )
                findings, complete, errors, _ = await extract_window(
                    HtmlWindow(0, len(html), html), page(html), llm, Path(directory), 0
                )
        records = []
        for _, record in findings:
            merge_finding(records, record)
        self.assertTrue(complete, errors)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].evidence_status, "source_matched")
        self.assertEqual(len(records[0].sources), 2)
        self.assertEqual(requests[0]["reasoning"], {"enabled": False})
        self.assertIn("evidence_fragment_absent", requests[1]["messages"][1]["content"])
