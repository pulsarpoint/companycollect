"""URL provenance, useful-source coverage and recovery of saved page extractions."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture
from test_package import assessment, page, response
from test_technologies import signal

from company_research.content import HtmlWindow, source_finding, split_html
from company_research.discovery import CrawlQueue
from company_research.llm import ModelClient
from company_research.models import (
    OBJECTIVES,
    RECORD_TYPES,
    Findings,
    ResearchConfig,
    ResearchResult,
)
from company_research.research import (
    extract_saved_page,
    set_job_detail_context,
    update_statuses,
)
from company_research.review import review_claims


class PostingURLTests(unittest.TestCase):
    def test_primary_title_gets_observed_url_without_changing_related_jobs(self):
        url = "https://target.example/job/old-title-123"
        html = '<h1>Senior Engineer</h1><p>DemoWorks: Our team develops tools in Python.</p><h2>Related Engineer</h2><a href="/job/related-456">Related Engineer</a>'
        source = page(html, url)
        queue = CrawlQueue("https://target.example/", ResearchConfig())
        queue.add(url, source=queue.site_url)
        queue.candidates[url].job_record_ids = ["observed-listing"]
        set_job_detail_context(source, queue, html)
        for title, supplied, expected in (
            ("Senior Engineer", "/job/invented-title-123", url),
            ("Senior Engineer", None, url),
            (
                "Related Engineer",
                "/job/related-456",
                "https://target.example/job/related-456",
            ),
        ):
            with self.subTest(title=title, supplied=supplied):
                record = source_finding(
                    "technology_signals",
                    signal(
                        job_title=title,
                        job_url=supplied,
                        evidence=[
                            "DemoWorks",
                            title,
                            "Our team develops tools in Python.",
                        ],
                    ),
                    page=source.model_dump(),
                    window=HtmlWindow(0, len(html), html),
                )
                self.assertEqual(record.data["job_url"], expected)
                self.assertEqual(record.evidence_status, "source_matched")
                self.assertEqual(
                    "job_url_binding" in record.data, title == "Senior Engineer"
                )

    def test_listing_redirect_and_ambiguous_heading_do_not_authorize_binding(self):
        url = "https://target.example/job/engineer"
        html = "<h1>Engineer</h1>"
        source = page(html, url)
        queue = CrawlQueue("https://target.example/", ResearchConfig())
        queue.add(url, source=queue.site_url)
        set_job_detail_context(source, queue, html)
        self.assertIsNone(source.job_detail)
        queue.candidates[url].job_record_ids = ["observed-listing"]
        source.source_url = "https://target.example/careers"
        set_job_detail_context(source, queue, html)
        self.assertIsNone(source.job_detail)
        source.source_url = url
        set_job_detail_context(source, queue, html + "<h1>Another job</h1>")
        self.assertIsNone(source.job_detail)


class CoverageTests(unittest.TestCase):
    def test_medium_direct_engineering_is_reserved_even_after_technology_found(self):
        queue = CrawlQueue(
            "https://target.example/", ResearchConfig(engineering_page_reserve=1)
        )
        for path, kind, potential in (
            ("/mechanical-engineering", "service_detail", "medium"),
            ("/news/new-development", "news", "high"),
        ):
            queue.add(path, source=queue.site_url)
            candidate = queue.candidates[queue.site_url.rstrip("/") + path]
            candidate.assessment = assessment(
                candidate.candidate_id,
                technology_signals=potential,
                products_services="high",
            )
            candidate.assessment.page_kind = kind
            candidate.assessment.target_relevance = "target"
        selected, focus = queue.pick(dict.fromkeys(OBJECTIVES, 10))
        self.assertEqual(selected.url, "https://target.example/mechanical-engineering")
        self.assertEqual(focus, "engineering_followup")
        self.assertEqual(len(queue.engineering_attempts), 1)

    def test_news_without_new_records_loses_priority_to_direct_medium_source(self):
        queue = CrawlQueue(
            "https://target.example/", ResearchConfig(engineering_page_reserve=0)
        )
        queue.objective_order = ["technology_signals"]
        for path, kind, potential in (
            ("/news/old", "news", "high"),
            ("/news/new", "news", "high"),
            ("/design", "service_detail", "medium"),
        ):
            queue.add(path, source=queue.site_url)
            candidate = queue.candidates[queue.site_url.rstrip("/") + path]
            candidate.assessment = assessment(
                candidate.candidate_id, technology_signals=potential
            )
            candidate.assessment.page_kind = kind
        old = page("<h1>News</h1>", "https://target.example/news/old")
        old.extraction_status = "complete"
        old.objectives_examined = ["technology_signals"]
        queue.visited.add(old.source_url)
        queue.observe_page_yield(old, Findings(**{o: [] for o in RECORD_TYPES}))
        selected, _ = queue.pick(dict.fromkeys(OBJECTIVES, 1))
        self.assertEqual(selected.url, "https://target.example/design")

    def test_failed_partial_and_unexamined_pages_do_not_penalize_future_news(self):
        queue = CrawlQueue("https://target.example/", ResearchConfig())
        queue.add("/news/example", source=queue.site_url)
        candidate = queue.candidates["https://target.example/news/example"]
        source = page("<h1>News</h1>", candidate.url)
        source.objectives_examined = ["technology_signals"]
        for status in ("not_assessed", "failed", "partial", "complete"):
            with self.subTest(status=status):
                source.extraction_status = status
                queue.observe_page_yield(
                    source, Findings(**{objective: [] for objective in RECORD_TYPES})
                )
                self.assertEqual(
                    queue.repetition_penalty(candidate, "technology_signals"),
                    int(status == "complete"),
                )
                self.assertEqual(queue.repetition_penalty(candidate, "jobs"), 0)


class SavedExtractionTests(unittest.IsolatedAsyncioTestCase):
    async def test_review_accepts_separate_standard_version_but_not_changed_numbers(
        self,
    ):
        html = "Target obtained ISO 9001:2015 certification."
        for reconstructed, expected in (
            ("ISO 9001:2015", True),
            ("ISO 9001", True),
            ("ISO 9001:2008", False),
            ("ISO 14001:2015", False),
        ):
            with (
                self.subTest(reconstructed=reconstructed),
                TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                record = source_finding(
                    "certifications_compliance",
                    {
                        "subject_name": "Target",
                        "subject_kind": "company",
                        "standard_name": "ISO 9001",
                        "standard_version": "2015",
                        "claim_type": "certification",
                        "evidence": [html],
                    },
                    page=page(html).model_dump(),
                    window=HtmlWindow(0, len(html), html),
                )
                review = {
                    "record_id": "r1",
                    "supported": True,
                    "reason": "Explicit certificate statement",
                    "source_subject": "Target",
                    "source_subject_kind": "company",
                    "source_value": reconstructed,
                    "source_claim_type": "certification",
                    "source_object": None,
                    "specific_technology": None,
                    "source_signal": None,
                }
                async with httpx.AsyncClient(
                    base_url="https://test.invalid/",
                    transport=httpx.MockTransport(
                        lambda _, review=review: httpx.Response(
                            200, json=response({"reviews": [review]})
                        )
                    ),
                ) as client:
                    await review_claims(
                        [record],
                        ModelClient(
                            client, "test", ResearchConfig(max_review_attempts=1), root
                        ),
                        root,
                        "standard-version",
                    )
                self.assertEqual(record.evidence_status == "source_matched", expected)

    async def test_short_review_ids_expand_exactly_and_unknown_ids_fail(self):
        html = "Target was founded in 2000."
        for returned_id in ("r1", "r999"):
            with (
                self.subTest(returned_id=returned_id),
                TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                record = source_finding(
                    "company_profile",
                    {
                        "company": "Target",
                        "field": "founded",
                        "value": "2000",
                        "evidence": [html],
                    },
                    page=page(html).model_dump(),
                    window=HtmlWindow(0, len(html), html),
                )

                def handle(request, returned_id=returned_id):
                    prompt = json.loads(request.content)["messages"][-1]["content"]
                    claims = json.loads(prompt.split("INPUT DATA:\n")[-1])["claims"]
                    self.assertEqual(claims[0]["record_id"], "r1")
                    return httpx.Response(
                        200,
                        json=response(
                            {
                                "reviews": [
                                    {
                                        "record_id": returned_id,
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

                async with httpx.AsyncClient(
                    base_url="https://test.invalid/",
                    transport=httpx.MockTransport(handle),
                ) as client:
                    await review_claims(
                        [record],
                        ModelClient(
                            client, "test", ResearchConfig(max_review_attempts=1), root
                        ),
                        root,
                        "alias-test",
                    )
                self.assertEqual(
                    record.evidence_status == "source_matched", returned_id == "r1"
                )
                if returned_id == "r1":
                    review = json.loads(
                        (root / "reviews/alias-test-0.json").read_text()
                    )
                    self.assertEqual(
                        review["reviews"][0]["record_id"], record.record_id
                    )

    async def test_retry_uses_saved_html_and_does_not_repeat_completed_chunks(self):
        html = (
            "<section>"
            + "Alpha " * 170
            + "</section><section>"
            + "Beta " * 200
            + "</section>"
        )
        config = ResearchConfig(
            chunk_chars=1200, overlap_chars=0, max_http_attempts=1, max_corrections=0
        )
        self.assertEqual(len(split_html(html, max_chars=1200, overlap_chars=0)), 2)
        source = page(html)
        requests = []

        def handle(request):
            requests.append(json.loads(request.content))
            if len(requests) == 2:
                return httpx.Response(503)
            return httpx.Response(200, json=response({o: [] for o in RECORD_TYPES}))

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "html").mkdir()
            (root / source.html_file).write_text(html, encoding="utf-8")
            result = ResearchResult(
                schema_version="1.7",
                run_id="test",
                technology_catalog=None,
                input_url=source.source_url,
                site_url=source.source_url,
                started_at=source.fetched_at,
                finished_at=None,
                status="running",
                stop_reason=None,
                config=config,
                objectives={},
                records=Findings(**{o: [] for o in RECORD_TYPES}),
                technology_summary=[],
                site_profile=None,
                company_overview=None,
                pages=[source],
                discovery={},
                usage={},
                errors=[],
                output_directory=directory,
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", config, root)
                await extract_saved_page(result, source, llm, root, catalog_fixture())
                await extract_saved_page(result, source, llm, root, catalog_fixture())
            self.assertEqual(len(requests), 3)
            self.assertEqual(source.attempts, 1)
            self.assertEqual(source.extraction_status, "complete")
            self.assertEqual(
                [
                    (a.chunk_index, a.attempt, a.status)
                    for a in source.extraction_attempts
                ],
                [(0, 1, "complete"), (1, 1, "retry_pending"), (1, 2, "complete")],
            )
            self.assertEqual(
                len(list((root / "extraction-attempts").glob("*.json"))), 3
            )
            self.assertEqual(
                (root / source.html_file).read_text(encoding="utf-8"), html
            )

    async def test_retry_budget_preserves_pending_work_for_later_saved_replay(self):
        html = "<h1>Company</h1>"
        source = page(html)
        config = ResearchConfig(
            max_http_attempts=1, max_corrections=0, max_saved_extraction_retries=0
        )
        calls = []

        def handle(request):
            calls.append(request)
            return (
                httpx.Response(503)
                if len(calls) == 1
                else httpx.Response(200, json=response({o: [] for o in RECORD_TYPES}))
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "html").mkdir()
            (root / source.html_file).write_text(html, encoding="utf-8")
            result = ResearchResult(
                schema_version="1.7",
                run_id="test",
                technology_catalog=None,
                input_url=source.source_url,
                site_url=source.source_url,
                started_at=source.fetched_at,
                finished_at=None,
                status="running",
                stop_reason=None,
                config=config,
                objectives={},
                records=Findings(**{o: [] for o in RECORD_TYPES}),
                technology_summary=[],
                site_profile=None,
                company_overview=None,
                pages=[source],
                discovery={},
                usage={},
                errors=[],
                output_directory=directory,
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", config, root)
                await extract_saved_page(result, source, llm, root, catalog_fixture())
                update_statuses(result, CrawlQueue(source.source_url, config), llm)
                self.assertEqual(len(calls), 1)
                self.assertEqual(len(result.discovery["pending_extractions"]), 1)
                source.chunks_planned = 2
                update_statuses(result, CrawlQueue(source.source_url, config), llm)
                self.assertEqual(
                    result.discovery["pending_extractions"][1],
                    {
                        "page_id": source.page_id,
                        "chunk_index": 1,
                        "attempts": 0,
                        "status": "not_attempted",
                    },
                )
                result.config.max_saved_extraction_retries = 1
                await extract_saved_page(result, source, llm, root, catalog_fixture())
                self.assertEqual(source.extraction_status, "complete")
                (root / source.html_file).write_text("changed source", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    await extract_saved_page(
                        result, source, llm, root, catalog_fixture()
                    )
                self.assertEqual(len(calls), 2)
