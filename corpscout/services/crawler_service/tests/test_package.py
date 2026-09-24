import gzip
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from crawler_service.content import (
    HtmlWindow,
    merge_finding,
    source_finding,
    split_html,
)
from crawler_service.discovery import CrawlQueue, normalize_url, sitemap_urls
from crawler_service.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from crawler_service.models import (
    OBJECTIVES,
    RECORD_TYPES,
    CandidateAssessment,
    Findings,
    Page,
    ResearchConfig,
    ResearchResult,
)
from crawler_service.research import assess_links, extract_window, update_statuses
from crawler_service.storage import content_hash


def page(html: str, source_url: str = "https://example.test/company") -> Page:
    return Page(
        page_id="p0001",
        requested_url=source_url,
        source_url=source_url,
        selected_for="people",
        fetched_at="2026-09-06T00:00:00Z",
        status_code=200,
        fetch_status="fetched",
        extraction_status="not_assessed",
        objectives_examined=[],
        attempts=1,
        chunks_planned=1,
        chunks_completed=0,
        html_sha256=content_hash(html),
        html_file="html/p0001.html",
        errors=[],
    )


def person() -> dict:
    return {
        "name": "Ada Example",
        "role": "Group President & CEO",
        "company": "Example",
        "profile_url": None,
        "as_of": None,
        "evidence": ["Group President & CEO", "Ada Example"],
    }


def response(document: object) -> dict:
    if isinstance(document, dict) and isinstance(document.get("checks"), list):
        for check in document["checks"]:
            check.setdefault("source_attribution", None)
            if check.get("correction") is not None:
                check["correction"].setdefault("attribution", None)
                check["correction"].setdefault("evidence", None)
    if isinstance(document, dict) and isinstance(document.get("reviews"), list):
        document = dict(document) | {
            "reviews": [
                {
                    "identity_basis": None,
                    "source_subject_kind": None,
                    "source_value": None,
                    "source_claim_type": None,
                    "source_scope": None,
                }
                | value
                if "specific_technology" in value
                else value
                for value in document["reviews"]
            ]
        }
    return {
        "choices": [
            {"finish_reason": "stop", "message": {"content": json.dumps(document)}}
        ],
        "usage": {"prompt_tokens": 10, "completion_tokens": 20, "cost": 0.001},
        "provider": "test",
    }


def assessment(candidate_id: str, **potentials) -> CandidateAssessment:
    return CandidateAssessment.model_validate(
        {
            "candidate_id": candidate_id,
            "reason": "Test page metadata",
            "target_relevance": "target_evidence",
            "follow_scope": "single_page",
            "objectives": {
                o: {
                    "potential": potentials.get(o, "low"),
                    "role": "direct" if o in potentials else "none",
                }
                for o in OBJECTIVES
            },
        }
    )


class ContentTests(unittest.TestCase):
    def test_intervening_labels_do_not_discard_person(self):
        html = "<h2>Group President &amp; CEO</h2><p>File title: Ada Example</p>"
        finding = source_finding(
            "people",
            person(),
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(finding.evidence_status, "source_matched")
        self.assertEqual(finding.sources[0].url, "https://example.test/company")

    def test_fake_joined_quote_and_absent_url_are_reviewable(self):
        html = "<h2>Group President &amp; CEO</h2><p>File title: Ada Example</p>"
        record = person() | {
            "profile_url": "https://example.test/invented",
            "evidence": ["Group President & CEO Ada Example"],
        }
        finding = source_finding(
            "people",
            record,
            page=page(html).model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(finding.evidence_status, "needs_review")
        self.assertIn("profile_url_absent", finding.sources[0].issues)
        self.assertEqual(finding.data["name"], "Ada Example")

    def test_jobs_allow_intervening_dates_and_resolve_redirect_base(self):
        html = '<h1>Jobs at Example</h1><a href="/jobs/123">Engineer <b>5 Sep 2026</b> Aarhus, DK +1 more</a>'
        record = {
            "employer": "Example",
            "title": "Engineer",
            "location": "Aarhus, DK",
            "department": None,
            "employment_type": None,
            "workplace_type": None,
            "job_url": "/jobs/123",
            "evidence": ["Example", "Engineer", "5 Sep 2026 Aarhus, DK +1 more"],
        }
        finding = source_finding(
            "jobs",
            record,
            page=page(html, "https://careers.example.test/search").model_dump(),
            window=HtmlWindow(0, len(html), html),
        )
        self.assertEqual(finding.evidence_status, "source_matched")
        self.assertEqual(
            finding.data["job_url"], "https://careers.example.test/jobs/123"
        )

    def test_windows_cover_every_character_and_cap_giant_elements(self):
        html = (
            "<main>"
            + "<p>Text and facts</p>" * 300
            + '<a href="/job">'
            + "X" * 5000
            + "</a></main>"
        )
        windows = split_html(html, max_chars=1000, overlap_chars=200)
        reconstructed = ""
        for window in windows:
            self.assertLessEqual(len(window.content), 1000)
            self.assertEqual(window.content, html[window.start : window.end])
            reconstructed += html[max(len(reconstructed), window.start) : window.end]
        self.assertEqual(reconstructed, html)
        self.assertTrue(
            all(a.end > b.start for a, b in zip(windows, windows[1:], strict=False))
        )

    def test_merge_preserves_sources_and_does_not_collapse_case_sensitive_urls(self):
        html = '<a href="/Ada">Ada Example</a><a href="/ada">Ada Example</a><h2>Group President &amp; CEO</h2>'
        window = HtmlWindow(0, len(html), html)
        left = source_finding(
            "people",
            person() | {"profile_url": "/Ada"},
            page=page(html).model_dump(),
            window=window,
        )
        another_source = source_finding(
            "people",
            person() | {"profile_url": "/Ada"},
            page=page(html, "https://example.test/team").model_dump(),
            window=window,
        )
        different = source_finding(
            "people",
            person() | {"profile_url": "/ada"},
            page=page(html).model_dump(),
            window=window,
        )
        records = []
        for finding in [left, another_source, different]:
            merge_finding(records, finding)
        self.assertEqual(len(records), 2)
        self.assertEqual(len(records[0].sources), 2)


class QueueTests(unittest.TestCase):
    def test_new_link_metadata_reassesses_a_bare_sitemap_candidate(self):
        queue = CrawlQueue("https://example.test", ResearchConfig())
        queue.add("/page42", source=queue.site_url)
        candidate = next(iter(queue.candidates.values()))
        candidate.assessment = assessment(candidate.candidate_id)
        candidate.assessed = True
        self.assertFalse(queue.assessment_batch())
        queue.add("/page42", source=queue.site_url, label="Our management team")
        self.assertEqual(queue.assessment_batch(), [candidate])

    def test_undercovered_objectives_take_priority_but_found_collections_continue(self):
        queue = CrawlQueue("https://example.test", ResearchConfig())
        for path, objective in [
            ("/jobs/1", "jobs"),
            ("/jobs/2", "jobs"),
            ("/contact", "company_contacts"),
        ]:
            queue.add(path, source="https://example.test")
            candidate = queue.candidates[normalize_url(path, "https://example.test")]
            candidate.assessment = assessment(
                candidate.candidate_id, **{objective: "high"}
            )
        counts = dict.fromkeys(OBJECTIVES, 0)
        counts["jobs"] = 1
        selected = queue.pick(counts)
        assert selected is not None
        self.assertEqual(selected[1], "company_contacts")
        queue.visited.add(selected[0].url)
        next_selected = queue.pick(counts)
        assert next_selected is not None
        self.assertEqual(next_selected[1], "jobs")

    def test_external_budget_and_secondary_domains_do_not_expand_without_bound(self):
        queue = CrawlQueue("https://example.com", ResearchConfig(max_external_pages=1))
        queue.add("https://jobs.external.net/company", source="https://example.com")
        candidate = next(iter(queue.candidates.values()))
        candidate.assessment = assessment(candidate.candidate_id, jobs="high")
        self.assertIsNotNone(queue.pick(dict.fromkeys(OBJECTIVES, 0)))
        queue.visited.add(candidate.url)
        queue.add("https://another.org/unrelated", source=candidate.url)
        queue.add("https://jobs.external.net/company/job/1", source=candidate.url)
        self.assertEqual(queue.excluded["outside_approved_scope"], 2)
        self.assertIsNone(queue.pick(dict.fromkeys(OBJECTIVES, 0)))

    def test_missing_metadata_can_be_explored_and_login_is_excluded(self):
        queue = CrawlQueue("https://example.com", ResearchConfig())
        queue.add("/unknown", source="https://example.com")
        queue.add("/login", source="https://example.com")
        selected = queue.pick(dict.fromkeys(OBJECTIVES, 0))
        assert selected is not None
        self.assertTrue(selected[1].startswith("exploration"))
        self.assertEqual(len(queue.candidates), 1)


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_malformed_responses_are_saved_and_trip_bounded_failure_limit(self):
        replies = [
            {"choices": [None]},
            {"choices": [{"message": "wrong type", "finish_reason": "stop"}]},
            {
                "choices": [
                    {"message": {"content": "bad json"}, "finish_reason": "stop"}
                ]
            },
        ]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, json=replies.pop(0))
                ),
            ) as client:
                llm = ModelClient(client, "fake-key", ResearchConfig(), root)
                for index in range(1, 4):
                    reply = await llm.ask("prompt", {}, task="test")
                    self.assertIsNotNone(reply.error)
                    saved = json.loads(
                        (root / "calls" / f"{index:05}.json").read_text(
                            encoding="utf-8"
                        )
                    )
                    self.assertEqual(saved["error"], reply.error)
                with self.assertRaises(ModelUnavailable):
                    await llm.ask("prompt", {}, task="test")
                self.assertEqual(llm.usage()["calls"], 3)

    async def test_failed_link_assessments_remain_visible_and_explorable(self):
        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, json={"choices": []})
                ),
            ) as client:
                llm = ModelClient(client, "fake-key", ResearchConfig(), Path(directory))
                queue = CrawlQueue("https://example.test", llm.config)
                queue.add("/contact", source=queue.site_url)
                errors = await assess_links(queue, llm, Path(directory))
                self.assertEqual(errors[0]["stage"], "link_assessment")
                self.assertEqual(queue.snapshot()["assessment_failed_count"], 1)
                self.assertEqual(queue.snapshot()["unassessed_count"], 1)
                selected = queue.pick(dict.fromkeys(OBJECTIVES, 0))
                assert selected is not None
                self.assertTrue(selected[1].startswith("exploration"))

    async def test_compressed_sitemap_does_not_treat_image_locations_as_pages(self):
        body = '<urlset xmlns:image="urn:image"><url><loc>https://example.test/about</loc><image:image><image:loc>https://example.test/photo</image:loc></image:image></url></urlset>'

        def handle(request):
            return (
                httpx.Response(404)
                if request.url.path == "/robots.txt"
                else httpx.Response(200, content=gzip.compress(body.encode()))
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            found = await sitemap_urls(client, "https://example.test", ResearchConfig())
        self.assertEqual(found["urls"], ["https://example.test/about"])

    async def test_sitemap_index_failure_and_deduplication(self):
        def handle(request):
            bodies = {
                "/robots.txt": "Sitemap: https://example.test/sitemap.xml",
                "/sitemap.xml": '<sitemapindex xmlns="urn:sitemap"><sitemap><loc>https://example.test/child.xml</loc></sitemap></sitemapindex>',
                "/child.xml": '<urlset xmlns="urn:sitemap"><url><loc>https://example.test/about</loc></url><url><loc>https://example.test/about</loc></url></urlset>',
            }
            return httpx.Response(200, text=bodies[request.url.path])

        async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
            found = await sitemap_urls(
                client, "https://example.test/", ResearchConfig()
            )
        self.assertEqual(found["urls"], ["https://example.test/about"])
        self.assertEqual(len(found["files"]), 2)

    async def test_model_attempt_budget_records_http_errors_without_secrets(self):
        requests = []

        def handle(request):
            requests.append(request)
            return httpx.Response(503)

        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client,
                    "not-a-real-secret",
                    ResearchConfig(max_model_calls=1),
                    Path(directory),
                )
                with self.assertRaises(ModelBudgetExceeded):
                    await llm.ask("hello", {}, task="test")
                self.assertEqual(llm.usage()["calls"], 1)
                self.assertEqual(llm.usage()["unknown_cost_calls"], 1)
                saved = (Path(directory) / "calls/00001.json").read_text(
                    encoding="utf-8"
                )
                self.assertNotIn("not-a-real-secret", saved)
                self.assertNotIn("Authorization", saved)

    async def test_partial_extraction_survives_budget_exhaustion_and_marks_unassessed_objective(
        self,
    ):
        document = dict.fromkeys(RECORD_TYPES, []) | {
            "people": [person()],
            "jobs": "invalid array",
        }
        html = "<p>Group President &amp; CEO File title: Ada Example</p>"
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(
                    lambda _: httpx.Response(200, json=response(document))
                ),
            ) as client:
                llm = ModelClient(
                    client, "test-key", ResearchConfig(max_model_calls=1), root
                )
                finding_page = page(html)
                findings, complete, errors, assessed = await extract_window(
                    HtmlWindow(0, len(html), html), finding_page, llm, root, 0
                )
                self.assertFalse(complete)
                self.assertEqual(len(findings), 1)
                self.assertNotIn("jobs", assessed)
                finding_page.objectives_examined = [
                    o for o in OBJECTIVES if o in assessed
                ]
                finding_page.extraction_status = "partial"
                result = ResearchResult(
                    schema_version="1.4",
                    run_id="test-run",
                    technology_catalog=None,
                    input_url="https://example.test",
                    site_url="https://example.test",
                    started_at="2026-09-06",
                    finished_at=None,
                    status="running",
                    stop_reason=None,
                    config=llm.config,
                    objectives={},
                    records=Findings(**{o: [] for o in RECORD_TYPES}),
                    technology_summary=[],
                    site_profile=None,
                    company_overview=None,
                    pages=[finding_page],
                    discovery={},
                    usage={},
                    errors=[],
                    output_directory=directory,
                )
                merge_finding(result.records.people, findings[0][1])
                update_statuses(
                    result, CrawlQueue("https://example.test", llm.config), llm
                )
                self.assertEqual(result.objectives["jobs"].status, "not_assessed")
                self.assertEqual(result.objectives["people"].status, "found")
                self.assertIn("Model request budget reached", errors)


if __name__ == "__main__":
    unittest.main()
