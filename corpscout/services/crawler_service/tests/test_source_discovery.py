"""Discovery contracts at the model/browser boundaries and source-scope limits."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import httpx
from test_crawl import browser_responses
from test_package import response
from test_site_info import COMPANY, HTML, SITE

from crawler_service.crawl import crawl_company, select_next_page
from crawler_service.discovery import CrawlQueue
from crawler_service.link_selection import assess_links
from crawler_service.llm import ModelClient
from crawler_service.models import RequestedContentAssessment, ResearchConfig
from crawler_service.page_observations import collect_page_observations
from crawler_service.web_search import brave_results, discover_search_sources

PARENT = "https://parent.test/"
PARENT_TEXT = "Our parent company Parent Ltd publishes subsidiary reports."
REPORTS = PARENT + "investor/subsidiaries"
REPORT_HTML = """<html><body><main><h1>Financial statements of subsidiaries</h1>
<div class="row"><span>Example Ltd consolidated financials</span><div><a href="/files/abc.pdf">Download</a></div></div>
<div class="row"><span>Example India Ltd financial statements</span><div><a href="/files/xyz.pdf">Download</a></div></div>
</main></body></html>"""


def assessment(candidate, *, kind="parent_company", evidence=PARENT_TEXT, **changes):
    return RequestedContentAssessment.model_validate(
        {
            "candidate_id": candidate.candidate_id,
            "requested_content": {"potential": "high", "role": "navigation"},
            "target_relevance": "related_company",
            "follow_scope": "source_navigation",
            "navigation_source": {"kind": kind, "evidence": evidence} if kind else None,
            "reason": "Navigate the source to find the target's filings",
        }
        | changes
    )


def parent_queue(**limits):
    queue = CrawlQueue(
        SITE,
        ResearchConfig(max_external_pages=20, **limits),
        instructions="Find financial reports",
    )
    queue.add(
        PARENT,
        source=SITE,
        context={"source_url": SITE, "surrounding_text": PARENT_TEXT},
    )
    candidate = queue.candidates[PARENT]
    candidate.assessment = assessment(candidate)
    candidate.assessed = True
    return queue


class SourceNavigationTests(unittest.TestCase):
    def test_search_candidates_are_assessed_before_internal_archive_backlog(self):
        queue = parent_queue(selection_batch_size=2)
        for i in range(20):
            queue.add(SITE + f"news/{i}", source=SITE, label="Company archive")
        queue.add(
            REPORTS,
            source="https://search.brave.com/search",
            from_search=True,
            context={
                "extraction_method": "web_search",
                "surrounding_text": "Target financial reports",
            },
        )
        self.assertIn(REPORTS, [c.url for c in queue.assessment_batch()])

    def test_parent_route_reaches_documents_and_retains_context(self):
        queue = parent_queue()
        self.assertEqual(queue.pick_for_instructions()[0].url, PARENT)
        queue.visited.add(PARENT)
        queue.add(
            PARENT,
            source=PARENT,
            label="Parent home",
            context={"source_url": PARENT, "surrounding_text": "Home"},
        )
        self.assertIsNotNone(queue.candidates[PARENT].assessment)
        queue.redirects[PARENT + "home"] = PARENT
        queue.add(
            REPORTS, source=PARENT + "home", label="Subsidiary financial statements"
        )
        candidate = queue.candidates[REPORTS]
        self.assertEqual(candidate.navigation_root, PARENT)
        self.assertEqual(candidate.navigation_depth, 1)
        candidate.assessment = assessment(candidate, kind=None)
        self.assertEqual(queue.pick_for_instructions()[0].url, REPORTS)
        queue.add(
            PARENT + "files/abc.pdf",
            source=REPORTS,
            label="Download",
            context={"surrounding_text": "Example Ltd consolidated financials"},
        )
        reference = queue.document_candidates[PARENT + "files/abc.pdf"]
        self.assertFalse(reference["content_examined"])
        self.assertEqual(reference["link_contexts"][0]["source_url"], REPORTS)
        self.assertIn("Example Ltd", reference["link_contexts"][0]["surrounding_text"])
        self.assertNotIn(PARENT + "files/abc.pdf", queue.candidates)
        queue.add("https://unrelated.test/investors", source=REPORTS)
        self.assertNotIn("https://unrelated.test/investors", queue.candidates)

    def test_parent_requires_source_evidence_not_search_snippet_or_model_claim(self):
        for source, quote in [
            (SITE, "Invented ownership relationship"),
            ("https://search.brave.com/search", PARENT_TEXT),
        ]:
            queue = parent_queue()
            candidate = queue.candidates[PARENT]
            candidate.link_contexts[0]["source_url"] = source
            candidate.assessment = assessment(candidate, evidence=quote)
            self.assertIsNotNone(
                queue.navigation_source_error(candidate, candidate.assessment)
            )
            self.assertIsNone(queue.pick_for_instructions())
        queue = parent_queue()
        candidate = queue.candidates[PARENT]
        candidate.assessment = assessment(
            candidate, follow_scope="single_page", navigation_source=None
        )
        self.assertIsNone(queue.pick_for_instructions())

    def test_source_domain_page_and_depth_limits(self):
        for limits in [
            {"max_source_domains": 0},
            {"max_source_pages_per_domain": 1},
            {"max_source_depth": 0},
        ]:
            with self.subTest(limits=limits):
                queue = parent_queue(**limits)
                chosen = queue.pick_for_instructions()
                if "max_source_domains" in limits:
                    self.assertIsNone(chosen)
                    continue
                queue.visited.add(PARENT)
                queue.add(REPORTS, source=PARENT)
                if REPORTS in queue.candidates:
                    candidate = queue.candidates[REPORTS]
                    candidate.assessment = assessment(candidate, kind=None)
                self.assertIsNone(queue.pick_for_instructions())

    def test_direct_subsidiary_index_wins_remaining_source_budget(self):
        queue = parent_queue(max_source_pages_per_domain=2)
        queue.pick_for_instructions()
        queue.visited.add(PARENT)
        annual_reports = PARENT + "investor/annual-reports"
        for url, priority in [(annual_reports, 60), (REPORTS, 95)]:
            queue.add(url, source=PARENT)
            candidate = queue.candidates[url]
            candidate.assessment = assessment(candidate, kind=None, priority=priority)
        selected = queue.pick_for_instructions()[0]
        self.assertEqual(selected.url, REPORTS)
        queue.visited.add(selected.url)
        self.assertIsNone(queue.pick_for_instructions())
        self.assertEqual(queue.source_budget_reason(), "source_page_budget")
        queue.candidates[annual_reports].assessment = None
        self.assertIsNone(queue.source_budget_reason())

    def test_source_limit_does_not_claim_matching_candidates_are_exhausted(self):
        queue = parent_queue(max_source_domains=0)
        self.assertIsNone(queue.pick_for_instructions())
        self.assertEqual(queue.source_budget_reason(), "source_domain_budget")
        queue.candidates[PARENT].assessment = assessment(
            queue.candidates[PARENT], evidence="Unsupported ownership claim"
        )
        self.assertIsNone(queue.source_budget_reason())

    def test_registry_source_requires_context_and_stays_separate_from_target(self):
        queue = parent_queue(max_source_domains=1)
        candidate = queue.candidates[PARENT]
        candidate.assessment = assessment(
            candidate,
            kind="filing_source",
            evidence="publishes subsidiary reports.",
            target_relevance="unknown",
        )
        self.assertIsNotNone(queue.pick_for_instructions())
        queue.visited.add(PARENT)
        other = "https://registry.test/"
        queue.add(
            other,
            source=SITE,
            context={
                "source_url": SITE,
                "surrounding_text": "Registry publishes company filings",
            },
        )
        candidate = queue.candidates[other]
        candidate.assessment = assessment(
            candidate,
            kind="filing_source",
            evidence="Registry publishes company filings",
            target_relevance="unknown",
        )
        self.assertIsNone(queue.pick_for_instructions())
        self.assertEqual(
            queue.navigation_sources["parent.test"]["basis"],
            "source_matched_navigation_hypothesis",
        )

    def test_financial_download_labels_are_scoped_to_each_document(self):
        observations = collect_page_observations(
            REPORT_HTML,
            page_id="p1",
            source_url=REPORTS,
            representation="rendered_html",
        )
        links = observations["financial_links"]
        self.assertEqual(len(links), 2)
        self.assertEqual(links[0]["basis"], "surrounding_context")
        self.assertIn(
            "Example Ltd consolidated", links[0]["context"]["surrounding_text"]
        )
        self.assertNotIn("India", links[0]["context"]["surrounding_text"])
        self.assertIn("India", links[1]["context"]["surrounding_text"])
        self.assertEqual(links[0]["source_url"], REPORTS)

    def test_document_page_title_preserves_entity_label_without_relabeling_navigation(
        self,
    ):
        observations = collect_page_observations(
            "<html><head><title>Financial Information</title></head><body>"
            '<div><span>Example Ltd</span><a href="/files/opaque.pdf">Download</a></div>'
            '<a href="/products">Products</a></body></html>',
            page_id="p1",
            source_url=REPORTS,
            representation="rendered_html",
        )
        self.assertEqual(len(observations["financial_links"]), 1)
        link = observations["financial_links"][0]
        self.assertEqual(link["basis"], "document_page_title")
        self.assertEqual(link["context"]["surrounding_text"], "Example Ltd Download")

    def test_search_document_reference_is_retained_without_becoming_a_fetch(self):
        queue = parent_queue()
        url = "https://filings.test/opaque.pdf"
        queue.add(
            url,
            source="https://search.brave.com/search?q=example",
            from_search=True,
            label="Example financial statements",
            context={
                "extraction_method": "web_search",
                "search_id": "s0001",
                "query": "Example financial statements",
            },
        )
        self.assertNotIn(url, queue.candidates)
        self.assertEqual(
            queue.document_candidates[url]["link_contexts"][0]["search_id"], "s0001"
        )


SEARCH_HTML = f"""<div id="llm-snippet"><a href="https://wrong.test"><div class="search-snippet-title">Generated answer</div></a></div>
<div class="snippet" data-type="web"><a href="{REPORTS}"><div class="search-snippet-title">Example subsidiary financial statements</div></a><div class="generic-snippet">Financial statements and subsidiary filings for Example Ltd.</div></div>
<div class="snippet" data-type="ad"><a href="https://ads.test"><div class="search-snippet-title">Sponsored</div></a></div>"""


class WebSearchTests(unittest.IsolatedAsyncioTestCase):
    async def test_child_selection_receives_previous_source_approval(self):
        queue = parent_queue()
        queue.pick_for_instructions()
        queue.visited.add(PARENT)
        queue.add(REPORTS, source=PARENT)

        def boundary(request):
            prompt = json.loads(request.content)["messages"][1]["content"]
            data = json.loads(prompt.split("INPUT DATA:\n")[1])
            self.assertEqual(
                data["navigation_sources"]["parent.test"]["evidence"]["evidence"],
                PARENT_TEXT,
            )
            self.assertEqual(data["candidates"][0]["navigation_root"], PARENT)
            return httpx.Response(
                200,
                json=response(
                    {
                        "assessments": [
                            assessment(queue.candidates[REPORTS], kind=None).model_dump()
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as temporary:
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(boundary),
            ) as http:
                model = ModelClient(
                    http, "key", queue.config, Path(temporary), api="deepseek"
                )
                errors = await assess_links(
                    queue, model, Path(temporary), reserved_calls=0
                )
            self.assertEqual(errors, [])
            self.assertEqual(queue.pick_for_instructions()[0].url, REPORTS)

    async def test_selection_reports_source_limit_in_manifest(self):
        with TemporaryDirectory() as temporary:
            queue = parent_queue(max_source_domains=0)
            queue.config.max_model_calls = 1
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda _: self.fail("Already assessed candidates need no model call")
                )
            ) as http:
                model = ModelClient(http, "key", queue.config, Path(temporary))
                manifest = {"errors": [], "stop_reason": None}
                selected = await select_next_page(queue, model, Path(temporary), manifest)
            self.assertIsNone(selected)
            self.assertEqual(manifest["stop_reason"], "source_domain_budget")
            self.assertEqual(manifest["errors"], [])

    async def test_followup_search_uses_bounded_captured_source_text(self):
        queue = parent_queue(web_search=True, search_context_chars=1000)
        queue.site_profile = {"operator_name": "Example"}
        prompts = []

        def boundary(request):
            prompt = json.loads(request.content)["messages"][1]["content"]
            prompts.append(json.loads(prompt.split("INPUT DATA:\n")[1]))
            return httpx.Response(200, json=response({"queries": []}))

        with TemporaryDirectory() as directory:
            root = Path(directory)
            pages = []
            for index in range(4):
                folder = root / f"p{index}"
                folder.mkdir()
                (folder / "input.json").write_text(
                    json.dumps(
                        {
                            "observations": {
                                "metadata": {"title": "Company disclosure"},
                                "text": {
                                    "main": "Our parent is Parent Ltd. " * 100,
                                    "visible": "",
                                },
                            }
                        }
                    )
                )
                pages.append(
                    {
                        "snapshot": folder.name,
                        "source_url": SITE + str(index),
                        "fetch_status": "fetched",
                    }
                )
            manifest = {
                "pages": pages,
                "web_search": {
                    "phases": [],
                    "queries": [],
                    "errors": [],
                    "status": "pending",
                },
            }
            async with (
                httpx.AsyncClient(
                    transport=httpx.MockTransport(boundary),
                    base_url="https://api.deepseek.com/",
                ) as http,
                browser_responses({}, []) as browser,
            ):
                llm = ModelClient(http, "key", queue.config, root, api="deepseek")
                await discover_search_sources(
                    browser, queue, llm, root, manifest, phase="followup"
                )
                await discover_search_sources(
                    browser, queue, llm, root, manifest, phase="followup"
                )
            self.assertEqual(len(prompts), 1)
            excerpts = prompts[0]["captured_source_excerpts"]
            self.assertEqual(
                [e["source_url"] for e in excerpts],
                [SITE + str(i) for i in range(1, 4)],
            )
            self.assertLessEqual(sum(len(e["source_text"]) for e in excerpts), 1000)
            self.assertIn("Our parent is Parent Ltd", excerpts[-1]["source_text"])
            self.assertEqual(prompts[0]["max_new_queries"], 1)

    async def test_site_info_alone_never_launches_search(self):
        with TemporaryDirectory() as directory:
            requested = []

            async def boundary(client, request, **kwargs):
                self.assertEqual(request.url.host, "api.deepseek.com")
                return httpx.Response(200, json=response(COMPANY))

            with (
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda: browser_responses({SITE: (HTML, [], 200, None)}, requested),
                ),
                patch.object(httpx.AsyncClient, "send", boundary),
            ):
                result = await crawl_company(
                    SITE,
                    output_dir=Path(directory),
                    site_info=True,
                    api_key="key",
                    config=ResearchConfig(web_search=True),
                )
            self.assertEqual(result["web_search"]["status"], "not_requested")
            self.assertEqual(result["usage"]["calls"], 1)
            self.assertEqual(requested, [SITE])

    async def test_selection_correction_explains_invalid_navigation_role(self):
        queue = parent_queue(max_model_calls=2)
        candidate = queue.candidates[PARENT]
        candidate.assessed = False
        candidate.assessment = None
        calls = []

        def boundary(request):
            prompt = json.loads(request.content)["messages"][1]["content"]
            calls.append(prompt)
            value = assessment(candidate).model_dump()
            if len(calls) == 1:
                value["requested_content"]["role"] = "direct"
            else:
                self.assertIn(
                    "Source navigation requires useful navigation potential", prompt
                )
            return httpx.Response(200, json=response({"assessments": [value]}))

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(boundary),
                base_url="https://api.deepseek.com/",
            ) as http:
                llm = ModelClient(http, "key", queue.config, root, api="deepseek")
                errors = await assess_links(queue, llm, root, reserved_calls=0)
            self.assertEqual(len(calls), 2)
            self.assertEqual(errors, [])
            self.assertEqual(queue.pick_for_instructions()[0].url, PARENT)

    def test_parser_ignores_generated_answers_and_advertising(self):
        results = brave_results(SEARCH_HTML, 5)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["url"], REPORTS)
        self.assertIn("subsidiary filings", results[0]["snippet"])

    async def test_full_search_receipts_and_documents_survive_json_only_output(self):
        for failed in (False, True):
            with self.subTest(failed=failed), TemporaryDirectory() as temporary:
                requested, tasks = [], []
                query_url = "https://search.brave.com/search?q=%22Example%22+financial+statements&source=web"
                pages = {
                    SITE: (HTML, [], 200, None),
                    query_url: (SEARCH_HTML, [], 429 if failed else 200, None),
                    REPORTS: (REPORT_HTML, [], 200, None),
                }

                async def boundary(client, request, tasks=tasks, **kwargs):
                    if request.url.host != "api.deepseek.com":
                        return httpx.Response(404)
                    prompt = json.loads(request.content)["messages"][1]["content"]
                    data = json.loads(prompt.split("INPUT DATA:\n")[1])
                    if data.get("task") == "site_classification":
                        tasks.append("gate")
                        value = COMPANY
                    elif "max_new_queries" in data:
                        tasks.append("search")
                        value = {
                            "queries": [
                                {
                                    "terms": "financial statements",
                                    "reason": "Locate official financial sources",
                                }
                            ]
                        }
                    else:
                        tasks.append("selection")
                        value = {
                            "assessments": [
                                {
                                    "candidate_id": c["candidate_id"],
                                    "requested_content": {
                                        "potential": "high",
                                        "role": "direct",
                                    },
                                    "target_relevance": "target_evidence",
                                    "follow_scope": "single_page",
                                    "reason": "The search result names the target's reports",
                                }
                                for c in data["candidates"]
                            ]
                        }
                    return httpx.Response(200, json=response(value))

                root = Path(temporary)
                with (
                    patch(
                        "crawler_service.crawl.open_browser",
                        lambda pages=pages, requested=requested: browser_responses(
                            pages, requested
                        ),
                    ),
                    patch.object(httpx.AsyncClient, "send", boundary),
                ):
                    manifest = await crawl_company(
                        SITE,
                        output_dir=root,
                        crawl="full",
                        save_artifacts=False,
                        api_key="key",
                        config=ResearchConfig(max_search_queries=1),
                    )
                result = json.loads((root / "result.json").read_text())
                self.assertEqual(list(root.iterdir()), [root / "result.json"])
                self.assertEqual(
                    manifest["status"], "failed" if failed else "finished"
                )
                receipt = result["crawl"]["web_search"]["queries"][0]
                self.assertEqual(
                    parse_qs(urlsplit(receipt["search_url"]).query)["q"],
                    ['"Example" financial statements'],
                )
                self.assertEqual(receipt["status"], "blocked" if failed else "complete")
                self.assertEqual(
                    tasks,
                    ["gate", "search"] if failed else ["gate", "search", "selection"],
                )
                documents = result["crawl"]["discovery"]["document_links"]
                if failed:
                    self.assertEqual(len(documents), 0)
                    self.assertEqual(requested, [SITE, query_url])
                else:
                    self.assertEqual(len(documents), 2)
                    self.assertIn(
                        "Example Ltd",
                        documents[0]["link_contexts"][0]["surrounding_text"],
                    )
                    self.assertEqual(requested, [SITE, query_url, REPORTS])
                    self.assertEqual(len(result["documents"]), 2)

    async def test_explicit_page_list_never_launches_search(self):
        with TemporaryDirectory() as temporary:
            requested = []
            with (
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda: browser_responses({SITE: (HTML, [], 200, None)}, requested),
                ),
                patch.object(
                    httpx.AsyncClient,
                    "send",
                    side_effect=AssertionError("No model or search allowed"),
                ),
            ):
                result = await crawl_company(
                    SITE,
                    output_dir=Path(temporary),
                    pages=[SITE],
                    config=ResearchConfig(web_search=True),
                )
            self.assertEqual(result["web_search"]["status"], "not_requested")
            self.assertEqual(requested, [SITE])
