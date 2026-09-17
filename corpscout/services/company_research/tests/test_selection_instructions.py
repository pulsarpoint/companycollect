"""Custom requests control navigation without extracting company records."""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from pydantic import ValidationError
from test_crawl import browser_responses
from test_package import assessment, response
from test_site_gate import classification

from company_research.crawl import crawl_company, main
from company_research.models import RequestedContentAssessment, ResearchConfig
from company_research.prompts import selection_prompt


def requested_assessment(
    candidate: dict, role: str, *, target="target", follow="single_page"
) -> dict:
    return {
        "candidate_id": candidate["candidate_id"],
        "target_relevance": target,
        "follow_scope": follow,
        "requested_content": {
            "potential": "low" if role == "none" else "high",
            "role": role,
        },
        "reason": "Link metadata contributes to the requested content"
        if role != "none"
        else "Outside the requested content",
    }


class SelectionInstructionTests(unittest.IsolatedAsyncioTestCase):
    async def test_external_page_limit_is_reported_as_a_budget_not_no_matching_pages(
        self,
    ):
        with TemporaryDirectory() as temporary:
            with (
                patch(
                    "company_research.crawl.open_browser",
                    side_effect=AssertionError("External budget is zero"),
                ),
                patch.object(
                    httpx.AsyncClient,
                    "send",
                    side_effect=AssertionError("No eligible candidates to rank"),
                ),
            ):
                result = await crawl_company(
                    "https://example.test/",
                    pages=["https://hiring.test/openings/42"],
                    instructions="Get job descriptions",
                    output_dir=Path(temporary),
                    api_key="test-key",
                    config=ResearchConfig(
                        model="deepseek-flash", provider=None, max_external_pages=0
                    ),
                )
            self.assertEqual(result["stop_reason"], "external_page_budget")
            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["pages"], [])

    async def test_custom_instructions_follow_opaque_careers_and_external_board_links(
        self,
    ):
        instructions = "Collect current job listings and full job descriptions. Skip employee stories and other employers."
        site = "https://example.test/"
        gateway = site + "sr/mogucnosti"
        board = "https://hiring.test/employers/Example"
        opening = "https://hiring.test/openings/42"
        requested, selection_calls = [], []
        pages = {
            site: (
                "<h1>Example</h1><p>Industrial robot design</p>",
                [
                    {"href": gateway, "text": "Otvorene pozicije"},
                    {"href": site + "products", "text": "Products"},
                    {"href": site + "stories", "text": "Employee stories"},
                ],
                200,
                None,
            ),
            gateway: (
                "<h1>Example careers</h1>",
                [{"href": board, "text": "Example current openings"}],
                200,
                None,
            ),
            board: (
                "<h1>Example vacancies</h1>",
                [
                    {"href": opening, "text": "Example: Embedded Engineer"},
                    {
                        "href": "https://hiring.test/all-employers",
                        "text": "All employers",
                    },
                ],
                200,
                None,
            ),
            opening: (
                "<h1>Example: Embedded Engineer</h1><p>Full job description</p>",
                [],
                200,
                None,
            ),
        }

        async def model_boundary(client, request, **kwargs):
            self.assertEqual(request.url.host, "api.deepseek.com")
            body = json.loads(request.content)
            prompt = next(m["content"] for m in body["messages"] if m["role"] == "user")
            payload = json.loads(prompt.split("INPUT DATA:\n")[1])
            if payload.get("task") == "site_classification":
                result = classification(
                    crawl_decision="continue_crawling",
                    site_description="Example offers industrial robot design.",
                    site_types=["company"],
                    research_profiles=["service_provider"],
                    purpose="Industrial robot design",
                    operator_name="Example",
                    business_activities=["Industrial robot design"],
                    evidence=["Example", "Industrial robot design"],
                )
            else:
                self.assertEqual(payload["selection_instructions"], instructions)
                self.assertNotIn("OBJECTIVES:", prompt)
                self.assertNotIn("coverage", payload)
                selection_calls.append(payload)
                result = {
                    "assessments": [
                        requested_assessment(
                            candidate,
                            "navigation"
                            if candidate["url"] in {gateway, board}
                            else "direct"
                            if candidate["url"] == opening
                            else "none",
                            target="unrelated"
                            if candidate["url"].endswith("all-employers")
                            else "target_evidence"
                            if candidate["external"]
                            else "target",
                            follow="target_navigation"
                            if candidate["url"] == board
                            else "single_page",
                        )
                        for candidate in payload["candidates"]
                    ]
                }
            return httpx.Response(200, json=response(result))

        with TemporaryDirectory() as temporary:
            with (
                patch(
                    "company_research.crawl.open_browser",
                    lambda: browser_responses(pages, requested),
                ),
                patch.object(httpx.AsyncClient, "send", model_boundary),
            ):
                manifest = await crawl_company(
                    site,
                    instructions=instructions,
                    output_dir=Path(temporary),
                    api_key="test-key",
                    config=ResearchConfig(
                        model="deepseek-flash", provider=None, max_sitemap_urls=0
                    ),
                )
            self.assertEqual(requested, [site, gateway, board, opening])
            self.assertGreaterEqual(len(selection_calls), 3)
            self.assertEqual(manifest["selection_instructions"], instructions)
            self.assertEqual(manifest["status"], "finished")
            self.assertEqual(
                manifest["pages"][-1]["selection"]["requested_content"]["role"],
                "direct",
            )
            self.assertTrue(
                all(
                    Path(temporary, p["html_file"]).is_file() for p in manifest["pages"]
                )
            )

    async def test_same_page_list_can_answer_different_instructions_without_leaving_list(
        self,
    ):
        site = "https://example.test/"
        for instructions, selected_path in (
            ("Get jobs pages", "/a"),
            ("Get office contact details", "/b"),
        ):
            with (
                self.subTest(instructions=instructions),
                TemporaryDirectory() as temporary,
            ):
                requested = []
                pages = {
                    site.rstrip("/") + selected_path: (
                        "<h1>Example</h1>",
                        [{"href": site + "unlisted", "text": "More relevant content"}],
                        200,
                        None,
                    )
                }

                async def model_boundary(
                    client,
                    request,
                    *,
                    instructions=instructions,
                    selected_path=selected_path,
                    **kwargs,
                ):
                    prompt = json.loads(request.content)["messages"][1]["content"]
                    data = json.loads(prompt.split("INPUT DATA:\n")[1])
                    self.assertEqual(data["selection_instructions"], instructions)
                    self.assertNotIn("task", data)
                    return httpx.Response(
                        200,
                        json=response(
                            {
                                "assessments": [
                                    requested_assessment(
                                        c,
                                        "direct"
                                        if c["url"].endswith(selected_path)
                                        else "none",
                                    )
                                    for c in data["candidates"]
                                ]
                            }
                        ),
                    )

                with (
                    patch(
                        "company_research.crawl.open_browser",
                        lambda pages=pages, requested=requested: browser_responses(
                            pages, requested
                        ),
                    ),
                    patch.object(httpx.AsyncClient, "send", model_boundary),
                ):
                    manifest = await crawl_company(
                        site,
                        pages=["/a", "/b"],
                        instructions=instructions,
                        output_dir=Path(temporary),
                        api_key="test-key",
                        config=ResearchConfig(
                            model="deepseek-flash", provider=None, max_pages=1
                        ),
                    )
                self.assertEqual(requested, [site.rstrip("/") + selected_path])
                self.assertEqual(manifest["site_gate"]["reason"], "supplied_pages")
                self.assertFalse((Path(temporary) / "sitemaps.json").exists())
                queue = json.loads(
                    (Path(temporary) / "queue.json").read_text(encoding="utf-8")
                )
                self.assertEqual(queue["allowed_urls"], [site + "a", site + "b"])
                self.assertNotIn(
                    site + "unlisted", [c["url"] for c in queue["candidates"]]
                )

    async def test_missing_custom_assessments_never_fall_back_to_broad_research(self):
        for valid in (False, True):
            with self.subTest(valid=valid), TemporaryDirectory() as temporary:

                async def model_boundary(client, request, *, valid=valid, **kwargs):
                    data = json.loads(
                        json.loads(request.content)["messages"][1]["content"].split(
                            "INPUT DATA:\n"
                        )[1]
                    )
                    values = [
                        requested_assessment(c, "none")
                        if valid
                        else assessment(
                            c["candidate_id"], products_services="high"
                        ).model_dump()
                        for c in data["candidates"]
                    ]
                    return httpx.Response(200, json=response({"assessments": values}))

                with (
                    patch(
                        "company_research.crawl.open_browser",
                        side_effect=AssertionError(
                            "Do not fetch irrelevant or unassessed pages"
                        ),
                    ),
                    patch.object(httpx.AsyncClient, "send", model_boundary),
                ):
                    manifest = await crawl_company(
                        "https://example.test/",
                        pages=["/products"],
                        instructions="Get current job descriptions",
                        output_dir=Path(temporary),
                        api_key="test-key",
                        config=ResearchConfig(
                            model="deepseek-flash", provider=None, max_corrections=0
                        ),
                    )
                self.assertEqual(manifest["pages"], [])
                self.assertEqual(manifest["status"], "finished" if valid else "failed")
                self.assertEqual(bool(manifest["errors"]), not valid)


class CompactSelectionTests(unittest.TestCase):
    def test_compact_contract_rejects_broad_fields_and_missing_scope(self):
        value = requested_assessment({"candidate_id": "c1"}, "navigation")
        RequestedContentAssessment.model_validate(value)
        for changes in (
            {"objectives": assessment("c1").objectives.model_dump()},
            {"requested_content": {"potential": "high", "role": "none"}},
            {"requested_content": {"potential": "low", "role": "navigation"}},
            {"reason": "x" * 301},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                RequestedContentAssessment.model_validate(value | changes)
        for field in ("target_relevance", "follow_scope"):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                RequestedContentAssessment.model_validate(
                    {key: item for key, item in value.items() if key != field}
                )

    def test_custom_schema_omits_objectives_while_general_prompt_keeps_them(self):
        custom = selection_prompt("https://example.test/", [], instructions="Find jobs")
        schema = json.loads(
            custom.split("OUTPUT SCHEMA:\n")[1].split("\n\nINPUT DATA:")[0]
        )
        fields = schema["$defs"]["RequestedContentAssessment"]["properties"]
        self.assertEqual(
            set(fields),
            {
                "candidate_id",
                "requested_content",
                "target_relevance",
                "follow_scope",
                "reason",
            },
        )
        self.assertNotIn("OBJECTIVES:", custom)
        general = selection_prompt("https://example.test/", [])
        self.assertIn("OBJECTIVES:", general)
        self.assertIn("ObjectivePotentials", general)


class PageListCliTests(unittest.TestCase):
    def test_cli_reads_custom_instructions_file_and_selects_within_page_list(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            instructions = "Get full job descriptions.\nSkip office contacts."
            instruction_file = root / "instructions.txt"
            instruction_file.write_text(instructions, encoding="utf-8")
            requested = []
            pages = {
                "https://example.test/jobs/42": ("<h1>Engineer</h1>", [], 200, None)
            }

            async def model_boundary(client, request, **kwargs):
                prompt = json.loads(request.content)["messages"][1]["content"]
                data = json.loads(prompt.split("INPUT DATA:\n")[1])
                self.assertEqual(data["selection_instructions"], instructions)
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "assessments": [
                                requested_assessment(
                                    c, "direct" if c["url"].endswith("42") else "none"
                                )
                                for c in data["candidates"]
                            ]
                        }
                    ),
                )

            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "company-research-crawl",
                        "https://example.test/",
                        "--pages",
                        "/contacts",
                        "/jobs/42",
                        "--instructions-file",
                        str(instruction_file),
                        "--output-dir",
                        str(root / "crawl"),
                    ],
                ),
                patch(
                    "company_research.crawl.open_browser",
                    lambda: browser_responses(pages, requested),
                ),
                patch.object(httpx.AsyncClient, "send", model_boundary),
                patch.dict("os.environ", {"DEEPSEEK": "test-key"}),
                redirect_stdout(stdout),
                redirect_stderr(io.StringIO()),
            ):
                main()
            result = json.loads(stdout.getvalue())
            self.assertEqual(requested, ["https://example.test/jobs/42"])
            self.assertEqual(result["selection_instructions"], instructions)
            self.assertEqual(result["status"], "finished")

    def test_cli_accepts_multiple_urls_and_emits_one_json_object_without_llm(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            requested = []
            pages = {
                f"https://example.test/{name}": (f"<h1>{name}</h1>", [], 200, None)
                for name in ("a", "b", "c")
            }
            stdout = io.StringIO()
            with (
                patch(
                    "sys.argv",
                    [
                        "company-research-crawl",
                        "https://example.test/",
                        "--pages",
                        "/a",
                        "/b",
                        "--page",
                        "/c",
                        "--output-dir",
                        str(root),
                    ],
                ),
                patch(
                    "company_research.crawl.open_browser",
                    lambda: browser_responses(pages, requested),
                ),
                patch.object(
                    httpx.AsyncClient,
                    "send",
                    side_effect=AssertionError("No model call for explicit pages"),
                ),
                patch.dict("os.environ", {}, clear=True),
                redirect_stdout(stdout),
                redirect_stderr(io.StringIO()),
            ):
                main()
            manifest = json.loads(stdout.getvalue())
            self.assertEqual(requested, list(pages))
            self.assertEqual(manifest["usage"]["calls"], 0)
            self.assertEqual(len(manifest["pages"]), 3)
