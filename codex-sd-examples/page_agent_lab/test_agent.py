"""Exercise routing and extraction through the actual model HTTP boundary."""

import asyncio
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from crawler_service.llm import ModelClient
from crawler_service.models import OBJECTIVES, ResearchConfig
from crawler_service.storage import content_hash
from jsonschema import Draft202012Validator

from page_agent_lab.agent import (
    PageAgent,
    PageInput,
    dispatch_decisions,
    page_inventory,
    response_schema,
    validate_links,
    validate_records,
)

HTML = """<h1>Example Labs</h1><p>Team expertise in BuildTool.</p>
<p>Intervening source text.</p><p>Recruiter Ada Example</p>
<a href="mailto:ada@example.test">ada@example.test</a>
<a href="/jobs">Jobs</a><iframe src="https://archive.example.test/reports"></iframe>"""


def sample_page() -> PageInput:
    links, headings = page_inventory(
        HTML, "https://example.test/", html_kind="native_cleaned_html"
    )
    return PageInput(
        page={
            "page_id": "sample",
            "source_url": "https://example.test/",
            "html_file": "page.html",
            "html_sha256": content_hash(HTML),
            "fetched_at": "2026-09-16T00:00:00Z",
        },
        html=HTML,
        links=links,
        headings=headings,
        target_url="https://example.test/",
    )


def contact() -> dict:
    return {
        "owner": "Ada Example",
        "owner_kind": "person",
        "type": "email",
        "value": "ada@example.test",
        "purpose": "Recruitment",
        "evidence": ["Recruiter Ada Example", "ada@example.test"],
    }


def decisions() -> dict:
    return {
        key: {"decision": "skip", "reason": "No signal", "section_ids": []}
        for key in OBJECTIVES
    }


class ValidationTests(unittest.TestCase):
    def test_schemas_resolve_and_bad_records_do_not_discard_supported_fragments(self):
        for objectives, routing, links in [
            (list(OBJECTIVES), False, True),
            ([], True, True),
            (["company_contacts"], False, False),
        ]:
            Draft202012Validator.check_schema(
                response_schema(objectives, routing=routing, links=links)
            )
        value = validate_records(
            {"company_contacts": [contact(), {"value": "invalid"}]},
            ["company_contacts"],
            sample_page(),
        )
        self.assertEqual(len(value["records"]["company_contacts"]), 1)
        self.assertEqual(len(value["rejections"]), 1)
        self.assertEqual(
            value["records"]["company_contacts"][0]["evidence_status"], "source_matched"
        )
        self.assertEqual(value["coverage"]["company_contacts"]["status"], "partial")

    def test_inventory_preserves_occurrences_query_iframe_and_pagination(self):
        html = """<base href="https://other.example.test/base/">
        <a href="jobs?q=one#roles">Jobs</a><a href="jobs?q=one#roles">Jobs again</a>
        <a href="javascript:bad()">Bad</a><a href="mailto:hi@example.test">Mail</a>
        <iframe src="../archive"></iframe>
        <nav aria-label="Pagination"><button>1</button><button>Nästa</button></nav>"""
        links, _ = page_inventory(
            html, "https://example.test/", html_kind="rendered_html"
        )
        self.assertEqual(len(links), 5)
        self.assertEqual(
            links[0]["url"], "https://other.example.test/base/jobs?q=one#roles"
        )
        self.assertEqual(links[1]["url"], links[0]["url"])
        self.assertNotEqual(links[1]["link_id"], links[0]["link_id"])
        self.assertEqual(links[2]["kind"], "iframe")
        self.assertEqual(links[-1]["kind"], "control")
        self.assertIsNone(links[-1]["url"])

    def test_invented_duplicate_missing_scores_are_explicit(self):
        observed = sample_page().links
        assessment = {
            "link_id": observed[0]["link_id"],
            "priority": 90,
            "objectives": ["jobs"],
            "target_relevance": "target",
            "reason": "Jobs",
        }
        values, errors = validate_links(
            [assessment, assessment, assessment | {"link_id": "invented"}], observed
        )
        self.assertEqual(len(values), len(observed))
        self.assertTrue(all(v["assessment"] is None for v in values))
        self.assertIn("invented_link:invented", errors)
        self.assertTrue(any(e.startswith("duplicate_link:") for e in errors))

    def test_missing_route_is_uncertain_and_mailto_guards_contacts(self):
        raw = decisions()
        del raw["jobs"]
        raw["people"]["section_ids"] = ["invented-heading"]
        result, overrides = dispatch_decisions(raw, sample_page())
        self.assertEqual(result["jobs"]["decision"], "uncertain")
        self.assertEqual(result["people"]["decision"], "uncertain")
        self.assertEqual(result["company_contacts"]["decision"], "skip")
        self.assertIn("company_contacts", overrides)


class HTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_specialists_get_same_source_and_global_concurrency_is_bounded(self):
        requests = []
        active = maximum = 0

        async def handle(request):
            nonlocal active, maximum
            active += 1
            maximum = max(active, maximum)
            body = json.loads(request.content)
            requests.append(body)
            schema = json.loads(
                body["messages"][0]["content"].split(
                    "Required JSON Schema (validated by the application):\n", 1
                )[1]
            )
            payload = json.loads(
                body["messages"][1]["content"].split("\nSOURCE SNAPSHOT:\n", 1)[1]
            )
            self.assertEqual(payload, json.loads(sample_page().payload()))
            self.assertEqual(body["reasoning_effort"], "high")
            self.assertNotIn("tools", body)
            self.assertEqual(len(body["messages"]), 2)
            result = {}
            if "decisions" in schema["properties"]:
                routes = decisions()
                routes["people"]["decision"] = "run"
                result["decisions"] = routes
            if "data" in schema["properties"]:
                requested = schema["properties"]["data"]["required"]
                result["data"] = {
                    key: [contact()] if key == "company_contacts" else []
                    for key in requested
                }
            if "links" in schema["properties"]:
                result["links"] = [
                    {
                        "link_id": link["link_id"],
                        "priority": 90,
                        "objectives": ["jobs"],
                        "target_relevance": "unknown",
                        "reason": "Observed navigation",
                    }
                    for link in payload["observed_links"]
                ]
            await asyncio.sleep(0.01)
            active -= 1
            return httpx.Response(
                200,
                json={
                    "model": "deepseek-flash",
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "role": "assistant",
                                "content": json.dumps(result),
                            },
                        }
                    ],
                    "usage": {"prompt_tokens": 100, "completion_tokens": 20},
                },
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(handle),
            ) as client:
                agent = PageAgent(
                    ModelClient(
                        client,
                        "test-secret",
                        ResearchConfig(
                            model="deepseek-flash",
                            reasoning_effort="high",
                            extraction_concurrency=2,
                        ),
                        root,
                        api="deepseek",
                    )
                )
                results = await asyncio.gather(
                    agent.analyze(sample_page(), "routed", root / "a.json"),
                    agent.analyze(sample_page(), "routed", root / "b.json"),
                    agent.analyze(sample_page(), "one_pass", root / "c.json"),
                )
            self.assertEqual(
                len(requests), 7
            )  # two routes + four specialists + baseline
            self.assertEqual(maximum, 2)
            self.assertEqual(set(results[0]), {"schema_version", "data", "links"})
            self.assertEqual(
                results[0]["data"]["coverage"]["jobs"]["status"], "not_selected"
            )
            self.assertEqual(
                results[0]["data"]["records"]["company_contacts"],
                results[2]["data"]["records"]["company_contacts"],
            )
            for response in results[0]["data"]["processing"]["responses"].values():
                self.assertEqual(response["schema_errors"], [])
            specialist_bodies = [
                body
                for body in requests
                if '"data"' in body["messages"][0]["content"]
                and len(
                    json.loads(
                        body["messages"][0]["content"].split(
                            "Required JSON Schema (validated by the application):\n", 1
                        )[1]
                    )["properties"]["data"]["required"]
                )
                == 1
            ]
            self.assertEqual(len(specialist_bodies), 4)
            for path in (root / "calls").glob("*.json"):
                self.assertNotIn("test-secret", path.read_text())

    async def test_transport_failure_keeps_observed_links_and_failed_coverage(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(lambda _: httpx.Response(503)),
            ) as client:
                agent = PageAgent(
                    ModelClient(
                        client,
                        "fake",
                        ResearchConfig(max_http_attempts=1),
                        root,
                        api="deepseek",
                    )
                )
                result = await agent.analyze(
                    sample_page(), "one_pass", root / "page.json"
                )
            self.assertEqual(len(result["links"]), len(sample_page().links))
            self.assertTrue(
                all(
                    status["status"] == "failed"
                    for status in result["data"]["coverage"].values()
                )
            )


if __name__ == "__main__":
    unittest.main()
