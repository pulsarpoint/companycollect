"""Exercise the real MCP stdio boundary and shared catalog/proposal validation."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture, proposal
from test_package import response

from company_research.llm import ModelClient
from company_research.models import ResearchConfig
from company_research.technology_catalog import (
    CatalogSnapshot,
    TechnologyCatalog,
    snapshot_version,
    validate_technology_match,
)

if importlib.util.find_spec("mcp") is None:
    raise unittest.SkipTest("Install company-research[mcp] to run MCP protocol tests")

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


class ProposalTests(unittest.TestCase):
    def test_description_and_category_are_required_by_shared_validator(self):
        catalog = catalog_fixture()
        for changes in (
            {"description": " "},
            {"description": None},
            {"category_ids": [], "category_suggestion": None},
            {"category_ids": [], "category_suggestion": " "},
            {"category_ids": [65535]},
        ):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                value = proposal()
                value["proposed_technology"].update(changes)
                validate_technology_match(
                    "Yocto", value, catalog, [catalog.search("Yocto")]
                )
        for changes in (
            {"category_ids": [1], "category_suggestion": None},
            {"category_ids": [], "category_suggestion": "Embedded build tools"},
        ):
            value = proposal()
            value["proposed_technology"].update(changes)
            self.assertEqual(
                validate_technology_match(
                    "Yocto", value, catalog, [catalog.search("Yocto")]
                )["status"],
                "proposed",
            )

    def test_generic_observation_cannot_be_renamed_into_a_proposal(self):
        with self.assertRaises(ValueError):
            validate_technology_match(
                "XML", proposal(), catalog_fixture(), [catalog_fixture().search("XML")]
            )

    def test_inconsistent_category_labels_are_not_presented_as_known_mapping(self):
        snapshot = catalog_fixture().snapshot.model_dump()
        snapshot["entries"][0]["categories"] = ["Conflicting label"]
        snapshot["version"] = snapshot_version(snapshot["entries"], snapshot["aliases"])
        options = TechnologyCatalog(
            CatalogSnapshot.model_validate(snapshot)
        ).category_options()
        self.assertEqual(options["categories"], [])
        self.assertEqual(options["ambiguous_category_ids"], [1])


class CatalogMCPTests(unittest.IsolatedAsyncioTestCase):
    async def test_agent_search_category_and_proposal_flow_over_stdio(self):
        catalog = catalog_fixture()
        snapshot = catalog.snapshot.model_dump()
        for name, description in [
            ("API Spreadsheets", "An API for spreadsheet documents"),
            ("Carbon Ads", "Marketing automation for displaying ads"),
        ]:
            snapshot["entries"].append(
                {
                    "technology": name,
                    "description": description,
                    "website": "https://example.test",
                    "category_ids": [1],
                    "categories": ["Development"],
                    "groups": ["Software"],
                }
            )
        snapshot["version"] = snapshot_version(snapshot["entries"], snapshot["aliases"])
        with TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text(json.dumps(snapshot), encoding="utf-8")
            before = path.read_bytes()
            params = StdioServerParameters(
                command=sys.executable,
                args=[
                    "-m",
                    "company_research.catalog_mcp",
                    "--offline-catalog",
                    "--technology-catalog",
                    str(path),
                ],
            )
            async with (
                stdio_client(params) as streams,
                ClientSession(*streams) as session,
            ):
                await session.initialize()
                listing = await session.list_tools()
                self.assertEqual(
                    {t.name for t in listing.tools},
                    {
                        "get_catalog_info",
                        "get_technology",
                        "search_technologies",
                        "list_technology_categories",
                        "prepare_technology_proposal",
                    },
                )
                self.assertTrue(
                    all(
                        t.annotations is not None and t.annotations.readOnlyHint
                        for t in listing.tools
                    )
                )
                result = await session.call_tool(
                    "search_technologies",
                    {
                        "queries": ["GIT", "AWS", "ADS", "C++"],
                        "context": "RF circuit simulation and automation",
                    },
                )
                self.assertFalse(result.isError, result.content)
                assert result.structuredContent is not None
                rows = result.structuredContent["results"]
                self.assertEqual(rows[0]["canonical_technology"], "git")
                self.assertEqual(rows[1]["canonical_technology"], "Amazon Web Services")
                self.assertEqual(rows[2]["candidates"], [])
                self.assertEqual(rows[3]["canonical_technology"], "C++")
                self.assertEqual(
                    {r["technology"] for r in rows[3]["candidates"]}, {"C++"}
                )
                self.assertIn("description", rows[0]["candidates"][0])
                detail = await session.call_tool("get_technology", {"name": "AWS"})
                assert detail.structuredContent is not None
                self.assertEqual(
                    detail.structuredContent["technology"]["technology"],
                    "Amazon Web Services",
                )
                categories = await session.call_tool("list_technology_categories", {})
                assert categories.structuredContent is not None
                self.assertEqual(
                    categories.structuredContent["categories"],
                    [{"id": 1, "name": "Development"}],
                )
                args = {
                    "observed_name": "Yocto",
                    "proposal": proposal()["proposed_technology"],
                    "reason": "No equivalent after catalog lookup",
                    "alternative_names": ["Yocto Project"],
                }
                prepared = await session.call_tool("prepare_technology_proposal", args)
                self.assertFalse(prepared.isError, prepared.content)
                assert prepared.structuredContent is not None
                self.assertEqual(
                    prepared.structuredContent["submission_status"],
                    "prepared_not_submitted",
                )
                match = prepared.structuredContent["catalog_match"]
                self.assertEqual(
                    match["proposed_technology"]["description"],
                    "Embedded Linux build tooling",
                )
                self.assertEqual(
                    match["proposed_technology"]["category_suggestion"],
                    "Development tools",
                )
                self.assertEqual(
                    {r["query"] for r in match["searches"]}, {"Yocto", "Yocto Project"}
                )
                repeated = await session.call_tool("prepare_technology_proposal", args)
                assert repeated.structuredContent is not None
                self.assertEqual(
                    repeated.structuredContent["catalog_match"]["proposal_id"],
                    match["proposal_id"],
                )
                args["observed_name"] = "GIT"
                args["proposal"]["name"] = "Git"
                existing = await session.call_tool("prepare_technology_proposal", args)
                assert existing.structuredContent is not None
                self.assertEqual(
                    existing.structuredContent["catalog_match"]["canonical_technology"],
                    "git",
                )
                args["observed_name"] = "Yocto"
                args["proposal"] = proposal()["proposed_technology"]
                args["proposal"]["category_suggestion"] = None
                invalid = await session.call_tool("prepare_technology_proposal", args)
                self.assertTrue(invalid.isError)
            self.assertEqual(path.read_bytes(), before)

    async def test_deepseek_tool_loop_can_request_category_options(self):
        requests = []

        def handle(request):
            body = json.loads(request.content)
            requests.append(body)
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json={
                        "choices": [
                            {
                                "finish_reason": "tool_calls",
                                "message": {
                                    "role": "assistant",
                                    "content": None,
                                    "tool_calls": [
                                        {
                                            "id": "categories",
                                            "type": "function",
                                            "function": {
                                                "name": "list_technology_categories",
                                                "arguments": "{}",
                                            },
                                        }
                                    ],
                                },
                            }
                        ]
                    },
                )
            self.assertEqual(
                json.loads(body["messages"][-1]["content"])["categories"],
                [{"id": 1, "name": "Development"}],
            )
            return httpx.Response(200, json=response({"done": True}))

        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://openrouter.test/v1/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(client, "test-key", ResearchConfig(), Path(directory))
                result = await llm.ask(
                    "Resolve technologies", {}, task="test", catalog=catalog_fixture()
                )
        self.assertEqual(result.document, {"done": True})
        self.assertEqual(len(requests), 2)
