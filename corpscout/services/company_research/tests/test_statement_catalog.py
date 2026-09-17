"""Catalog reply completeness and preservation of already resolved names."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture, proposal
from test_package import response

from company_research.llm import ModelClient
from company_research.models import Finding, ResearchConfig
from company_research.resolution import resolve_technologies


class RequiredCatalogKeysTests(unittest.IsolatedAsyncioTestCase):
    async def test_only_failed_catalog_key_is_retried_and_generic_abstention_is_preserved(
        self,
    ):
        requests = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            requested = [item["request_id"] for item in data["technologies"]]
            requests.append(requested)
            required = body["response_format"]["json_schema"]["schema"]["properties"][
                "resolutions"
            ]["required"]
            self.assertEqual(required, requested)
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "resolutions": {
                                "t1": proposal(),
                                "t2": {
                                    "status": "matched",
                                    "canonical_technology": "Invented catalog name",
                                    "proposed_technology": None,
                                    "reason": "Invalid identity",
                                },
                                "t3": None,
                            }
                        }
                    ),
                )
            draft = proposal()
            draft["proposed_technology"]["name"] = "Simulink"
            draft["proposed_technology"]["description"] = "Simulation environment"
            return httpx.Response(200, json=response({"resolutions": {"t2": draft}}))

        records = [
            Finding(
                record_id=name,
                data={"technology": name, "context": "Company engineering skills"},
                sources=[],
                evidence_status="source_matched",
            )
            for name in ["Yocto", "Simulink", "CMOS"]
        ]
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                issues = await resolve_technologies(
                    records,
                    catalog_fixture(),
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "test",
                )
        self.assertEqual(requests, [["t1", "t2", "t3"], ["t2"]])
        self.assertEqual(
            records[0].data["catalog_match"]["proposed_technology"]["name"], "Yocto"
        )
        self.assertEqual(
            records[1].data["catalog_match"]["proposed_technology"]["name"], "Simulink"
        )
        self.assertNotIn("catalog_error", records[0].data)
        self.assertNotIn("catalog_error", records[1].data)
        self.assertIsNone(records[2].data["catalog_match"])
        self.assertIn("declined", records[2].data["catalog_error"])
        self.assertEqual(len(issues), 1)
