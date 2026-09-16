"""Adversarial boundaries exposed by the saved multi-company experiment."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture
from test_package import response
from test_statements import (
    accept_descriptions,
    decisions,
    save_page,
    statement,
    technology,
)

from company_research.analytics import accepted_finding
from company_research.content import HtmlWindow, source_finding
from company_research.llm import ModelClient
from company_research.models import ResearchConfig
from company_research.review import review_claims
from company_research.statement_review import (
    DescriptionChecks,
    accepted_description,
    review_page_descriptions,
)
from company_research.statements import (
    accept_statement_decisions,
    consolidate_page_statements,
    statement_finding,
)
from company_research.storage import write_json


class AttributionReplayTests(unittest.IsolatedAsyncioTestCase):
    def test_strict_review_schema_has_no_optional_object_properties(self):
        for definition in DescriptionChecks.model_json_schema()["$defs"].values():
            if definition.get("type") == "object":
                self.assertEqual(
                    set(definition["properties"]), set(definition["required"])
                )

    async def test_correct_prose_does_not_compensate_for_tool_as_company(self):
        html = "<h1>DemoWorks</h1><p>DemoWorks uses Python.</p>"
        requests = []

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n"
                )[1]
            )
            requests.append(data)
            return httpx.Response(
                200,
                json=response(
                    {
                        "checks": [
                            {
                                "statement_id": "s1",
                                "supported": True,
                                "reason": "Correct prose.",
                                "source_attribution": {
                                    "subject_name": "DemoWorks",
                                    "subject_kind": "company",
                                    "source_name": "Python",
                                },
                                "correction": None,
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    subject_name="Python",
                    section_heading=None,
                    context="DemoWorks uses Python.",
                    evidence=["DemoWorks uses Python."],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_page_descriptions(
                    [record],
                    [page],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
            self.assertFalse(accepted_description(record))
            self.assertIn(
                "Structured actor", record.data["description_review"]["reason"]
            )

    async def test_actor_correction_keeps_client_and_requires_separate_review(self):
        html = "<h1>DemoWorks consulting</h1><h2>Client cases</h2><p>Our client Acme uses Python.</p>"
        requests = []

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n"
                )[1]
            )
            requests.append(data)
            if data["task"] == "evidence_repair":
                self.assertEqual(record.data["subject_name"], "Python")
                self.assertEqual(data["records"][0]["data"]["subject_name"], "Acme")
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "repairs": [
                                {
                                    "record_id": data["records"][0]["record_id"],
                                    "evidence": ["Our client Acme uses Python."],
                                }
                            ]
                        }
                    ),
                )
            if len(requests) == 3:
                self.assertEqual(data["statements"][0]["data"]["subject_name"], "Acme")
                self.assertEqual(
                    data["statements"][0]["original_description"]["subject_name"],
                    "Python",
                )
                return accept_descriptions(data)
            actor = {
                "subject_name": "Acme",
                "subject_kind": "company",
                "source_name": "Python",
            }
            return httpx.Response(
                200,
                json=response(
                    {
                        "checks": [
                            {
                                "statement_id": "s1",
                                "supported": False,
                                "reason": "Activity belongs to named client.",
                                "source_attribution": actor,
                                "correction": {
                                    "context": "The client Acme uses Python.",
                                    "application_context": None,
                                    "qualifiers": ["client"],
                                    "attribution": actor,
                                    "evidence": ["Our client uses Python."],
                                },
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    subject_name="Python",
                    section_heading="Client cases",
                    evidence=["Our client Acme uses Python."],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            old_id = record.record_id
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_page_descriptions(
                    [record],
                    [page],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
            self.assertTrue(accepted_description(record))
            self.assertEqual(len(requests), 3)
            self.assertEqual(record.record_id, old_id)
            self.assertEqual(record.data["subject_name"], "Acme")
            self.assertEqual(
                record.data["description_revisions"][0]["before"]["subject_name"],
                "Python",
            )
            record.data["subject_name"] = "DemoWorks"
            self.assertFalse(accepted_description(record))

    async def test_claim_review_cannot_accept_wrong_actor_even_with_matching_signal(
        self,
    ):
        html = "<p>DemoWorks uses Python.</p>"

        def handle(request):
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": "r1",
                                "supported": True,
                                "reason": "Explicit use.",
                                "source_subject": "DemoWorks",
                                "source_subject_kind": "company",
                                "source_object": None,
                                "specific_technology": True,
                                "source_signal": "stated_use",
                                "source_scope": "company",
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = source_finding(
                "technology_signals",
                {
                    k: v
                    for k, v in technology(
                        company="Python",
                        signal="stated_use",
                        evidence=["DemoWorks uses Python."],
                    ).items()
                    if k != "statement_ids"
                },
                page=page.model_dump(),
                window=HtmlWindow(0, len(html), html),
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_claims(
                    [record],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "actor",
                )
            self.assertFalse(accepted_finding(record))
            self.assertIn(
                "company/employer", record.data["interpretation_review"]["reason"]
            )

    async def test_empty_failed_extraction_is_pending_without_statement_ids(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root)
            write_json(
                root / "page-statements/p0001.json",
                {
                    "html_sha256": page.html_sha256,
                    "status": "partial",
                    "statements": [],
                },
            )
            async with httpx.AsyncClient(base_url="https://test.invalid/") as client:
                llm = ModelClient(client, "test", ResearchConfig(), root)
                result = await consolidate_page_statements(
                    [], [page], catalog_fixture(), llm, root
                )
            self.assertEqual(llm.calls, [])
            self.assertEqual(result["pending_statement_ids"], [])
            self.assertEqual(result["pending_extraction_page_ids"], ["p0001"])

    def test_typographic_quotes_match_but_omitted_words_and_names_do_not(self):
        html = (
            "<h1>DemoWorks</h1><p>Python is DMC’s tool and it isn’t being replaced.</p>"
        )
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            for quote, expected in [
                ("Python is DMC's tool and it isn't being replaced.", "source_matched"),
                ("Python is DMC's tool and it is being replaced.", "needs_review"),
                ("Python is DMC's tool being replaced.", "needs_review"),
            ]:
                record = statement_finding(
                    statement(section_heading=None, evidence=[quote]),
                    page,
                    HtmlWindow(0, len(html), html),
                )
                self.assertEqual(record.evidence_status, expected)
                self.assertEqual(record.sources[0].evidence[-1].text, quote)

    def test_person_skill_cannot_be_normalized_as_company_technology(self):
        html = "<h1>Pat</h1><p>Python</p>"
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    subject_name="Pat",
                    subject_kind="person",
                    section_heading=None,
                    evidence=["Pat", "Python"],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            findings, _, _, issues = accept_statement_decisions(
                decisions(technology()), {"s1": record}, {page.page_id: page}, root
            )
            self.assertEqual(findings, [])
            self.assertIn("person", issues["s1"])
