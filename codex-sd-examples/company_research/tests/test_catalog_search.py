import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock

import httpx
from test_package import page, response
from test_technologies import signal

from company_research.content import HtmlWindow
from company_research.llm import ModelClient, parse_model_json
from company_research.models import OBJECTIVES, ResearchConfig
from company_research.research import extract_window
from company_research.technology_catalog import (
    CatalogSnapshot,
    TechnologyCatalog,
    snapshot_version,
    sync_catalog,
    technology_key,
    validate_technology_match,
)


def catalog_fixture():
    entries = [
        {
            "technology": name,
            "description": name + " technology",
            "website": "https://example.test",
            "category_ids": [1],
            "categories": ["Development"],
            "groups": ["Software"],
        }
        for name in [
            "C",
            "C++",
            "C#",
            "git",
            "Python",
            "Amazon Web Services",
            "Moxie",
            "mOxie",
        ]
    ]
    aliases = [
        {"alias": "AWS", "alias_key": "aws", "technology": "Amazon Web Services"}
    ]
    return TechnologyCatalog(
        CatalogSnapshot.model_validate(
            {
                "schema_version": "1.0",
                "synced_at": "2026-09-07T00:00:00Z",
                "version": snapshot_version(entries, aliases),
                "entries": entries,
                "aliases": aliases,
            }
        )
    )


def proposal():
    return {
        "status": "proposed",
        "canonical_technology": None,
        "proposed_technology": {
            "name": "Yocto",
            "description": "Embedded Linux build tooling",
            "website": None,
            "category_ids": [],
            "category_suggestion": "Development tools",
            "saas": None,
            "oss": None,
            "pricing": [],
        },
        "reason": "No matching identity in the local catalog",
    }


class CatalogTests(unittest.TestCase):
    def test_fenced_output_preserves_json_without_repairing_invalid_data(self):
        self.assertEqual(parse_model_json('{"name":"C++"}'), ({"name": "C++"}, False))
        self.assertEqual(
            parse_model_json('Extraction follows.\n```json\n{"name":"C++"}\n```'),
            ({"name": "C++"}, True),
        )
        for raw in [
            '{"actor":"DMC","actor":"React"}',
            '```json\n{"actor":"DMC","actor":"React"}\n```',
            '{"actor":"DMC","actor":"DMC"}',
            '```json\n{"name":\n```',
            "```json\n{}\n```\n```json\n{}\n```",
            'Explanation: {"name":"C++"}',
        ]:
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_model_json(raw)

    def test_canonical_normalized_alias_and_ambiguous_names(self):
        catalog = catalog_fixture()
        for label, expected in [
            ("Git", ("git", "normalized")),
            ("GIT", ("git", "normalized")),
            ("AWS", ("Amazon Web Services", "alias")),
            ("C++", ("C++", "exact")),
            ("C#", ("C#", "exact")),
            ("C", ("C", "exact")),
            ("MOXIE", (None, "ambiguous")),
            ("mOxie", ("mOxie", "exact")),
        ]:
            self.assertEqual(catalog.resolve(label), expected)
        self.assertEqual(technology_key(" ＧＩＴ\u00a0"), "git")
        self.assertEqual(technology_key("Straße"), "strasse")

    def test_snapshot_rejects_corruption_and_dangling_aliases(self):
        snapshot = catalog_fixture().snapshot.model_dump()
        snapshot["entries"][0]["technology"] = "Changed"
        with self.assertRaisesRegex(ValueError, "hash"):
            TechnologyCatalog(CatalogSnapshot.model_validate(snapshot))
        snapshot["version"] = snapshot_version(snapshot["entries"], snapshot["aliases"])
        snapshot["aliases"][0]["technology"] = "Absent"
        snapshot["version"] = snapshot_version(snapshot["entries"], snapshot["aliases"])
        with self.assertRaisesRegex(ValueError, "dangling"):
            TechnologyCatalog(CatalogSnapshot.model_validate(snapshot))

    def test_proposal_requires_search_and_cannot_invent_target_or_category(self):
        catalog = catalog_fixture()
        search = [catalog.search("Yocto")]
        with self.assertRaisesRegex(ValueError, "successful search"):
            validate_technology_match("Yocto", proposal(), catalog, [])
        valid = validate_technology_match("Yocto", proposal(), catalog, search)
        self.assertEqual(valid["status"], "proposed")
        self.assertEqual(
            valid["proposal_id"],
            validate_technology_match("Yocto", proposal(), catalog, search)[
                "proposal_id"
            ],
        )
        bad = proposal()
        bad["proposed_technology"]["category_ids"] = [65535]
        with self.assertRaises(ValueError):
            validate_technology_match("Yocto", bad, catalog, search)
        bad = {
            "status": "matched",
            "canonical_technology": "Invented",
            "proposed_technology": None,
            "reason": "Guess",
        }
        with self.assertRaises(ValueError):
            validate_technology_match("Yocto", bad, catalog, search)
        ambiguous = proposal()
        ambiguous["proposed_technology"]["name"] = "MOXIE"
        self.assertEqual(
            validate_technology_match(
                "MOXIE", ambiguous, catalog, [catalog.search("MOXIE")]
            )["status"],
            "proposed",
        )

    def test_failed_sync_retains_previous_cache(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            path.write_text("original")
            client = Mock()
            client.execute.side_effect = [[], [], [], []] * 3
            with self.assertRaisesRegex(ValueError, "incomplete"):
                sync_catalog(client, path)
            self.assertEqual(path.read_text(), "original")

    def test_complete_sync_produces_valid_snapshot(self):
        catalog = catalog_fixture()
        rows = [
            (*entry.model_dump().values(), "run") for entry in catalog.snapshot.entries
        ]
        aliases = [
            (*entry.model_dump().values(), "run") for entry in catalog.snapshot.aliases
        ]
        client = Mock()
        client.execute.side_effect = [[("run",)], rows, aliases, [("run",)]]
        with TemporaryDirectory() as directory:
            path = Path(directory) / "catalog.json"
            synced = sync_catalog(client, path)
            self.assertEqual(synced.snapshot.version, catalog.snapshot.version)
            self.assertEqual(
                TechnologyCatalog.read(path).resolve("Git"), ("git", "normalized")
            )


def accepted_technology_reviews(body):
    prompt = body["messages"][1]["content"]
    if '"task": "proposal_review"' in prompt:
        proposals = json.loads(prompt.split("INPUT DATA:\n", 1)[1])["proposals"]
        return httpx.Response(
            200,
            json=response(
                {
                    "reviews": [
                        {
                            "record_id": proposal["record_id"],
                            "identity_supported": True,
                            "description_supported": True,
                            "category_supported": True,
                            "reason": "Appropriate tool draft",
                        }
                        for proposal in proposals
                    ]
                }
            ),
        )
    if '"task": "claim_review"' not in prompt:
        return None
    claims = json.loads(prompt.split("INPUT DATA:\n", 1)[1])["claims"]
    return httpx.Response(
        200,
        json=response(
            {
                "reviews": [
                    {
                        "record_id": claim["record_id"],
                        "supported": True,
                        "reason": "Specific source tool and signal",
                        "source_subject": claim["data"].get("company"),
                        "source_subject_kind": "company"
                        if claim["data"].get("company")
                        else "unknown",
                        "source_object": None,
                        "specific_technology": True,
                        "source_signal": claim["data"]["signal"],
                        "source_scope": claim["data"]["scope"],
                    }
                    for claim in claims
                ]
            }
        ),
    )


class CatalogToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_out_of_scope_names_do_not_escape_extraction_or_erase_service_details(
        self,
    ):
        html = (
            "<h1>DemoWorks Engineer</h1><p>DemoWorks offers radar development "
            "using Python, with JSON deliverables.</p>"
        )
        evidence = [
            "DemoWorks",
            "Engineer",
            "DemoWorks offers radar development using Python, with JSON deliverables.",
        ]
        document = {objective: [] for objective in OBJECTIVES} | {
            "explicit_negatives": []
        }
        document["products_services"] = [
            {
                "company": "DemoWorks",
                "kind": "service",
                "name": "radar development",
                "description": "Radar development using Python, with JSON deliverables.",
                "url": None,
                "price": None,
                "evidence": evidence,
            }
        ]
        document["technology_signals"] = [
            signal(technology=name, evidence=evidence)
            for name in ["Python", "JSON", "Radar"]
        ]
        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://test.invalid/",
                transport=httpx.MockTransport(
                    lambda request: (
                        accepted_technology_reviews(json.loads(request.content))
                        or httpx.Response(200, json=response(document))
                    )
                ),
            ) as client:
                llm = ModelClient(
                    client, "secret", ResearchConfig(max_corrections=0), Path(directory)
                )
                findings, complete, issues, assessed = await extract_window(
                    HtmlWindow(0, len(html), html),
                    page(html, "https://example.test/jobs/engineer"),
                    llm,
                    Path(directory),
                    0,
                    catalog_fixture(),
                )
        self.assertFalse(complete)
        self.assertTrue(issues)
        self.assertIn("products_services", assessed)
        technologies = [
            f for objective, f in findings if objective == "technology_signals"
        ]
        services = [f for objective, f in findings if objective == "products_services"]
        self.assertEqual([f.data["technology"] for f in technologies], ["Python"])
        self.assertEqual(technologies[0].data["catalog_match"]["status"], "matched")
        self.assertEqual(
            services[0].data["description"],
            "Radar development using Python, with JSON deliverables.",
        )
        self.assertEqual(services[0].evidence_status, "source_matched")

    async def test_extraction_calls_local_search_and_preserves_original_evidence(self):
        html = "<h1>DemoWorks Engineer</h1><p>Our team uses Git and Yocto.</p>"
        document = {objective: [] for objective in OBJECTIVES} | {
            "explicit_negatives": []
        }
        evidence = ["DemoWorks", "Engineer", "Our team uses Git and Yocto."]
        document["technology_signals"] = [
            signal(technology="Git", evidence=evidence),
            signal(technology="Yocto", evidence=evidence),
        ]
        requests = []

        def handle(request):
            body = json.loads(request.content)
            review = accepted_technology_reviews(body)
            if review is not None:
                return review
            requests.append(body)
            if len(requests) == 1:
                self.assertNotIn("tools", body)
                return httpx.Response(200, json=response(document))
            if len(requests) == 2:
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
                                            "id": "search1",
                                            "type": "function",
                                            "function": {
                                                "name": "search_technologies",
                                                "arguments": json.dumps(
                                                    {"queries": ["Git", "Yocto"]}
                                                ),
                                            },
                                        }
                                    ],
                                },
                            }
                        ]
                    },
                )
            result = json.loads(body["messages"][-1]["content"])
            self.assertEqual(result["results"][0]["canonical_technology"], "git")
            return httpx.Response(
                200,
                json=response(
                    {
                        "resolutions": [
                            {"technology": "Yocto", "catalog_match": proposal()}
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(
                    client, "secret", ResearchConfig(max_corrections=0), Path(directory)
                )
                findings, complete, issues, _ = await extract_window(
                    HtmlWindow(0, len(html), html),
                    page(html, "https://example.test/jobs/engineer"),
                    llm,
                    Path(directory),
                    0,
                    catalog_fixture(),
                )
        self.assertTrue(complete, issues)
        self.assertEqual(len(requests), 3)
        self.assertEqual(len(llm.calls), 5)
        self.assertTrue(all("tools" in request for request in requests[1:]))
        self.assertEqual(
            [finding.data["technology"] for _, finding in findings], ["Git", "Yocto"]
        )
        self.assertEqual(
            [finding.data["catalog_match"]["status"] for _, finding in findings],
            ["matched", "proposed"],
        )
        self.assertTrue(
            all(finding.evidence_status == "source_matched" for _, finding in findings)
        )

    async def test_unknown_tools_are_not_executed_and_rounds_are_bounded(self):
        payload = {
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "bad",
                                "type": "function",
                                "function": {"name": "shell", "arguments": "{}"},
                            }
                        ],
                    },
                }
            ]
        }
        with TemporaryDirectory() as directory:
            async with httpx.AsyncClient(
                base_url="https://test.invalid/",
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(200, json=payload)
                ),
            ) as client:
                llm = ModelClient(
                    client,
                    "secret",
                    ResearchConfig(max_technology_tool_rounds=1),
                    Path(directory),
                )
                reply = await llm.ask(
                    "test", {}, task="test", catalog=catalog_fixture()
                )
        self.assertEqual(reply.error, "Technology tool round limit reached")
        self.assertEqual(len(llm.calls), 2)
        self.assertEqual(reply.searches, [])
