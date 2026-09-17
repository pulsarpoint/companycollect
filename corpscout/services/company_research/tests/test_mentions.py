"""Evidence preservation, two-stage HTTP contract and catalog lookup."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import catalog_fixture

from company_research import analysis, mentions, page_agent
from company_research.llm import ModelClient
from company_research.models import OBJECTIVES, ResearchConfig
from company_research.storage import content_hash

HTML = "<h1>Example Labs</h1><h2>Data engineer</h2><p>Our team plans Fabric.</p><p>Fabric experience required.</p><table><tr><th>Product</th><th>Connection</th></tr><tr><td>Widget</td><td>Compatible with Python</td></tr></table>"


def mention(sections):
    return {
        "source_name": "Fabric",
        "section_ids": [sections[2]["section_id"]],
        "context_section_ids": [sections[0]["section_id"], sections[1]["section_id"]],
        "actor": "Example Labs",
        "job_title": "Data engineer",
        "context": "Plans Fabric",
    }


def decision(item):
    return {
        "mention_id": item["mention_id"],
        "disposition": "specific_technology",
        "category": "Data platform",
        "description": "Planned platform",
        "reason": "Explicit future plan",
        "relationships": [
            {
                "relationship": "planned_adoption",
                "actor": "Example Labs",
                "scope": "team",
                "subject": "Data platform team",
                "job_title": "Data engineer",
                "alternative_group": None,
                "as_of": None,
                "description": "Team plans Fabric",
                "section_ids": item["section_ids"],
            }
        ],
    }


class MentionTests(unittest.IsolatedAsyncioTestCase):
    def test_client_context_comes_from_original_text_not_record_paraphrases(self):
        sections = mentions.source_sections(
            "<p>Client B selected Supplier A to build Gateway X.</p><p>Linux powers the gateway.</p>",
            "case",
            "native_cleaned_html",
        )
        page = {
            "source_sections": sections,
            "data": {
                "records": {
                    "company_profile": [],
                    "products_services": [],
                    "company_relationships": [
                        {
                            "data": {"description": "Invented summary"},
                            "sources": [
                                {
                                    "evidence": [
                                        {"text": "Client B selected Supplier A"},
                                        {"text": "absent quotation"},
                                    ]
                                }
                            ],
                        }
                    ],
                }
            },
        }
        self.assertEqual(
            mentions.classification_context(page), [sections[0]["section_id"]]
        )
        batch = [
            {"mention_id": "one", "source_section_ids": [sections[0]["section_id"]]},
            {
                "mention_id": "two",
                "source_section_ids": [
                    sections[0]["section_id"],
                    sections[1]["section_id"],
                ],
            },
        ]
        payload = json.loads(
            mentions.classification_payload(
                batch, {section["section_id"]: section for section in sections}
            )
        )
        self.assertEqual(len(payload["source_sections"]), 2)
        self.assertNotIn("Invented summary", json.dumps(payload))

    def test_source_sections_and_invalid_ids_preserve_raw_evidence(self):
        sections = mentions.source_sections(HTML, "page", "native_cleaned_html")
        raw = mention(sections)
        validated = mentions.validate_mentions(
            [raw, raw | {"section_ids": ["invented"]}, {"bad": "kept"}],
            sections,
            "page",
        )
        self.assertEqual(validated["status"], "partial")
        self.assertEqual(len(validated["mentions"]), 3)
        self.assertEqual(validated["mentions"][0]["status"], "source_linked")
        self.assertIn(
            sections[3]["section_id"], validated["mentions"][0]["source_section_ids"]
        )
        self.assertEqual(sections[-1]["table_headers"], ["Product", "Connection"])
        self.assertEqual(
            sections, mentions.source_sections(HTML, "page", "native_cleaned_html")
        )
        self.assertNotEqual(
            sections[0]["section_id"],
            mentions.source_sections(HTML, "page", "rendered_html")[0]["section_id"],
        )

    def test_duplicate_unknown_and_cross_mention_references_are_not_accepted(self):
        sections = mentions.source_sections(HTML, "page", "native_cleaned_html")
        item = mentions.validate_mentions([mention(sections)], sections, "page")[
            "mentions"
        ][0]
        valid = decision(item)
        for raw in (
            [valid, valid],
            [valid | {"mention_id": "invented"}],
            [
                valid
                | {
                    "relationships": [
                        valid["relationships"][0] | {"section_ids": ["different_page"]}
                    ]
                }
            ],
        ):
            results, rejected = mentions.accept_decisions(raw, [item])
            self.assertTrue(rejected)
            self.assertIsNone(results[0]["classification"])
            self.assertEqual(results[0]["status"], "needs_review")

    def test_shared_page_context_still_requires_primary_technology_evidence(self):
        sections = mentions.source_sections(HTML, "page", "native_cleaned_html")
        item = mentions.validate_mentions([mention(sections)], sections, "page")[
            "mentions"
        ][0]
        context_id = "page:native_cleaned_html:s9999"
        other_page_id = "other:native_cleaned_html:s0001"
        views = mentions.batch_validation_mentions(
            [
                item,
                {"mention_id": "page:m0002", "source_section_ids": [context_id]},
                {"mention_id": "other:m0000", "source_section_ids": [other_page_id]},
            ]
        )
        self.assertIn(context_id, views[0]["source_section_ids"])
        self.assertNotIn(other_page_id, views[0]["source_section_ids"])
        raw = decision(item)
        raw["relationships"][0]["section_ids"] = [*item["section_ids"], context_id]
        accepted, rejected = mentions.accept_decisions([raw], [views[0]])
        self.assertFalse(rejected)
        self.assertEqual(accepted[0]["status"], "classified")
        raw["relationships"][0]["section_ids"] = [context_id]
        accepted, rejected = mentions.accept_decisions([raw], [views[0]])
        self.assertTrue(rejected)
        self.assertEqual(accepted[0]["status"], "needs_review")

    def test_catalog_lookup_keeps_case_alias_ambiguity_and_failure_separate(self):
        catalog = catalog_fixture()
        for name, expected in [
            ("Git", "matched"),
            ("AWS", "matched"),
            ("MOXIE", "ambiguous"),
            ("zzzzqqqq", "not_found"),
        ]:
            self.assertEqual(
                mentions.lookup_mention({"source_name": name}, catalog)["status"],
                expected,
            )
        self.assertEqual(
            mentions.lookup_mention({"source_name": "Git"}, None)["status"], "failed"
        )

    def test_short_names_do_not_match_inside_another_technology(self):
        sections = mentions.source_sections(HTML, "page", "native_cleaned_html")
        checked = mentions.validate_mentions(
            [mention(sections) | {"source_name": "C"}], sections, "page"
        )
        self.assertEqual(checked["mentions"][0]["status"], "needs_review")

    def test_rendered_evidence_keeps_its_own_capture_hash(self):
        native = "<h1>Example Labs</h1>"
        rendered = native + "<p>Example Labs info@example.test</p>"
        page = page_agent.PageInput(
            page={
                "page_id": "sample",
                "source_url": "https://example.test/",
                "html_file": "page.html",
                "html_sha256": content_hash(native),
                "fetched_at": "2026-09-16T00:00:00Z",
            },
            html=native,
            headings=[],
            links=[],
            target_url="https://example.test/",
        )
        raw = {
            objective: []
            for objective in OBJECTIVES
            if objective != "technology_signals"
        }
        raw["company_contacts"] = [
            {
                "owner": "Example Labs",
                "owner_kind": "company",
                "type": "email",
                "value": "info@example.test",
                "purpose": None,
                "evidence": ["Example Labs info@example.test"],
            }
        ]
        checked = mentions.validate_page_records(raw, page, rendered)
        record = checked["records"]["company_contacts"][0]
        self.assertEqual(record["evidence_status"], "source_matched")
        self.assertEqual(record["sources"][0]["evidence_status"], "needs_review")
        self.assertEqual(record["sources"][1]["representation"], "rendered_html")
        self.assertEqual(record["sources"][1]["html_sha256"], content_hash(rendered))
        self.assertEqual(checked["coverage"]["company_contacts"]["status"], "processed")

    async def test_page_retry_checks_source_names_even_when_schema_is_valid(self):
        sections = mentions.source_sections(HTML, "page", "native_cleaned_html")
        calls = []

        def handle(request):
            calls.append(json.loads(request.content))
            raw = mention(sections) | {
                "source_name": "Data engineer" if len(calls) == 1 else "Fabric"
            }
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {
                                "content": json.dumps({"technology_mentions": [raw]})
                            },
                        }
                    ]
                },
            )

        with TemporaryDirectory() as temporary:
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client,
                    "fake",
                    ResearchConfig(model="deepseek-flash"),
                    Path(temporary),
                    api="deepseek",
                )
                response = await mentions.request_json(
                    llm,
                    "Extract the page",
                    {"type": "object"},
                    "retry",
                    sections=sections,
                )
            self.assertEqual(len(calls), 2)
            self.assertEqual(response["status"], "processed")
            self.assertTrue(response["attempts"][0]["schema_errors"])

    async def test_two_stage_http_flow_retries_and_keeps_original_sources(self):
        calls = []

        def handle(request):
            body = json.loads(request.content)
            calls.append(body)
            prompt = body["messages"][1]["content"]
            self.assertNotIn("tools", body)
            self.assertEqual(body["reasoning_effort"], "high")
            if "SOURCE SNAPSHOT:" in prompt:
                source = json.loads(prompt.split("SOURCE SNAPSHOT:\n")[1])
                doc = {
                    "data": {o: [] for o in OBJECTIVES if o != "technology_signals"},
                    "technology_mentions": [mention(source["source_sections"])],
                    "links": [],
                }
                text = json.dumps(doc) + ("}" if len(calls) == 1 else "")
            else:
                batch = json.loads(prompt.split("MENTIONS AND ORIGINAL SECTIONS:\n")[1])
                self.assertIn(
                    "Our team plans Fabric.",
                    [s["text"] for s in batch["source_sections"]],
                )
                text = json.dumps({"decisions": [decision(batch["mentions"][0])]})
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {"finish_reason": "stop", "message": {"content": text}}
                    ],
                    "usage": {"prompt_tokens": 10, "completion_tokens": 20},
                },
            )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            config = ResearchConfig(
                model="deepseek-flash",
                reasoning_effort="high",
                max_model_calls=10,
                max_http_attempts=1,
            )
            page = page_agent.PageInput(
                page={
                    "page_id": "sample",
                    "source_url": "https://example.test/",
                    "html_file": "page.html",
                    "html_sha256": content_hash(HTML),
                    "fetched_at": "2026-09-16T00:00:00Z",
                },
                html=HTML,
                headings=[],
                links=[],
                target_url="https://example.test/",
            )
            async with httpx.AsyncClient(
                base_url="https://api.deepseek.com/",
                transport=httpx.MockTransport(handle),
            ) as client:
                llm = ModelClient(
                    client, "never-save-this-secret", config, root, api="deepseek"
                )
                page_result = await mentions.MentionPageAgent(llm).analyze(page)
                classified = await mentions.classify_mentions(
                    llm, [page_result], catalog_fixture()
                )
            self.assertEqual(len(calls), 3)
            self.assertEqual(
                page_result["captures"]["native_cleaned_html"]["content"], HTML
            )
            self.assertEqual(
                classified["decisions"][0]["classification"]["relationships"][0][
                    "scope"
                ],
                "team",
            )
            result = analysis.build_result(
                page.target_url, [page_result], classified, config, "run", {}, {}
            )
            self.assertEqual(result["crawl_status"], "saved_pages_only")
            for path in root.rglob("*.json"):
                self.assertNotIn("never-save-this-secret", path.read_text())
