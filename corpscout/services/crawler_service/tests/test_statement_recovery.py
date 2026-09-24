"""Regression boundaries for reviewed descriptions and isolated normalization repairs."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import accepted_technology_reviews, catalog_fixture
from test_package import response
from test_statements import (
    HTML,
    accept_descriptions,
    decisions,
    save_page,
    statement,
    technology,
)

from crawler_service.content import HtmlWindow
from crawler_service.llm import ModelClient
from crawler_service.models import ResearchConfig
from crawler_service.statement_review import (
    accepted_description,
    review_page_descriptions,
)
from crawler_service.statements import (
    accept_normalization_items,
    accept_statement_decisions,
    consolidate_page_statements,
    statement_decision_schema,
    statement_finding,
)


class DescriptionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_correction_is_rechecked_and_approval_is_bound_to_exact_description(
        self,
    ):
        requests = []

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n"
                )[1]
            )
            requests.append(data)
            self.assertEqual(data["cleaned_html"], HTML)
            if len(requests) == 1:
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "checks": [
                                {
                                    "statement_id": "s1",
                                    "supported": False,
                                    "reason": "A skills table does not support deployment or radar use.",
                                    "correction": {
                                        "context": statement().context,
                                        "application_context": None,
                                        "qualifiers": ["skills"],
                                    },
                                }
                            ]
                        }
                    ),
                )
            self.assertEqual(data["statements"][0]["data"]["application_context"], None)
            self.assertFalse(data["statements"][0]["correction_allowed"])
            return accept_descriptions(data)

        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root)
            record = statement_finding(
                statement(
                    context="DemoWorks deploys Python.",
                    application_context="Radar processing",
                ),
                page,
                HtmlWindow(0, len(HTML), HTML),
            )
            record_id = record.record_id
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", ResearchConfig(), root)
                errors = await review_page_descriptions([record], [page], llm, root)
                self.assertEqual(errors, [])
                self.assertTrue(accepted_description(record))
                self.assertEqual(record.record_id, record_id)
                self.assertEqual(record.data["subject_name"], "DemoWorks")
                self.assertEqual(len(record.data["description_revisions"]), 1)
                await review_page_descriptions([record], [page], llm, root)
                self.assertEqual(len(requests), 2)
            record.data["context"] = "DemoWorks deploys Python in production."
            self.assertFalse(accepted_description(record))

    async def test_failed_correction_cannot_enter_normalization(self):
        requests = []

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n"
                )[1]
            )
            requests.append(data["task"])
            self.assertEqual(data["task"], "page_description_review")
            return httpx.Response(
                200,
                json=response(
                    {
                        "checks": [
                            {
                                "statement_id": "s1",
                                "supported": False,
                                "reason": "Usage remains unsupported.",
                                "correction": {
                                    "context": "DemoWorks uses Python.",
                                    "application_context": None,
                                    "qualifiers": [],
                                }
                                if len(requests) == 1
                                else None,
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root)
            record = statement_finding(
                statement(context="DemoWorks deploys Python for radar."),
                page,
                HtmlWindow(0, len(HTML), HTML),
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                result = await consolidate_page_statements(
                    [record],
                    [page],
                    catalog_fixture(),
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
            self.assertEqual(result["technology_signals"], [])
            self.assertEqual(result["pending_description_ids"], [record.record_id])
            self.assertEqual(result["pending_statement_ids"], [record.record_id])

    async def test_duplicate_description_verdict_holds_only_affected_statement(self):
        requests = []

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n"
                )[1]
            )
            ids = [item["statement_id"] for item in data["statements"]]
            requests.append(ids)
            checks = [
                {
                    "statement_id": key,
                    "supported": True,
                    "reason": "Listed skills",
                    "source_attribution": {
                        field: next(
                            item["data"]
                            for item in data["statements"]
                            if item["statement_id"] == key
                        )[field]
                        for field in ("subject_name", "subject_kind", "source_name")
                    },
                    "correction": None,
                }
                for key in ids
            ]
            if len(requests) == 1:
                checks += [
                    dict(checks[1]),
                    {
                        "statement_id": "unknown",
                        "supported": True,
                        "reason": "Ignore unknown",
                        "correction": None,
                    },
                ]
            return httpx.Response(200, json=response({"checks": checks}))

        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = HTML + "<p>Go</p>"
            page = save_page(root, html)
            records = [
                statement_finding(
                    statement(
                        source_name=name,
                        context=f"DemoWorks advertises {name} skills.",
                        evidence=[name],
                    ),
                    page,
                    HtmlWindow(0, len(html), html),
                )
                for name in ["Python", "Go"]
            ]
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_page_descriptions(
                    records,
                    [page],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
            self.assertEqual(requests, [["s1", "s2"], ["s2"]])
            self.assertTrue(all(accepted_description(record) for record in records))

    async def test_changed_html_cannot_reuse_description_approval(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root)
            record = statement_finding(
                statement(), page, HtmlWindow(0, len(HTML), HTML)
            )

            def handle(request):
                raise AssertionError("Source integrity must fail before the model call")

            (root / page.html_file).write_text("Changed", encoding="utf-8")
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                with self.assertRaisesRegex(ValueError, "hash mismatch"):
                    await review_page_descriptions(
                        [record],
                        [page],
                        ModelClient(client, "test", ResearchConfig(), root),
                        root,
                    )


class NormalizationRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def test_job_requirement_cannot_be_promoted_to_company_scope(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = HTML + "<h2>Engineer</h2><p>Python required.</p>"
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    job_title="Engineer",
                    context="The Engineer role requires Python.",
                    evidence=["Python required."],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            tech, _, _, errors = accept_statement_decisions(
                decisions(technology(signal="required_experience", scope="company")),
                {"s1": record},
                {page.page_id: page},
                root,
            )
            self.assertEqual(errors, {})
            self.assertEqual(tech[0].data["scope"], "role")
            self.assertEqual(tech[0].data["job_title"], "Engineer")
            self.assertEqual(tech[0].data["job_employer"], "DemoWorks")

    def test_keyed_decisions_preserve_good_neighbor_when_an_item_is_omitted(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = HTML + "<p>Go</p>"
            page = save_page(root, html)
            records = {
                key: statement_finding(
                    statement(source_name=name, evidence=[name]),
                    page,
                    HtmlWindow(0, len(html), html),
                )
                for key, name in [("s1", "Python"), ("s2", "Go")]
            }
            schema = statement_decision_schema(records)
            self.assertEqual(
                schema["properties"]["decisions"]["required"], ["s1", "s2"]
            )
            tech, _, _, issues = accept_statement_decisions(
                decisions(technology()), records, {page.page_id: page}, root
            )
            self.assertEqual([r.data["technology"] for r in tech], ["Python"])
            self.assertEqual(set(issues), {"s2"})
            forged = decisions(technology())
            forged["decisions"]["s1"]["technologies"][0]["technology"] = (
                "Unrelated tool"
            )
            tech, _, _, issues = accept_statement_decisions(
                forged, records, {page.page_id: page}, root
            )
            self.assertEqual(tech, [])
            self.assertEqual(set(issues), {"s1", "s2"})

    def test_credential_identity_and_version_come_from_source_with_future_status(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = "<h1>DemoWorks</h1><p>DemoWorks is working toward ISO 9001:2015 certification.</p>"
            page = save_page(root, html)
            record = statement_finding(
                statement(
                    kind="certification",
                    source_name="ISO 9001:2015",
                    context="DemoWorks is working toward ISO 9001:2015 certification.",
                    section_heading=None,
                    qualifiers=["working toward"],
                    evidence=[
                        "DemoWorks is working toward ISO 9001:2015 certification."
                    ],
                ),
                page,
                HtmlWindow(0, len(html), html),
            )
            value = {
                "decisions": {
                    "s1": {
                        "disposition": "certification",
                        "reason": "Future claim",
                        "technologies": [],
                        "certification": {
                            "claim_type": "working_toward",
                            "scope": None,
                            "issuer_or_assessor": None,
                            "certificate_or_report_id": None,
                            "issued_on": None,
                            "valid_until": None,
                            "document_url": None,
                            "document_type": None,
                        },
                    }
                }
            }
            tech, certs, _, errors = accept_statement_decisions(
                value, {"s1": record}, {page.page_id: page}, root
            )
            self.assertEqual(tech, [])
            self.assertEqual(errors, {})
            self.assertEqual(certs[0].data["standard_name"], "ISO 9001")
            self.assertEqual(certs[0].data["standard_version"], "2015")
            self.assertEqual(certs[0].data["subject_name"], "DemoWorks")
            self.assertEqual(certs[0].data["claim_type"], "working_toward")

    async def test_saved_normalization_and_description_are_reused_but_source_claim_is_checked(
        self,
    ):
        stages = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            stages.append(data["task"])
            if data["task"] == "page_description_review":
                return accept_descriptions(data)
            if data["task"] == "normalize_page_statements":
                return httpx.Response(200, json=response(decisions(technology())))
            reply = accepted_technology_reviews(body)
            if reply is None:
                raise AssertionError(data["task"])
            return reply

        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = save_page(root)
            record = statement_finding(
                statement(), page, HtmlWindow(0, len(HTML), HTML)
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", ResearchConfig(), root)
                first = await consolidate_page_statements(
                    [record], [page], catalog_fixture(), llm, root
                )
                second = await consolidate_page_statements(
                    [record],
                    [page],
                    catalog_fixture(),
                    llm,
                    root,
                    seed_normalization={
                        "technology_records": first["technology_signals"],
                        "certification_records": [],
                        "dispositions": [],
                    },
                )
            self.assertEqual(len(second["technology_summary"]), 1)
            self.assertEqual(second["pending_statement_ids"], [])
            self.assertEqual(
                stages,
                [
                    "page_description_review",
                    "normalize_page_statements",
                    "claim_review",
                    "claim_review",
                ],
            )

    def test_bad_item_schema_does_not_remove_valid_neighbor(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = HTML + "<p>Go</p>"
            page = save_page(root, html)
            records = {
                key: statement_finding(
                    statement(source_name=name, evidence=[name]),
                    page,
                    HtmlWindow(0, len(html), html),
                )
                for key, name in [("s1", "Python"), ("s2", "Go")]
            }
            document = {
                "technologies": [
                    technology(),
                    {"statement_ids": ["s2"], "technology": "Go"},
                ],
                "certifications": [],
                "exclusions": [],
            }
            tech, certs, excluded, issues = accept_normalization_items(
                document, records, {page.page_id: page}, root
            )
            self.assertEqual([r.data["technology"] for r in tech], ["Python"])
            self.assertEqual(certs + excluded, [])
            self.assertEqual(set(issues), {"s2"})
            document["exclusions"] = [
                {"statement_id": "s2", "disposition": "excluded", "reason": "Conflict"}
            ]
            tech, _, _, issues = accept_normalization_items(
                document, records, {page.page_id: page}, root
            )
            self.assertEqual(len(tech), 1)
            self.assertEqual(set(issues), {"s2"})

    async def test_only_failed_normalization_is_retried_and_good_records_survive_timeout(
        self,
    ):
        requests = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            if data["task"] == "page_description_review":
                return accept_descriptions(data)
            if data["task"] == "normalize_page_statements":
                ids = [item["statement_id"] for item in data["statements"]]
                requests.append(ids)
                if len(requests) == 1:
                    return httpx.Response(
                        200,
                        json=response(
                            decisions(
                                technology(),
                                technology(
                                    signal="unsupported_signal", statement_ids=["s2"]
                                ),
                            )
                        ),
                    )
                raise httpx.ReadTimeout("Retry transport failed", request=request)
            reviewed = accepted_technology_reviews(body)
            if reviewed is None:
                raise AssertionError(data["task"])
            return reviewed

        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = HTML + "<p>Go</p>"
            page = save_page(root, html)
            records = [
                statement_finding(
                    statement(source_name=name, evidence=[name]),
                    page,
                    HtmlWindow(0, len(html), html),
                )
                for name in ["Python", "Go"]
            ]
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                result = await consolidate_page_statements(
                    records,
                    [page],
                    catalog_fixture(),
                    ModelClient(
                        client, "test", ResearchConfig(max_http_attempts=1), root
                    ),
                    root,
                )
            self.assertEqual(requests, [["s1", "s2"], ["s2"]])
            self.assertEqual(
                [r["data"]["technology"] for r in result["technology_signals"]],
                ["Python"],
            )
            self.assertEqual(result["pending_statement_ids"], [records[1].record_id])
            self.assertEqual(len(result["technology_summary"]), 1)
