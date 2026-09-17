"""Source, attribution and HTTP boundaries for page descriptions and normalization."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import accepted_technology_reviews, catalog_fixture
from test_package import page, response
from test_technologies import signal

from company_research.analytics import accepted_finding
from company_research.content import HtmlWindow
from company_research.llm import ModelClient
from company_research.models import Finding, ResearchConfig
from company_research.statements import (
    NormalizedStatements,
    PageStatement,
    consolidate_page_statements,
    extract_page_statements,
    normalized_statement_findings,
    statement_finding,
)

HTML = "<h1>DemoWorks</h1><h2>Engineering skills</h2><p>Python</p>"


def statement(**changes):
    return PageStatement.model_validate(
        {
            "kind": "technology",
            "subject_name": "DemoWorks",
            "subject_kind": "company",
            "source_name": "Python",
            "context": "DemoWorks advertises Python skills.",
            "application_context": None,
            "section_heading": "Engineering skills",
            "job_title": None,
            "qualifiers": ["skills"],
            "evidence": ["DemoWorks", "Engineering skills", "Python"],
        }
        | changes
    )


def technology(**changes):
    return (
        signal(
            company="DemoWorks",
            technology="Python",
            signal="advertised_expertise",
            scope="company",
            job_employer=None,
            job_title=None,
            job_url=None,
            evidence=["Generated quotations must not become evidence."],
        )
        | {"statement_ids": ["s1"]}
        | changes
    )


def normalized(**changes):
    return NormalizedStatements.model_validate(
        {"technologies": [technology()], "certifications": [], "exclusions": []}
        | changes
    )


def decisions(*observations):
    values = {}
    for observation in observations:
        for key in observation["statement_ids"]:
            decision = values.setdefault(
                key,
                {
                    "disposition": "technology",
                    "reason": "Source interpretation",
                    "technologies": [],
                    "certification": None,
                },
            )
            decision["technologies"].append(
                {
                    field: observation[field]
                    for field in (
                        "category",
                        "signal",
                        "scope",
                        "alternative_group",
                        "as_of",
                    )
                }
            )
    return {"decisions": values}


def save_page(root, html=HTML, page_id="p0001", url="https://example.test/skills"):
    source = page(html, url)
    source.page_id = page_id
    source.html_file = f"html/{page_id}.html"
    (root / "html").mkdir(exist_ok=True)
    (root / source.html_file).write_text(html, encoding="utf-8")
    return source


def accept_descriptions(data):
    return httpx.Response(
        200,
        json=response(
            {
                "checks": [
                    {
                        "statement_id": item["statement_id"],
                        "supported": True,
                        "reason": "Source establishes the described skills context.",
                        "source_attribution": {
                            field: item["data"][field]
                            for field in ("subject_name", "subject_kind", "source_name")
                        },
                        "correction": None,
                    }
                    for item in data["statements"]
                ]
            }
        ),
    )


class StatementBoundaryTests(unittest.TestCase):
    def test_repeated_context_combines_with_original_sources_and_ignores_generated_quotes(
        self,
    ):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            first = save_page(root)
            second = save_page(
                root, page_id="p0002", url="https://example.test/services"
            )
            statements = {
                key: statement_finding(
                    statement(), source, HtmlWindow(0, len(HTML), HTML)
                )
                for key, source in [("s1", first), ("s2", second)]
            }
            tech, certs, excluded = normalized_statement_findings(
                normalized(technologies=[technology(statement_ids=["s1", "s2"])]),
                statements,
                {p.page_id: p for p in [first, second]},
                root,
            )
            self.assertEqual(len(tech), 1)
            self.assertEqual(certs + excluded, [])
            self.assertEqual(
                {s.url for s in tech[0].sources}, {first.source_url, second.source_url}
            )
            self.assertEqual(len(tech[0].data["page_contexts"]), 2)
            self.assertTrue(
                all(
                    "Python" in {fragment.text for fragment in s.evidence}
                    for s in tech[0].sources
                )
            )
            self.assertIsNone(tech[0].data["page_contexts"][0]["application_context"])
            self.assertNotEqual(statements["s1"].record_id, statements["s2"].record_id)

            # A model can emit equivalent records separately instead of combining IDs.
            separate, _, _ = normalized_statement_findings(
                normalized(
                    technologies=[
                        technology(statement_ids=["s1"]),
                        technology(statement_ids=["s2"]),
                    ]
                ),
                statements,
                {p.page_id: p for p in [first, second]},
                root,
            )
            self.assertEqual(len(separate), 1)
            self.assertEqual(len(separate[0].data["page_contexts"]), 2)
            self.assertEqual(
                set(separate[0].data["statement_ids"]),
                {record.record_id for record in statements.values()},
            )

    def test_normalization_cannot_transfer_a_technology_to_another_company(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            records = {
                "s1": statement_finding(
                    statement(), source, HtmlWindow(0, len(HTML), HTML)
                )
            }
            with self.assertRaisesRegex(ValueError, "between source subjects"):
                normalized_statement_findings(
                    normalized(technologies=[technology(company="OtherCompany")]),
                    records,
                    {source.page_id: source},
                    root,
                )

    def test_normalization_cannot_assign_a_different_item_to_a_statement_id(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            records = {
                "s1": statement_finding(
                    statement(), source, HtmlWindow(0, len(HTML), HTML)
                )
            }
            with self.assertRaisesRegex(ValueError, "Statement s1.*'Go'.*'Python'"):
                normalized_statement_findings(
                    normalized(technologies=[technology(technology="Go")]),
                    records,
                    {source.page_id: source},
                    root,
                )

    def test_jobs_cannot_be_combined_into_a_company_stack(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = HTML + "<h2>Engineer</h2>"
            source = save_page(root, html)
            records = {
                "s1": statement_finding(
                    statement(
                        job_title="Engineer",
                        evidence=[
                            "DemoWorks",
                            "Engineering skills",
                            "Engineer",
                            "Python",
                        ],
                    ),
                    source,
                    HtmlWindow(0, len(html), html),
                )
            }
            with self.assertRaisesRegex(ValueError, "different jobs"):
                normalized_statement_findings(
                    normalized(), records, {source.page_id: source}, root
                )

    def test_person_credential_cannot_be_transferred_to_company(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            html = "<h1>DemoWorks</h1><p>Our experts are ISO 27001 certified.</p>"
            source = save_page(root, html)
            record = statement_finding(
                statement(
                    kind="certification",
                    source_name="ISO 27001",
                    subject_name=None,
                    subject_kind="person",
                    section_heading=None,
                    context="The page describes certified experts.",
                    evidence=["Our experts are ISO 27001 certified."],
                ),
                source,
                HtmlWindow(0, len(html), html),
            )
            document = normalized(
                technologies=[],
                certifications=[
                    {
                        "subject_name": None,
                        "subject_kind": "company",
                        "standard_name": "ISO 27001",
                        "standard_version": None,
                        "claim_type": "certification",
                        "scope": None,
                        "issuer_or_assessor": None,
                        "certificate_or_report_id": None,
                        "issued_on": None,
                        "valid_until": None,
                        "document_url": None,
                        "document_type": None,
                        "evidence": ["ISO 27001"],
                        "statement_ids": ["s1"],
                    }
                ],
            )
            with self.assertRaisesRegex(ValueError, "holder kinds"):
                normalized_statement_findings(
                    document, {"s1": record}, {source.page_id: source}, root
                )

    def test_missing_unknown_and_conflicting_statement_dispositions_fail(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            records = {
                "s1": statement_finding(
                    statement(), source, HtmlWindow(0, len(HTML), HTML)
                )
            }
            for document in [
                normalized(technologies=[]),
                normalized(technologies=[technology(statement_ids=["unknown"])]),
                normalized(
                    exclusions=[
                        {
                            "statement_id": "s1",
                            "disposition": "excluded",
                            "reason": "Conflicting duplicate",
                        }
                    ]
                ),
            ]:
                with self.subTest(document=document.model_dump()):
                    with self.assertRaises(ValueError):
                        normalized_statement_findings(
                            document, records, {source.page_id: source}, root
                        )
            _, _, dispositions = normalized_statement_findings(
                normalized(
                    technologies=[],
                    exclusions=[
                        {
                            "statement_id": "s1",
                            "disposition": "needs_review",
                            "reason": "Uncertain identity",
                        }
                    ],
                ),
                records,
                {source.page_id: source},
                root,
            )
            self.assertEqual(dispositions[0]["statement_id"], records["s1"].record_id)

    def test_company_background_cannot_supply_a_missing_technology(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            record = statement_finding(
                statement(kind="company_context", source_name=None),
                source,
                HtmlWindow(0, len(HTML), HTML),
            )
            with self.assertRaisesRegex(ValueError, "primary statements"):
                normalized_statement_findings(
                    normalized(), {"s1": record}, {source.page_id: source}, root
                )

    def test_missing_subject_or_heading_is_not_accepted_as_quoted_context(self):
        for changes in [
            {"subject_name": "OtherCompany"},
            {"section_heading": "Current deployed stack"},
        ]:
            with self.subTest(changes=changes):
                record = statement_finding(
                    statement(**changes), page(HTML), HtmlWindow(0, len(HTML), HTML)
                )
                self.assertEqual(record.evidence_status, "needs_review")

    def test_literal_fields_supply_anchors_without_removing_relation_quotations(self):
        html = HTML + "<p>Listed skills are offered as consulting expertise.</p>"
        record = statement_finding(
            statement(evidence=["Listed skills are offered as consulting expertise."]),
            page(html),
            HtmlWindow(0, len(html), html),
        )
        self.assertEqual(record.evidence_status, "source_matched")
        self.assertEqual(
            {fragment.text for fragment in record.sources[0].evidence},
            {
                "DemoWorks",
                "Python",
                "Engineering skills",
                "Listed skills are offered as consulting expertise.",
            },
        )

    def test_short_technology_name_requires_a_separate_token(self):
        for html, status in [
            (HTML + "<p>CATIA</p>", "needs_review"),
            (HTML + "<p>C/C++</p>", "source_matched"),
        ]:
            with self.subTest(html=html):
                record = statement_finding(
                    statement(source_name="C", evidence=["Engineering skills"]),
                    page(html),
                    HtmlWindow(0, len(html), html),
                )
                self.assertEqual(record.evidence_status, status)


class StatementHTTPTests(unittest.IsolatedAsyncioTestCase):
    async def test_quotation_repair_preserves_description_and_does_not_reextract_page(
        self,
    ):
        stages = []
        original = statement(evidence=["Engineering skills: Python"])

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            stages.append(data["task"])
            if data["task"] == "page_statements":
                return httpx.Response(
                    200, json=response({"statements": [original.model_dump()]})
                )
            self.assertEqual(data["task"], "statement_evidence_repair")
            self.assertEqual(
                data["records"][0]["data"], original.model_dump(exclude={"evidence"})
            )
            return httpx.Response(
                200,
                json=response(
                    {
                        "repairs": [
                            {
                                "record_id": "s1",
                                "evidence": ["Engineering skills", "Python"],
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                records = await extract_page_statements(
                    source, ModelClient(client, "test", ResearchConfig(), root), root
                )
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0].data, original.model_dump(exclude={"evidence"}))
            self.assertEqual(records[0].evidence_status, "source_matched")
            self.assertEqual(len(records[0].sources), 2)
            self.assertEqual(records[0].sources[0].evidence_status, "needs_review")
            self.assertEqual(records[0].sources[1].evidence_status, "source_matched")
        self.assertEqual(stages, ["page_statements", "statement_evidence_repair"])

    async def test_page_context_requests_do_not_include_other_pages_or_catalog(self):
        prompts = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            prompts.append(data)
            self.assertNotIn("tools", body)
            self.assertEqual(
                set(data),
                {"task", "source_url", "page_id", "cleaned_html", "observed_headings"},
            )
            return httpx.Response(
                200, json=response({"statements": [statement().model_dump()]})
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            one = save_page(root)
            two = save_page(root, page_id="p0002", url="https://example.test/services")
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", ResearchConfig(), root)
                first = await extract_page_statements(one, llm, root)
                second = await extract_page_statements(two, llm, root)
            self.assertEqual(len(first), 1)
            self.assertEqual(first[0].sources[0].page_id, "p0001")
            self.assertEqual(second[0].sources[0].page_id, "p0002")
            self.assertEqual(len(list((root / "page-statements").glob("*.json"))), 2)
        self.assertEqual([p["page_id"] for p in prompts], ["p0001", "p0002"])

    async def test_false_compressed_usage_is_rejected_against_original_source(self):
        stages = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            stages.append(data["task"])
            if data["task"] == "page_description_review":
                return accept_descriptions(data)
            if data["task"] == "normalize_page_statements":
                return httpx.Response(
                    200,
                    json=response(
                        decisions(
                            technology(
                                signal="stated_use",
                                context="The company uses Python in production.",
                            )
                        )
                    ),
                )
            self.assertEqual(data["task"], "claim_review")
            for claim in data["claims"]:
                self.assertNotIn("context", claim["data"])
                self.assertNotIn("page_contexts", claim["data"])
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": claim["record_id"],
                                "supported": claim["data"]["signal"]
                                == "advertised_expertise",
                                "reason": "Skills establish expertise, not deployment.",
                                "source_subject": claim["data"].get("company"),
                                "source_subject_kind": "company"
                                if claim["data"].get("company")
                                else "unknown",
                                "source_object": None,
                                "specific_technology": True,
                                "source_signal": "advertised_expertise",
                                "source_scope": "company",
                            }
                            for claim in data["claims"]
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            record = statement_finding(
                statement(),
                source,
                HtmlWindow(0, len(HTML), HTML),
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                result = await consolidate_page_statements(
                    [record],
                    [source],
                    catalog_fixture(),
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
            self.assertFalse(
                accepted_finding(
                    Finding.model_validate(result["technology_signals"][0])
                )
            )
            self.assertEqual(
                result["technology_summary"][0]["signal"], "advertised_expertise"
            )
            self.assertEqual(
                result["technology_signals"][0]["data"]["context"], statement().context
            )
        self.assertEqual(
            stages,
            [
                "page_description_review",
                "normalize_page_statements",
                "claim_review",
                "claim_review",
            ],
        )

    async def test_known_technology_reuses_catalog_after_context_normalization(self):
        stages = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(body["messages"][1]["content"].split("INPUT DATA:\n")[1])
            stages.append(data["task"])
            if data["task"] == "page_description_review":
                return accept_descriptions(data)
            if data["task"] == "normalize_page_statements":
                return httpx.Response(200, json=response(decisions(technology())))
            reviewed = accepted_technology_reviews(body)
            if reviewed is None:
                raise AssertionError(
                    "Known catalog identity should not need a model catalog request"
                )
            return reviewed

        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = save_page(root)
            record = statement_finding(
                statement(), source, HtmlWindow(0, len(HTML), HTML)
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                result = await consolidate_page_statements(
                    [record],
                    [source],
                    catalog_fixture(),
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                )
            self.assertEqual(
                result["technology_signals"][0]["data"]["catalog_match"][
                    "canonical_technology"
                ],
                "Python",
            )
            self.assertEqual(
                result["technology_summary"][0]["signal"], "advertised_expertise"
            )
            self.assertEqual(result["pending_statement_ids"], [])
        self.assertEqual(
            stages,
            ["page_description_review", "normalize_page_statements", "claim_review"],
        )
