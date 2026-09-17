"""HTTP-boundary regressions for independent source and catalog approval stages."""

import json
import unittest
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx
from test_catalog_search import accepted_technology_reviews, catalog_fixture, proposal
from test_package import page, response
from test_technologies import finding, signal

from company_research.analytics import (
    accepted_finding,
    source_supported_finding,
    summarize_technologies,
)
from company_research.content import HtmlWindow, merge_finding, proposal_metadata_hash
from company_research.llm import ModelClient
from company_research.models import RECORD_TYPES, ResearchConfig
from company_research.research import extract_window, process_technology_metadata
from company_research.review import (
    correct_reviewed_claims,
    repair_proposal_metadata,
    review_claims,
    review_proposals,
)


def proposed_record():
    html = "<h1>DemoWorks Engineer</h1><p>Our team uses Yocto.</p>"
    record = finding(
        signal(
            technology="Yocto",
            evidence=["DemoWorks", "Engineer", "Our team uses Yocto."],
        ),
        html,
    )
    record.data["catalog_match"] = proposal()
    record.data["catalog_match"]["proposed_technology"]["category_suggestion"] = (
        "Electronic design automation"
    )
    return record


class MetadataRepairTests(unittest.IsolatedAsyncioTestCase):
    async def test_repairs_metadata_only_rechecks_and_skips_already_accepted_draft(
        self,
    ):
        record = proposed_record()
        before = record.model_copy(deep=True)
        tasks = []

        def handle(request):
            body = json.loads(request.content)
            data = json.loads(
                body["messages"][1]["content"].split("INPUT DATA:\n", 1)[1]
            )
            tasks.append(data["task"])
            if data["task"] == "proposal_metadata_repair":
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "repairs": [
                                {
                                    "record_id": "r1",
                                    "description": "Tools for building embedded Linux systems.",
                                    "category_ids": [],
                                    "category_suggestion": "Embedded Linux build tools",
                                }
                            ]
                        }
                    ),
                )
            valid = (
                data["proposals"][0]["proposal"]["category_suggestion"]
                == "Embedded Linux build tools"
            )
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": "r1",
                                "identity_supported": True,
                                "category_supported": valid,
                                "description_supported": valid,
                                "reason": "Check the draft category and definition",
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", ResearchConfig(), root)
                errors = await review_proposals(
                    [record], llm, root, "first", catalog_fixture().category_options()
                )
                self.assertEqual(errors, [])
                self.assertTrue(source_supported_finding(record))
                self.assertTrue(accepted_finding(record))
                self.assertEqual(
                    tasks,
                    ["proposal_review", "proposal_metadata_repair", "proposal_review"],
                )
                await review_proposals(
                    [record], llm, root, "resume", catalog_fixture().category_options()
                )
                self.assertEqual(len(tasks), 3)
        self.assertEqual(record.sources, before.sources)
        for key in ("technology", "company", "signal", "scope", "context", "job_url"):
            self.assertEqual(record.data[key], before.data[key])
        for key in ("name", "website", "saas", "oss", "pricing"):
            self.assertEqual(
                record.data["catalog_match"]["proposed_technology"][key],
                before.data["catalog_match"]["proposed_technology"][key],
            )
        self.assertEqual(len(record.data["proposal_metadata_repairs"]), 1)
        duplicate = before.model_copy(deep=True)
        for key in ("catalog_match", "proposal_review", "proposal_metadata_repairs"):
            duplicate.data.pop(key, None)
        merged = [record.model_copy(deep=True)]
        merge_finding(merged, duplicate)
        self.assertEqual(len(merged[0].data["proposal_metadata_repairs"]), 1)
        record.data["catalog_match"]["proposed_technology"]["description"] = (
            "Changed after review"
        )
        self.assertTrue(source_supported_finding(record))
        self.assertFalse(accepted_finding(record))

    async def test_identity_rejection_is_not_repaired_or_promoted(self):
        record = proposed_record()
        calls = []

        def handle(request):
            calls.append(request)
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": "r1",
                                "identity_supported": False,
                                "category_supported": False,
                                "description_supported": False,
                                "reason": "Identity needs administrator investigation",
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_proposals(
                    [record],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "identity",
                    catalog_fixture().category_options(),
                )
        self.assertEqual(len(calls), 1)
        self.assertTrue(source_supported_finding(record))
        self.assertFalse(accepted_finding(record))
        self.assertNotIn("proposal_metadata_repairs", record.data)

    async def test_unknown_category_id_cannot_replace_rejected_metadata(self):
        record = proposed_record()
        draft = deepcopy(record.data["catalog_match"]["proposed_technology"])
        record.data["proposal_review"] = {
            "status": "rejected",
            "identity_supported": True,
            "category_supported": False,
            "description_supported": True,
            "reason": "Wrong category",
            "metadata_sha256": proposal_metadata_hash(draft),
        }

        def handle(request):
            return httpx.Response(
                200,
                json=response(
                    {
                        "repairs": [
                            {
                                "record_id": "r1",
                                "description": draft["description"],
                                "category_ids": [999],
                                "category_suggestion": None,
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await repair_proposal_metadata(
                    [record],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "category",
                    catalog_fixture().category_options(),
                )
        self.assertEqual(record.data["catalog_match"]["proposed_technology"], draft)
        self.assertIn(
            "unknown category IDs", record.data["proposal_metadata_repairs"][0]["error"]
        )
        self.assertFalse(accepted_finding(record))

    async def test_metadata_transport_failure_resumes_without_extraction_or_source_review(
        self,
    ):
        html = "<h1>DemoWorks Engineer</h1><p>Our team uses Yocto.</p>"
        extraction = {key: [] for key in RECORD_TYPES}
        extraction["technology_signals"] = [
            signal(
                technology="Yocto",
                evidence=["DemoWorks", "Engineer", "Our team uses Yocto."],
            )
        ]
        source = page(html, "https://example.test/jobs/engineer")
        requests = []
        fail_resolution = True

        def handle(request):
            body = json.loads(request.content)
            prompt = body["messages"][1]["content"]
            if '"task": "technology_resolution"' in prompt:
                requests.append("resolve")
                if fail_resolution:
                    return httpx.Response(503)
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
            reviewed = accepted_technology_reviews(body)
            if reviewed is not None:
                requests.append(
                    "proposal_review"
                    if '"task": "proposal_review"' in prompt
                    else "source_review"
                )
                return reviewed
            requests.append("extract")
            return httpx.Response(200, json=response(extraction))

        config = ResearchConfig(
            max_http_attempts=1, max_review_attempts=1, max_corrections=0
        )
        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                records, complete, errors, _ = await extract_window(
                    HtmlWindow(0, len(html), html),
                    source,
                    ModelClient(client, "test", config, root),
                    root,
                    0,
                    catalog_fixture(),
                )
                self.assertTrue(
                    complete,
                    f"Catalog failure must not schedule the HTML extraction again: {errors}",
                )
                self.assertTrue(errors)
                self.assertTrue(
                    all(error.startswith("technology_metadata:") for error in errors)
                )
                record = records[0][1]
                self.assertTrue(source_supported_finding(record))
                self.assertFalse(accepted_finding(record))
                self.assertIn("catalog_error", record.data)
                fail_resolution = False
                await process_technology_metadata(
                    [record],
                    catalog_fixture(),
                    ModelClient(client, "test", config, root),
                    root,
                    "retry-metadata",
                )
                self.assertNotIn("catalog_error", record.data)
                self.assertTrue(accepted_finding(record))
        self.assertEqual(
            requests,
            ["extract", "source_review", "resolve", "resolve", "proposal_review"],
        )

    async def test_expertise_correction_preserves_original_and_saved_quotations(self):
        html = "<h1>DemoWorks</h1><h2>Engineering skills</h2><p>We have experience with Ansys HFSS.</p>"
        record = finding(
            signal(
                technology="Ansys HFSS",
                scope="company",
                job_title=None,
                job_url=None,
                evidence=[
                    "DemoWorks",
                    "Engineering skills",
                    "We have experience with Ansys HFSS.",
                ],
            ),
            html,
        )

        def handle(request):
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n", 1
                )[1]
            )
            for claim in data["claims"]:
                self.assertNotIn("context", claim["data"])
                self.assertNotIn("proposed_technology", claim)
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": claim["record_id"],
                                "specific_technology": True,
                                "supported": claim["data"]["signal"]
                                == "advertised_expertise",
                                "reason": "Advertised engineering skills, no stated deployment",
                                "source_signal": "advertised_expertise",
                                "source_scope": "company",
                                "source_subject": claim["data"].get("company"),
                                "source_subject_kind": "company"
                                if claim["data"].get("company")
                                else "unknown",
                                "source_object": None,
                            }
                            for claim in data["claims"]
                        ]
                    }
                ),
            )

        record.data["catalog_match"] = proposal()
        record.data["catalog_match"]["proposed_technology"]["name"] = "Ansys HFSS"
        record.data["proposal_review"] = {
            "status": "accepted",
            "identity_supported": True,
            "category_supported": True,
            "description_supported": True,
            "metadata_sha256": proposal_metadata_hash(
                record.data["catalog_match"]["proposed_technology"]
            ),
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "html").mkdir()
            (root / "html" / f"{record.sources[0].page_id}.html").write_text(
                html, encoding="utf-8"
            )
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                llm = ModelClient(client, "test", ResearchConfig(), root)
                await review_claims([record], llm, root, "expertise")
                corrections = await correct_reviewed_claims(
                    [record], llm, root, "expertise"
                )
        self.assertFalse(accepted_finding(record))
        self.assertEqual(record.data["signal"], "stated_use")
        self.assertEqual(len(corrections), 1)
        corrected = corrections[0]
        self.assertTrue(accepted_finding(corrected))
        self.assertEqual(corrected.data["correction_of"], record.record_id)
        self.assertEqual(
            corrected.data["proposal_review"], record.data["proposal_review"]
        )
        self.assertEqual(corrected.sources[0].evidence, record.sources[0].evidence)
        self.assertNotEqual(corrected.record_id, record.record_id)
        summaries = summarize_technologies([record, corrected])
        self.assertEqual(summaries[0].signal, "advertised_expertise")

    async def test_exact_catalog_retry_clears_obsolete_proposal_gate_without_model_call(
        self,
    ):
        html = "<h1>DemoWorks Engineer</h1><p>Our team develops tools in Python.</p>"
        record = finding(signal(), html)
        record.data["catalog_match"] = proposal()
        record.data["catalog_error"] = "Previous processing failed"
        record.data["proposal_review"] = {"status": "rejected"}
        record.data["required_reviews"] = ["proposal_metadata"]
        previous = record.model_copy(deep=True)

        def handle(request):
            raise AssertionError("Exact catalog lookup should not call the model")

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await process_technology_metadata(
                    [record],
                    catalog_fixture(),
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "exact-retry",
                )
        self.assertTrue(accepted_finding(record))
        self.assertEqual(record.data["catalog_match"]["canonical_technology"], "Python")
        self.assertNotIn("catalog_error", record.data)
        self.assertNotIn("proposal_review", record.data)
        self.assertNotIn("proposal_metadata", record.data["required_reviews"])
        merged = [previous]
        merge_finding(merged, record)
        self.assertTrue(accepted_finding(merged[0]))

    async def test_unchanged_valid_metadata_can_be_rechecked_within_repair_budget(self):
        record = proposed_record()
        record.data["catalog_match"]["proposed_technology"] = proposal()[
            "proposed_technology"
        ]
        original = deepcopy(record.data["catalog_match"])
        review_calls = 0

        def handle(request):
            nonlocal review_calls
            data = json.loads(
                json.loads(request.content)["messages"][1]["content"].split(
                    "INPUT DATA:\n", 1
                )[1]
            )
            if data["task"] == "proposal_metadata_repair":
                draft = data["proposals"][0]["proposal"]
                return httpx.Response(
                    200,
                    json=response(
                        {
                            "repairs": [
                                {
                                    "record_id": "r1",
                                    **{
                                        key: draft[key]
                                        for key in [
                                            "description",
                                            "category_ids",
                                            "category_suggestion",
                                        ]
                                    },
                                }
                            ]
                        }
                    ),
                )
            review_calls += 1
            return httpx.Response(
                200,
                json=response(
                    {
                        "reviews": [
                            {
                                "record_id": "r1",
                                "identity_supported": True,
                                "description_supported": True,
                                "category_supported": review_calls > 1,
                                "reason": "This descriptive category fits the product.",
                            }
                        ]
                    }
                ),
            )

        with TemporaryDirectory() as directory:
            root = Path(directory)
            async with httpx.AsyncClient(
                base_url="https://test.invalid/", transport=httpx.MockTransport(handle)
            ) as client:
                await review_proposals(
                    [record],
                    ModelClient(client, "test", ResearchConfig(), root),
                    root,
                    "unchanged",
                    catalog_fixture().category_options(),
                )
        self.assertTrue(accepted_finding(record))
        self.assertEqual(review_calls, 2)
        self.assertEqual(record.data["catalog_match"], original)
        self.assertEqual(len(record.data["proposal_metadata_repairs"]), 1)
