"""Jev routing is opt-in, budgeted, encrypted and changes actual crawl choices."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from pydantic import ValidationError
from test_crawl import browser_responses
from test_llm_profile import API_KEY, KEY, profile_payload
from test_package import response
from test_site_info import COMPANY, HTML, SITE

from crawler_service.crawl import crawl_company
from crawler_service.discovery import CrawlQueue
from crawler_service.jev import JevClient
from crawler_service.llm import ModelBudgetExceeded, ModelClient, ModelUnavailable
from crawler_service.models import RequestedContentAssessment, ResearchConfig
from crawler_service.service import CrawlRequest


def decisions_payload(request, selected=None):
    body = json.loads(request.content)
    selected = selected or {}
    return {
        "model": body["model"],
        "answers": {
            key: {
                "type": "choice",
                "choice": selected.get(key, next(iter(question["criteria"]))),
                "confidence": 0.9,
            }
            for key, question in body["questions"].items()
        },
        "usage": {"input_tokens": 123, "output_tokens": 9, "cost": 0.0001},
    }


class JevTests(unittest.IsolatedAsyncioTestCase):
    async def test_company_ranking_uses_choice_probability_and_includes_no_match(self):
        companies = [
            {"company_id": "5560123456", "legal_name": "Example AB"},
            {"company_id": "5560999999", "legal_name": "Unrelated AB"},
        ]

        def boundary(request):
            payload = decisions_payload(
                request,
                {
                    "operator": "5560123456",
                    "basis_5560123456": "registration_number",
                    "basis_5560999999": "insufficient",
                },
            )
            payload["answers"]["operator"].update(
                confidence=0.4,
                probabilities={"none": 0.05, "5560123456": 0.9, "5560999999": 0.05},
            )
            return httpx.Response(200, json=payload)

        async with httpx.AsyncClient(transport=httpx.MockTransport(boundary)) as http:
            budget = ModelClient(http, "processing-key", ResearchConfig(), None)
            ranked = await JevClient(
                http, API_KEY, "typesafe/jev-1.13", ["company_match"], budget
            ).rank_companies([], companies)
        self.assertEqual(ranked["assessment"]["confidence"], 0.9)
        self.assertEqual(ranked["assessment"]["basis"], "registration_number")
        self.assertEqual(ranked["no_match_probability"], 0.05)
        self.assertEqual(len(ranked["candidates"]), 2)

    async def test_company_ranking_rejects_missing_or_invalid_probabilities(self):
        companies = [{"company_id": "5560123456", "legal_name": "Example AB"}]
        for probabilities in [
            None,
            {"none": 0.1},
            {"none": -0.1, "5560123456": 1.1},
            {"none": 0.1, "5560123456": 0.2},
        ]:

            def boundary(request, distribution=probabilities):
                payload = decisions_payload(request)
                payload["answers"]["operator"]["probabilities"] = distribution
                return httpx.Response(200, json=payload)

            async with httpx.AsyncClient(
                transport=httpx.MockTransport(boundary)
            ) as http:
                budget = ModelClient(http, "processing-key", ResearchConfig(), None)
                with self.assertRaisesRegex(
                    ModelUnavailable, "probability distribution"
                ):
                    await JevClient(
                        http, API_KEY, "typesafe/jev-1.13", ["company_match"], budget
                    ).rank_companies([], companies)

    async def test_typed_calls_share_budget_usage_and_redacted_artifacts(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(
                    lambda request: httpx.Response(
                        200,
                        json=decisions_payload(
                            request,
                            {"site_type": "online_store", "company_operator": "yes"},
                        ),
                    )
                )
            ) as http:
                budget = ModelClient(
                    http, "processing-key", ResearchConfig(max_model_calls=1), root
                )
                jev = JevClient(
                    http, API_KEY, "typesafe/jev-1.13", ["site_eligibility"], budget
                )
                decision = await jev.site_eligibility(SITE, HTML)
                self.assertEqual(decision["crawl_decision"], "skip_crawling")
                self.assertTrue(decision["reasons"])
                self.assertEqual(budget.usage()["prompt_tokens"], 123)
                self.assertEqual(budget.usage()["completion_tokens"], 9)
                self.assertEqual(budget.usage()["known_cost_usd"], 0.0001)
                self.assertEqual(budget.remaining, 0)
                with self.assertRaises(ModelBudgetExceeded):
                    await jev.site_eligibility(SITE, HTML)
                artifact = (root / "calls/00001.json").read_text()
                self.assertIn("site_eligibility", artifact)
                self.assertNotIn(API_KEY, artifact)

    async def test_incomplete_decision_fails_without_silent_processing_fallback(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"answers": {}, "usage": {"input_tokens": 10}}
                )
            )
        ) as http:
            budget = ModelClient(http, "processing-key", ResearchConfig(), None)
            jev = JevClient(
                http, API_KEY, "typesafe/jev-1.13", ["site_eligibility"], budget
            )
            with self.assertRaisesRegex(ModelUnavailable, "missing"):
                await jev.site_eligibility(SITE, HTML)
            self.assertEqual(len(budget.calls), 1)

    async def test_link_decisions_select_real_candidates_and_keep_source_quotes(self):
        queue = CrawlQueue(SITE, ResearchConfig(), instructions="Find annual reports")
        queue.add(
            "https://parent.test/investors",
            source=SITE,
            label="Example is a subsidiary of Parent Inc",
            context={
                "source_url": SITE,
                "anchor_text": "Example is a subsidiary of Parent Inc",
            },
        )
        batch = queue.assessment_batch()
        cid = batch[0].candidate_id

        def boundary(request):
            return httpx.Response(
                200,
                json=decisions_payload(
                    request,
                    {
                        f"{cid}_content": "high_navigation",
                        f"{cid}_target": "related_company",
                        f"{cid}_scope": "source_0",
                    },
                ),
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(boundary)) as http:
            budget = ModelClient(http, "processing-key", ResearchConfig(), None)
            reply = await JevClient(
                http, API_KEY, "typesafe/jev-1.13", ["link_selection"], budget
            ).assess_links(queue, batch)
        result = RequestedContentAssessment.model_validate(
            reply.document["assessments"][0]
        )
        self.assertEqual(result.follow_scope, "source_navigation")
        self.assertEqual(
            result.navigation_source.evidence, "Example is a subsidiary of Parent Inc"
        )
        self.assertIsNone(queue.navigation_source_error(batch[0], result))

    async def test_site_decision_controls_crawl_and_keeps_description_from_processing(
        self,
    ):
        requested = []

        async def boundary(client, request, **kwargs):
            if request.url.path.endswith("/decisions"):
                return httpx.Response(
                    200,
                    json=decisions_payload(
                        request, {"site_type": "news_media", "company_operator": "yes"}
                    ),
                )
            if request.url.path.endswith("/chat/completions"):
                return httpx.Response(200, json=response(COMPANY))
            return httpx.Response(404)

        with (
            TemporaryDirectory() as folder,
            patch(
                "crawler_service.crawl.open_browser",
                lambda: browser_responses(
                    {SITE: (HTML, [{"href": SITE + "jobs"}], 200, None)}, requested
                ),
            ),
            patch.object(httpx.AsyncClient, "send", boundary),
        ):
            result = await crawl_company(
                SITE,
                output_dir=Path(folder) / "crawl",
                crawl="full",
                site_info=True,
                config=ResearchConfig(web_search=False),
                api_key="processing-key",
                decision_model="typesafe/jev-1.13",
                decision_api_key=API_KEY,
                decision_tasks=["site_eligibility"],
            )
            self.assertEqual(requested, [SITE])
            self.assertEqual(result["status"], "skip_crawling")
            self.assertEqual(
                result["site_info"]["site_description"], COMPANY["site_description"]
            )
            self.assertEqual(result["site_info"]["site_types"], ["news_media"])
            self.assertEqual(
                result["site_gate"]["decision_model"]["model"], "typesafe/jev-1.13"
            )
            self.assertEqual(result["usage"]["calls"], 2)

    def test_request_requires_a_compatible_encrypted_decision_model(self):
        processing = profile_payload()
        jev = profile_payload(
            model="typesafe/jev-1.13", base_url="https://openrouter.ai/api/v1"
        )
        request = CrawlRequest(
            url=SITE,
            crawl="full",
            llm=processing,
            decision_llm=jev,
            decision_tasks=["link_selection"],
        )
        self.assertEqual(
            request.decision_llm.decrypt_api_key({"CRAWLER_LLM_ENCRYPTION_KEY": KEY}),
            API_KEY,
        )
        for invalid in [
            {"decision_llm": processing, "decision_tasks": ["link_selection"]},
            {"decision_llm": jev},
            {"decision_tasks": ["link_selection"]},
            {
                "decision_llm": jev,
                "decision_tasks": ["link_selection"],
                "crawl": False,
                "site_info": True,
            },
            {
                "decision_llm": jev,
                "decision_tasks": ["link_selection"],
                "pages": [SITE],
                "crawl": None,
            },
        ]:
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                CrawlRequest.model_validate(
                    {"url": SITE, "llm": processing, "crawl": "full", **invalid}
                )
        self.assertNotIn(
            "decision_llm", CrawlRequest(url=SITE, pages=[SITE]).model_dump()
        )
        self.assertNotIn(
            "decision_tasks", CrawlRequest(url=SITE, pages=[SITE]).model_dump()
        )
