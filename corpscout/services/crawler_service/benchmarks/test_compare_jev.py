"""Check billing arithmetic and malformed Decisions API responses without API spend."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import httpx

from benchmarks.compare_jev import (
    decision_request,
    recorded_post,
    usage_totals,
    validate_answers,
)
from benchmarks.page_gate_jev import (
    SITE_TYPES,
    explain_page,
    page_questions,
    scope_from_answers,
)


class JevBenchmarkTests(unittest.TestCase):
    def test_candidate_ids_and_full_context_survive_mapping(self):
        data = {
            "candidates": [
                {
                    "candidate_id": "c009",
                    "url": "https://example.com/job",
                    "link_contexts": [{"text": "An original evidence quotation"}],
                }
            ],
            "selection_instructions": "Get job descriptions",
        }
        call = {
            "task": "link_assessment",
            "request": {
                "messages": [
                    {
                        "role": "user",
                        "content": "INPUT DATA:\n"
                        + json.dumps(data)
                        + "\nCorrection suffix",
                    }
                ]
            },
        }
        request = decision_request(call)
        self.assertEqual(request["state"]["input"], data)
        self.assertEqual(
            set(request["questions"]),
            {"c009_content", "c009_target", "c009_scope", "c009_priority"},
        )

    def test_missing_unknown_and_out_of_range_answers_are_rejected(self):
        request = {
            "questions": {
                "gate": {
                    "type": "choice",
                    "criteria": {"yes": "eligible", "unknown": "insufficient evidence"},
                },
                "priority": {"type": "score", "criteria": ["low", "high"]},
            }
        }
        valid = {
            "answers": {
                "gate": {"type": "choice", "choice": "unknown"},
                "priority": {"type": "score", "score": 0.5},
            },
            "usage": {"input_tokens": 10, "output_tokens": 3, "cost": 0.001},
        }
        validate_answers(request, valid)
        for invalid in (
            valid | {"answers": {}},
            valid
            | {
                "answers": valid["answers"]
                | {"gate": {"type": "choice", "choice": "invented"}}
            },
            valid
            | {
                "answers": valid["answers"]
                | {"priority": {"type": "score", "score": 2}}
            },
            valid | {"usage": {"input_tokens": 10, "output_tokens": 3}},
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                validate_answers(request, invalid)

    def test_deepseek_prices_cache_hit_miss_and_output_without_double_reasoning(self):
        totals = usage_totals(
            [
                {
                    "usage": {
                        "prompt_tokens": 1000,
                        "prompt_cache_hit_tokens": 600,
                        "completion_tokens": 200,
                        "completion_tokens_details": {"reasoning_tokens": 100},
                    }
                }
            ],
            jev=False,
        )
        self.assertAlmostEqual(totals["estimated_cost_usd_off_peak"], 0.0001818)
        self.assertAlmostEqual(totals["estimated_cost_usd_peak"], 0.0003636)
        self.assertIsNone(totals["reported_cost_usd"])
        self.assertEqual(totals["reasoning_tokens"], 100)

    def test_absent_usage_never_becomes_zero_cost(self):
        totals = usage_totals([{"error": "timeout"}], jev=False)
        self.assertIsNone(totals["estimated_cost_usd_off_peak"])
        self.assertEqual(totals["missing_usage_calls"], 1)

    def test_jev_uses_provider_reported_cost(self):
        totals = usage_totals(
            [
                {
                    "response": {
                        "usage": {
                            "input_tokens": 100,
                            "output_tokens": 50,
                            "cost": 0.0123,
                        }
                    }
                }
            ],
            jev=True,
        )
        self.assertEqual(totals["reported_cost_usd"], 0.0123)
        self.assertIsNone(totals["estimated_cost_usd_off_peak"])


class ArtifactTests(unittest.IsolatedAsyncioTestCase):
    async def test_http_failure_is_recorded_without_credentials(self):
        async def handle(request):
            self.assertEqual(
                request.headers["authorization"], "Bearer private-test-key"
            )
            return httpx.Response(429, json={"error": {"message": "Rate limit"}})

        with TemporaryDirectory() as directory:
            path = Path(directory) / "attempt.json"
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handle)
            ) as client:
                with self.assertRaises(httpx.HTTPStatusError):
                    await recorded_post(
                        client,
                        "https://example.com/api",
                        "private-test-key",
                        {"model": "test"},
                        path,
                    )
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("private-test-key", text)
            self.assertEqual(json.loads(text)["http_status"], 429)


class CrawlScopeTests(unittest.TestCase):
    def test_company_site_can_continue_but_every_other_type_keeps_basic_info(self):
        for site_type in SITE_TYPES:
            answers = {
                "site_type": {"choice": site_type},
                "company_operator": {"choice": "yes"},
            }
            with self.subTest(site_type=site_type):
                scope = scope_from_answers(answers)
                self.assertTrue(scope["basic_information_required"])
                self.assertEqual(
                    scope["scope"],
                    "whole_site" if site_type == "company" else "first_page_only",
                )

    def test_uncertain_company_gets_one_page_and_review(self):
        result = scope_from_answers(
            {
                "site_type": {"choice": "company"},
                "company_operator": {"choice": "uncertain"},
            }
        )
        self.assertEqual(result["scope"], "first_page_only")
        self.assertTrue(result["needs_review"])

    def test_explicit_full_crawl_all_can_override_shop_gate(self):
        result = scope_from_answers(
            {
                "site_type": {"choice": "online_shop"},
                "company_operator": {"choice": "yes"},
            },
            full_crawl_all=True,
        )
        self.assertEqual(result["scope"], "whole_site")
        self.assertEqual(result["reasons"], ["explicit_full_crawl_all"])

    def test_page_reasons_include_positive_and_uncertain_evidence(self):
        answers = {
            key: {"type": "choice", "choice": "no"}
            for key in page_questions(homepage=False)
        }
        answers.update(
            process_page={"choice": "process"},
            target_relevance={"choice": "target"},
            jobs={"choice": "yes", "confidence": 0.9},
            financials={"choice": "uncertain"},
        )
        result = explain_page(answers)
        self.assertEqual(
            [reason["code"] for reason in result["reasons"]], ["jobs", "financials"]
        )
        self.assertEqual(result["reasons"][0]["confidence"], 0.9)


if __name__ == "__main__":
    unittest.main()
