import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx

from jobs_extraction_lab.corpus import content_hash, write_json
from jobs_extraction_lab.crawl4ai_html_report import score_titles
from jobs_extraction_lab.format_benchmark import FORMAT_INSTRUCTIONS, format_prompt
from jobs_extraction_lab.models import JobExtraction
from jobs_extraction_lab.tests import sample_job
from jobs_extraction_lab.validated_html import (
    page_link_urls,
    run_validated,
    validate_response,
)
from jobs_extraction_lab.validated_html_report import create_report


class ValidatedHtmlTests(unittest.IsolatedAsyncioTestCase):
    def test_generic_links_and_duplicates_keep_query_parameters(self) -> None:
        source = "https://example.com/careers"
        links = page_link_urls(
            '<a href="/opening?id=one">Role</a><a href="mailto:x@y">Mail</a><a href="https://[bad">Bad</a>',
            source,
        )
        self.assertEqual(links, {"https://example.com/opening?id=one"})
        jobs = [
            sample_job().model_copy(update={"job_url": url})
            for url in (
                "https://example.com/opening?id=one",
                "https://example.com/opening?id=one#apply",
                "https://example.com/opening?id=two",
                None,
                "https://[bad",
                "/opening?id=one",
            )
        ]
        extraction, issues = validate_response(
            JobExtraction(jobs=jobs).model_dump_json(), "stop", source, links
        )
        self.assertIsNotNone(extraction)
        self.assertEqual(
            [i["type"] for i in issues],
            [
                "duplicate_job_url",
                "url_not_in_page",
                "missing_job_url",
                "invalid_job_url",
                "invalid_job_url",
            ],
        )
        score = score_titles(
            jobs, {"https://example.com/opening?id=one": jobs[0].title}, source
        )
        self.assertEqual(score["correct_titles"], 0)
        self.assertIn("invalid-url:https://[bad", score["unexpected_urls"])

    def test_schema_and_truncation_cannot_be_accepted(self) -> None:
        job = sample_job().model_dump()
        job["work_place_type"] = job.pop("workplace_type")
        for raw, finish in (
            (json.dumps({"jobs": [job]}), "stop"),
            ('{"jobs":[]} trailing', "stop"),
            ('{"jobs":[]}', "length"),
        ):
            extraction, issues = validate_response(
                raw, finish, "https://example.com", set()
            )
            self.assertIsNone(extraction)
            self.assertTrue(issues)

    async def test_retry_recovers_schema_but_rejects_repeated_url_error_and_resumes(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            html = '<main class="unfamiliar"><a href="https://example.com/jobs/1"><h2>Engineer</h2><p>Paris</p></a></main>'
            inputs = [
                {
                    "page_id": page,
                    "source_url": "https://example.com/careers",
                    "format": "crawl4ai_cleaned_html",
                    "file": f"inputs/{page}.html",
                    "sha256": content_hash(html),
                    "source_html_file": f"inputs/{page}.html",
                    "source_html_sha256": content_hash(html),
                }
                for page in ("schema", "bad-url", "valid")
            ]
            for item in inputs:
                path = root / item["file"]
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(html, encoding="utf-8")
            write_json(
                root / "manifest.json", {"inputs": inputs, "source_dir": str(root)}
            )
            settings = {
                "manifest_sha256": content_hash(
                    (root / "manifest.json").read_text(encoding="utf-8")
                ),
                "model": "test",
                "provider": {
                    "only": ["test"],
                    "allow_fallbacks": False,
                    "require_parameters": True,
                },
                "reasoning": {"enabled": True, "exclude": True, "effort": "low"},
                "max_tokens": 32768,
                "temperature": 0,
                "timeout": 5,
                "attempts": 1,
                "interval": 0,
                "instructions": FORMAT_INSTRUCTIONS,
                "schema": JobExtraction.model_json_schema(),
                "examples": "Synthetic example",
                "input_policy": "One complete Crawl4AI cleaned HTML page per call; no truncation or segmentation",
            }
            write_json(root / "runs/baseline/settings.json", settings)
            (root / ".env").write_text(
                "OPENROUTER_API_KEY=fake-test-key\n", encoding="utf-8"
            )
            good = sample_job().model_copy(
                update={"title": "Engineer", "job_url": "https://example.com/jobs/1"}
            )
            bad = good.model_copy(update={"job_url": "https://example.com/invented"})
            good_json = JobExtraction(jobs=[good]).model_dump_json()
            bad_json = JobExtraction(jobs=[bad]).model_dump_json()
            replies = ['{"jobs":[]} trailing', good_json, bad_json, bad_json, good_json]
            requests = []

            def respond(request: httpx.Request) -> httpx.Response:
                requests.append(json.loads(request.content))
                return httpx.Response(
                    200,
                    json={
                        "model": "test",
                        "provider": "test",
                        "id": f"response-{len(requests)}",
                        "choices": [
                            {
                                "finish_reason": "stop",
                                "message": {
                                    "content": replies[
                                        (len(requests) - 1) % len(replies)
                                    ]
                                },
                            }
                        ],
                        "usage": {"cost": 0.001},
                    },
                )

            with patch(
                "jobs_extraction_lab.validated_html.httpx.AsyncClient",
                return_value=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
            ):
                await run_validated(root, "new", "baseline", root / ".env")
            self.assertEqual(len(requests), 5)
            original = format_prompt(inputs[0], html, settings["examples"])
            self.assertEqual(requests[0]["messages"][0]["content"], original)
            self.assertTrue(requests[1]["messages"][0]["content"].startswith(original))
            self.assertIn("validation_errors", requests[1]["messages"][0]["content"])
            self.assertIn("url_not_in_page", requests[3]["messages"][0]["content"])
            outcomes = {
                p.stem: json.loads(p.read_text(encoding="utf-8"))
                for p in (root / "runs/new/responses").glob("*.json")
            }
            self.assertTrue(outcomes["schema"]["succeeded"])
            self.assertFalse(outcomes["bad-url"]["succeeded"])
            self.assertIsNone(outcomes["bad-url"]["extraction"])
            self.assertEqual(outcomes["bad-url"]["attempt_count"], 2)
            self.assertEqual(outcomes["valid"]["attempt_count"], 1)
            attempts = [
                json.loads(p.read_text(encoding="utf-8"))
                for p in (root / "runs/new/attempts").glob("*/*.json")
            ]
            self.assertAlmostEqual(sum(a["usage"]["cost"] for a in attempts), 0.005)
            self.assertEqual(len(attempts), 5)
            with patch(
                "jobs_extraction_lab.validated_html.httpx.AsyncClient",
                return_value=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
            ):
                await run_validated(root, "new", "baseline", root / ".env")
            self.assertEqual(len(requests), 5)
            with patch(
                "jobs_extraction_lab.validated_html.httpx.AsyncClient",
                return_value=httpx.AsyncClient(transport=httpx.MockTransport(respond)),
            ):
                await run_validated(root, "second", "baseline", root / ".env")
            self.assertEqual(len(requests), 10)
            catalog = {
                i["page_id"]: {"https://example.com/jobs/1": "Engineer"} for i in inputs
            }
            write_json(
                root / "comparison.json",
                {
                    "manifest_sha256": settings["manifest_sha256"],
                    "negative_examples": [],
                    "reference_catalog_sha256": content_hash(
                        json.dumps(catalog, sort_keys=True)
                    ),
                },
            )
            for item in inputs:
                path = (
                    root / "reference/full-pages/clean_html" / f"{item['page_id']}.html"
                )
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(
                    '<article><h3>Engineer</h3><a href="https://example.com/jobs/1">Apply</a></article>',
                    encoding="utf-8",
                )
            report = create_report(root, root / "reference", ("new", "second"))
            for arm in report["arms"]:
                self.assertEqual(arm["final"]["expected_jobs"], 3)
                self.assertEqual(arm["final"]["correct_titles"], 2)
                self.assertEqual(arm["first_pass"]["correct_titles"], 1)
                self.assertEqual(arm["recovered_pages"], 1)
                self.assertEqual(arm["final_validation_passed_pages"], 2)
                self.assertAlmostEqual(arm["known_cost_usd"], 0.005)
                self.assertAlmostEqual(arm["retry_cost_usd"], 0.002)
            self.assertEqual(report["repeatability"]["same_normalized_six_fields"], 2)
            rejected_path = root / "runs/new/responses/bad-url.json"
            rejected = json.loads(rejected_path.read_text(encoding="utf-8"))
            write_json(
                rejected_path,
                {**rejected, "extraction": JobExtraction(jobs=[good]).model_dump()},
            )
            with self.assertRaisesRegex(ValueError, "Final result differs"):
                create_report(root, root / "reference", ("new", "second"))
            write_json(rejected_path, rejected)
            (root / inputs[-1]["file"]).write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Native HTML input changed"):
                await run_validated(root, "changed", "baseline", root / ".env")
            self.assertEqual(len(requests), 10)


if __name__ == "__main__":
    unittest.main()
