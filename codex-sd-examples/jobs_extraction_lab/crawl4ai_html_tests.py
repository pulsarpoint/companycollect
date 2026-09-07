import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx

from jobs_extraction_lab.corpus import write_json
from jobs_extraction_lab.crawl4ai_html import prepare_html, run_html
from jobs_extraction_lab.crawl4ai_html_report import score_titles
from jobs_extraction_lab.tests import sample_job, sample_page


class NativeCrawlTests(unittest.IsolatedAsyncioTestCase):
    def test_duplicate_predictions_cannot_hide_a_missing_job(self) -> None:
        job = sample_job()
        self.assertIsNotNone(job.job_url)
        score = score_titles(
            [job, job],
            {
                str(job.job_url): job.title,
                "https://example.com/jobs/missing": "Another role",
            },
            "https://example.com/jobs",
        )
        self.assertEqual(score["expected_jobs"], 2)
        self.assertEqual(score["matched_urls"], 1)
        self.assertEqual(score["correct_titles"], 0)
        self.assertEqual(score["duplicate_predictions"], 1)
        self.assertEqual(score["missing_urls"], ["https://example.com/jobs/missing"])

    async def test_unknown_layout_and_wrong_known_links_do_not_filter_content(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source"
            page = sample_page().model_copy(
                update={
                    "platform": "unfamiliar-platform",
                    "company": "PRIVATE_REFERENCE_COMPANY",
                    "title": "PRIVATE_REFERENCE_TITLE",
                    "html_file": "page.html",
                    "job_links": ["https://example.com/incorrect-reference-url"],
                }
            )
            write_json(source / "manifest.json", {"pages": [page.model_dump()]})
            html = '<html><body><h1>Careers</h1><section class="never-seen-before"><a href="/openings/custom-id"><h2>Distributed Systems Builder</h2><p>Lisbon • Full time</p></a></section></body></html>'
            (source / "page.html").write_text(html, encoding="utf-8")
            manifest = await prepare_html(source, root / "prepared")
            item = manifest["inputs"][0]
            cleaned = (root / "prepared" / item["file"]).read_text(encoding="utf-8")
            markdown = (root / "prepared" / item["markdown_file"]).read_text(
                encoding="utf-8"
            )
            for content in (cleaned, markdown):
                self.assertIn("Distributed Systems Builder", content)
                self.assertIn("Lisbon", content)
                self.assertIn("/openings/custom-id", content)
                self.assertNotIn("PRIVATE_REFERENCE", content)
                self.assertNotIn("incorrect-reference-url", content)

            (root / ".env").write_text(
                "OPENROUTER_API_KEY=fake-test-key\n", encoding="utf-8"
            )
            (root / "examples.md").write_text("Synthetic example", encoding="utf-8")
            requests = []

            def respond(request: httpx.Request) -> httpx.Response:
                payload = json.loads(request.content)
                requests.append(payload)
                return httpx.Response(
                    200,
                    json={
                        "model": "test",
                        "provider": "test",
                        "choices": [
                            {
                                "finish_reason": "stop",
                                "message": {"content": '{"jobs":[]}'},
                            }
                        ],
                    },
                )

            for variant, expected in (("html", cleaned), ("markdown", markdown)):
                client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
                with patch(
                    "jobs_extraction_lab.crawl4ai_html.httpx.AsyncClient",
                    return_value=client,
                ):
                    await run_html(
                        root / "prepared",
                        run_id=variant,
                        env_file=root / ".env",
                        examples_file=root / "examples.md",
                        model="test",
                        provider="test",
                        max_tokens=32768,
                        timeout=5,
                        attempts=1,
                        interval=0,
                        input_format=variant,
                    )
                prompt = requests[-1]["messages"][0]["content"]
                submitted = json.loads(prompt.split("INPUT DATA:\n", 1)[1])
                self.assertEqual(submitted["content"], expected)
                self.assertEqual(
                    set(submitted), {"source_url", "input_format", "content"}
                )
                self.assertNotIn("PRIVATE_REFERENCE", prompt)

            # A saved input edited after preparation must fail before an API call.
            (root / "prepared" / item["markdown_file"]).write_text(
                "changed", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "Input changed"):
                await run_html(
                    root / "prepared",
                    run_id="changed",
                    env_file=root / ".env",
                    examples_file=root / "examples.md",
                    model="test",
                    provider="test",
                    max_tokens=32768,
                    timeout=5,
                    attempts=1,
                    interval=0,
                    input_format="markdown",
                )
            self.assertEqual(len(requests), 2)


if __name__ == "__main__":
    unittest.main()
