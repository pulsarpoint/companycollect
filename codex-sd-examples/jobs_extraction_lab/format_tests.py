import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from bs4 import BeautifulSoup

from jobs_extraction_lab.codex_backend import extract_codex
from jobs_extraction_lab.corpus import content_hash, write_json
from jobs_extraction_lab.format_benchmark import evaluate_output, run_benchmark
from jobs_extraction_lab.format_inputs import read_job_cards, render_cards
from jobs_extraction_lab.models import JobExtraction
from jobs_extraction_lab.tests import sample_job, sample_page


class CardFormatTests(unittest.TestCase):
    def test_wrapping_link_does_not_join_title_and_metadata(self):
        page = sample_page().model_copy(update={"platform": "ashby"})
        html = """<h2 class="ashby-department-heading"><span class="ashby-department-heading-level">Product</span><span class="ashby-department-heading-level">Engineering</span></h2>
        <a href="/jobs/123"><h3 class="ashby-job-posting-brief-title">Director of Engineering</h3><div class="ashby-job-posting-brief-details"><p>Engineering • London • Full time</p></div></a>"""
        cards = read_job_cards(page, html)
        self.assertEqual(cards[0].headings, ("Product", "Engineering"))
        self.assertEqual(cards[0].title, "Director of Engineering")
        markdown = render_cards(cards, html=False)
        self.assertIn("### Director of Engineering\n\nEngineering • London", markdown)
        self.assertIn("https://example.com/jobs/123", markdown)
        soup = BeautifulSoup(render_cards(cards, html=True), "html.parser")
        heading, link = soup.h3, soup.a
        assert heading is not None and link is not None
        self.assertEqual(heading.get_text(), "Director of Engineering")
        self.assertEqual(link.get_text(), "Job link")
        self.assertEqual(link["href"], cards[0].url)

    def test_badge_and_location_remain_outside_title(self):
        page = sample_page().model_copy(update={"platform": "greenhouse"})
        html = """<h3>Sales</h3><a href="/jobs/123"><p class="body--medium">Account Executive<span class="tag-container"><span class="tag-text">New</span></span></p><p class="body--metadata">CA Remote</p></a>"""
        card = read_job_cards(page, html)[0]
        self.assertEqual(card.title, "Account Executive")
        self.assertEqual(card.badges, ("New",))
        self.assertEqual(card.body, ("CA Remote",))
        markdown = render_cards([card], html=False)
        self.assertIn("### Account Executive\n\n> New\n\nCA Remote", markdown)

    def test_lever_parent_department_survives_group_continuation(self):
        page = sample_page().model_copy(update={"platform": "lever"})
        html = """<div class="postings-group"><div class="large-category-header">Core Functions</div></div><div class="postings-group"><div class="posting-category-title">Tech</div><a href="/jobs/123"><h5 data-qa="posting-name">Product Manager (m/f/x) Growth</h5><div class="posting-categories"><span>Hybrid —</span><span>Full Time</span><span>Munich</span></div></a></div>"""
        card = read_job_cards(page, html)[0]
        self.assertEqual(card.headings, ("Core Functions", "Tech"))
        self.assertEqual(card.body, ("Hybrid —", "Full Time", "Munich"))
        self.assertIn("Full Time\n\nMunich", render_cards([card], html=False))

    def test_missing_card_fails_instead_of_silently_dropping_a_job(self):
        page = sample_page().model_copy(update={"platform": "ashby"})
        with self.assertRaisesRegex(ValueError, "lost 1 collected URLs"):
            read_job_cards(page, "<p>No matching job card</p>")

    def test_duplicate_urls_do_not_inflate_title_score(self):
        job = sample_job()
        record = {
            "succeeded": True,
            "extraction": JobExtraction(jobs=[job, job]).model_dump(),
        }
        item = {"source_url": "https://example.com/jobs"}
        source = {
            "selected_cards": [
                {
                    "job_url": job.job_url,
                    "title": job.title,
                    "badges": [],
                    "body": ["London"],
                    "headings": [],
                }
            ],
            "preserved_prose": [],
        }
        result = evaluate_output(record, item, source, {"succeeded": False})
        self.assertEqual(result["matched_urls"], 1)
        self.assertEqual(result["exact_html_titles"], 0)
        self.assertEqual(result["duplicate_predictions"], 1)


class CodexModelTests(unittest.IsolatedAsyncioTestCase):
    async def test_routing_rejection_stops_before_sending_other_formats(self):
        requests = []

        def reject(request):
            requests.append(request)
            return httpx.Response(
                404, json={"error": {"message": "No compatible endpoints"}}
            )

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            content = "Engineer in London"
            (root / "sample.md").write_text(content, encoding="utf-8")
            (root / ".env").write_text(
                "OPENROUTER_API_KEY=not-a-real-key", encoding="utf-8"
            )
            (root / "examples.md").write_text("Synthetic examples", encoding="utf-8")
            item = {
                "page_id": "sample",
                "source_url": "https://example.com/jobs",
                "file": "sample.md",
                "sha256": content_hash(content),
                "job_links": ["https://example.com/jobs/123"],
            }
            source = {
                "page_id": "sample",
                "selected_cards": [
                    {
                        "job_url": "https://example.com/jobs/123",
                        "title": "Engineer",
                        "headings": [],
                        "badges": [],
                        "body": ["London"],
                    }
                ],
                "preserved_prose": [],
            }
            write_json(
                root / "manifest.json",
                {
                    "source_dir": str(root),
                    "source_audit": [source],
                    "inputs": [
                        {**item, "format": variant}
                        for variant in (
                            "original_markdown",
                            "clean_markdown",
                            "clean_html",
                        )
                    ],
                },
            )
            write_json(root / "runs/jobs-v2/codex/sample.json", {"succeeded": False})
            client = httpx.AsyncClient(transport=httpx.MockTransport(reject))
            with patch(
                "jobs_extraction_lab.format_benchmark.httpx.AsyncClient",
                return_value=client,
            ):
                await run_benchmark(
                    root,
                    run_id="routing-test",
                    env_file=root / ".env",
                    examples_file=root / "examples.md",
                    timeout=5,
                    attempts=1,
                    interval=0,
                    limit=None,
                    codex_bin=Path("/test/codex"),
                    models=("glm",),
                    glm_provider="test",
                    deepseek_provider="test",
                )
            self.assertEqual(len(requests), 1)
            self.assertTrue((root / "runs/routing-test/glm/stopped.json").exists())
            record = json.loads(
                (
                    root / "runs/routing-test/glm/original_markdown/sample.json"
                ).read_text()
            )
            self.assertFalse(record["succeeded"])

    async def test_astra_low_is_explicit_on_thread_and_turn(self):
        thread = SimpleNamespace(turn=AsyncMock())
        client = AsyncMock()
        client.__aenter__.return_value = client
        client.thread_start.return_value = thread
        result = SimpleNamespace(final_response='{"jobs": []}')
        with (
            patch("jobs_extraction_lab.codex_backend.AsyncCodex", return_value=client),
            patch(
                "jobs_extraction_lab.codex_backend._run_turn_with_timeout",
                new=AsyncMock(return_value=(result, False)),
            ),
            patch(
                "jobs_extraction_lab.codex_backend.analysis_token_usage",
                return_value=None,
            ),
        ):
            outcome = await extract_codex(
                "source",
                instructions="extract",
                timeout=5,
                operation="test",
                codex_bin=Path("/test/codex"),
                model="gpt-6-astra",
                reasoning_effort="low",
            )
        self.assertIsNone(outcome.error)
        self.assertEqual(client.thread_start.call_args.kwargs["model"], "gpt-6-astra")
        self.assertEqual(
            client.thread_start.call_args.kwargs["config"],
            {"model_reasoning_effort": "low"},
        )
        self.assertEqual(thread.turn.call_args.kwargs["model"], "gpt-6-astra")
        self.assertEqual(thread.turn.call_args.kwargs["effort"].value, "low")


if __name__ == "__main__":
    unittest.main()
