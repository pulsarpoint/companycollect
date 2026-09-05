import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import AsyncMock, patch

import httpx
from pydantic import ValidationError

from jobs_extraction_lab.compare import compare_jobs, create_comparison
from jobs_extraction_lab.corpus import content_hash, is_job_link, write_json
from jobs_extraction_lab.extract import (
    OPENROUTER_MODEL,
    create_prompt,
    extract_openrouter,
    run_extractions,
    validate_evidence,
)
from jobs_extraction_lab.models import ExtractionRun, Job, JobExtraction, Page
from jobs_extraction_lab.repeat import compare_outputs, compare_runs
from jobs_extraction_lab.segment import (
    make_windows,
    markdown_job_links,
    merge_predictions,
    merge_windows,
    prepare_windows,
)


def sample_page() -> Page:
    return Page(
        id="sample",
        company="Example",
        source_url="https://example.com/jobs",
        final_url="https://example.com/jobs",
        platform="fixture",
        fetched_at="2026-09-04T00:00:00Z",
        markdown_file="markdown/sample.md",
        html_file="html/sample.html",
        markdown_sha256=content_hash("Engineer in London"),
        markdown_chars=18,
        job_links=["https://example.com/jobs/123"],
        title="Jobs",
        language="en",
    )


def sample_job(**changes) -> Job:
    return Job.model_validate(
        {
            "title": "Engineer",
            "location": "London",
            "department": None,
            "employment_type": None,
            "workplace_type": None,
            "job_url": "https://example.com/jobs/123",
            "evidence": "Engineer in London",
            **changes,
        }
    )


class EvidenceTests(unittest.TestCase):
    def test_grounded_extraction_passes(self):
        self.assertEqual(
            validate_evidence(
                JobExtraction(jobs=[sample_job()]), "Engineer in London", sample_page()
            ),
            [],
        )

    def test_false_evidence_and_url_are_reported_without_rewriting(self):
        job = sample_job(evidence="Made up", job_url="https://elsewhere.example/123")
        issues = validate_evidence(
            JobExtraction(jobs=[job]), "Engineer in London", sample_page()
        )
        self.assertTrue(any("not a verbatim" in issue for issue in issues))
        self.assertTrue(any("job_url absent" in issue for issue in issues))
        self.assertEqual(job.job_url, "https://elsewhere.example/123")

    def test_unstated_employment_type_is_flagged(self):
        issues = validate_evidence(
            JobExtraction(jobs=[sample_job(employment_type="Full-time")]),
            "Engineer in London",
            sample_page(),
        )
        self.assertTrue(any("employment_type" in issue for issue in issues))

    def test_schema_requires_explicit_nulls_and_rejects_extra_fields(self):
        with self.assertRaises(ValidationError):
            JobExtraction.model_validate({"jobs": [{"title": "Engineer"}]})
        with self.assertRaises(ValidationError):
            JobExtraction.model_validate({"jobs": [], "invented": True})

    def test_prompt_preserves_complete_markdown(self):
        markdown = "a" * 80_000 + "IMPORTANT LAST JOB"
        self.assertIn(markdown, create_prompt(sample_page(), markdown))

    def test_known_job_url_shapes(self):
        self.assertTrue(
            is_job_link("https://job-boards.greenhouse.io/company/jobs/12345")
        )
        self.assertTrue(
            is_job_link(
                "https://jobs.ashbyhq.com/company/12345678-1234-1234-1234-123456789abc"
            )
        )
        self.assertFalse(is_job_link("https://jobs.ashbyhq.com/company"))


class ComparisonTests(unittest.TestCase):
    def test_field_disagreement_retains_both_values(self):
        result = compare_jobs(
            [sample_job()], [sample_job(location="Paris")], sample_page().final_url
        )
        self.assertEqual(result["matched_jobs"], 1)
        self.assertEqual(
            result["matches"][0]["differences"]["location"],
            {"codex": "London", "openrouter": "Paris"},
        )

    def test_duplicate_predictions_cannot_match_twice(self):
        result = compare_jobs(
            [sample_job()], [sample_job(), sample_job()], sample_page().final_url
        )
        self.assertLessEqual(result["matched_jobs"], 1)

    def test_empty_output_is_not_full_recall(self):
        result = compare_jobs([sample_job()], [], sample_page().final_url)
        self.assertEqual(result["matched_jobs"], 0)
        self.assertEqual(len(result["only_codex"]), 1)

    def test_missing_runs_remain_visible(self):
        with TemporaryDirectory() as directory:
            path = Path(directory)
            write_json(path / "manifest.json", {"pages": [sample_page().model_dump()]})
            report = create_comparison(path, "test")
            self.assertEqual(report["totals"]["paired_pages"], 0)
            self.assertEqual(report["backends"]["openrouter"]["missing_pages"], 1)
            self.assertIsNone(report["baseline_job_overlap"])

    def test_failed_candidate_keeps_full_corpus_baseline_visible(self):
        with TemporaryDirectory() as directory:
            path = Path(directory)
            page = sample_page()
            write_json(path / "manifest.json", {"pages": [page.model_dump()]})
            for backend in ("codex", "openrouter"):
                success = backend == "codex"
                record = ExtractionRun(
                    page_id=page.id,
                    backend=backend,
                    requested_model="test",
                    input_hash="test",
                    markdown_sha256=page.markdown_sha256,
                    started_at=page.fetched_at,
                    elapsed_seconds=1,
                    succeeded=success,
                    extraction=JobExtraction(jobs=[sample_job()]) if success else None,
                    error=None if success else "Output truncated",
                )
                write_json(
                    path / "runs/test" / backend / f"{page.id}.json",
                    record.model_dump(),
                )
            report = create_comparison(path, "test")
            self.assertEqual(report["totals"]["paired_pages"], 0)
            self.assertEqual(report["backends"]["codex"]["extracted_jobs"], 1)
            self.assertEqual(report["all_baseline_job_overlap"], 0)
            self.assertEqual(report["results"][0]["codex_jobs"], 1)
            self.assertEqual(
                report["backends"]["openrouter"]["errors"], {"Output truncated": 1}
            )


class RepeatComparisonTests(unittest.TestCase):
    def test_record_order_does_not_change_unordered_comparison(self):
        a = sample_job()
        b = sample_job(title="Designer", job_url="https://example.com/jobs/456")
        result = compare_outputs([a, b], [b, a], sample_page().final_url)
        self.assertFalse(result["same_parsed_output_in_order"])
        self.assertTrue(result["same_records_ignoring_order"])
        self.assertEqual(result["matched_jobs"], 2)

    def test_evidence_change_is_separate_from_extracted_field_change(self):
        result = compare_outputs(
            [sample_job()], [sample_job(evidence="Engineer")], sample_page().final_url
        )
        self.assertFalse(result["same_records_ignoring_order"])
        self.assertTrue(result["same_six_fields_ignoring_order"])
        self.assertEqual(result["same_normalized_six_fields"], 1)
        self.assertEqual(result["same_normalized_evidence"], 0)

    def test_duplicate_predictions_are_not_collapsed_into_equal_sets(self):
        result = compare_outputs(
            [sample_job()], [sample_job(), sample_job()], sample_page().final_url
        )
        self.assertFalse(result["same_job_keys"])
        self.assertFalse(result["same_records_ignoring_order"])
        self.assertEqual(result["matched_jobs"], 0)
        self.assertEqual(len(result["ambiguous_duplicate_keys"]), 1)

    def test_equal_job_counts_can_have_different_job_urls(self):
        result = compare_outputs(
            [sample_job()],
            [sample_job(job_url="https://example.com/jobs/999")],
            sample_page().final_url,
        )
        self.assertFalse(result["same_job_keys"])
        self.assertEqual(len(result["only_first"]), 1)
        self.assertEqual(len(result["only_second"]), 1)

    def test_failed_runs_are_not_equal_empty_extractions_and_inputs_are_verified(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            page = sample_page()
            write_json(root / "manifest.json", {"pages": [page.model_dump()]})
            (root / "markdown").mkdir()
            (root / page.markdown_file).write_text(
                "Engineer in London", encoding="utf-8"
            )
            record = ExtractionRun(
                page_id=page.id,
                backend="openrouter",
                requested_model="test",
                input_hash="same",
                markdown_sha256=page.markdown_sha256,
                started_at=page.fetched_at,
                elapsed_seconds=1,
                succeeded=False,
                error="Incomplete OpenRouter response: finish_reason=length",
            )
            for run_id in ("first", "second"):
                write_json(root / "runs" / run_id / "openrouter/settings.json", {})
                write_json(
                    root / "runs" / run_id / "openrouter/sample.json",
                    record.model_dump(),
                )
            report = compare_runs(root, "first", "second")
            self.assertEqual(report["transitions"], {"output_limit -> output_limit": 1})
            self.assertEqual(report["totals"]["paired_success_pages"], 0)
            self.assertEqual(report["totals"]["same_records_ignoring_order"], 0)
            record.input_hash = "different"
            write_json(root / "runs/second/openrouter/sample.json", record.model_dump())
            with self.assertRaisesRegex(ValueError, "inputs differ"):
                compare_runs(root, "first", "second")


class SegmentTests(unittest.TestCase):
    def test_merge_rejects_filter_links_and_measures_overlap_recovery(self):
        with TemporaryDirectory() as directory:
            source_dir = Path(directory) / "source"
            window_dir = Path(directory) / "windows"
            jobs = [
                sample_job(
                    title=f"Engineer {index}",
                    job_url=f"https://example.com/jobs/{index}",
                    evidence=f"Engineer {index}",
                )
                for index in range(1, 4)
            ]
            markdown = "# Careers in London\n" + "\n".join(
                f"### [{job.title}]({job.job_url})" for job in jobs
            )
            page = sample_page().model_copy(
                update={
                    "markdown_sha256": content_hash(markdown),
                    "markdown_chars": len(markdown),
                    "job_links": [job.job_url for job in jobs],
                }
            )
            write_json(source_dir / "manifest.json", {"pages": [page.model_dump()]})
            (source_dir / "markdown").mkdir()
            (source_dir / page.markdown_file).write_text(markdown, encoding="utf-8")
            metadata = prepare_windows(
                source_dir, window_dir, max_jobs=2, overlap_jobs=1, max_chars=1000
            )
            baseline = ExtractionRun(
                page_id=page.id,
                backend="codex",
                requested_model="test",
                input_hash="test",
                markdown_sha256=page.markdown_sha256,
                started_at=page.fetched_at,
                elapsed_seconds=1,
                succeeded=True,
                extraction=JobExtraction(jobs=jobs),
            )
            write_json(
                source_dir / "runs/baseline/codex/sample.json", baseline.model_dump()
            )
            manifest = json.loads((window_dir / "manifest.json").read_text())
            for index, window in enumerate(manifest["pages"]):
                predictions = (
                    [
                        jobs[0],
                        sample_job(
                            title="Filter",
                            job_url="https://example.com/jobs?team=engineering",
                        ),
                    ]
                    if index == 0
                    else jobs[1:]
                )
                record = baseline.model_copy(
                    update={
                        "page_id": window["id"],
                        "backend": "openrouter",
                        "markdown_sha256": window["markdown_sha256"],
                        "extraction": JobExtraction(jobs=predictions),
                    }
                )
                write_json(
                    window_dir / "runs/test/openrouter" / f"{window['id']}.json",
                    record.model_dump(),
                )
            result = merge_windows(window_dir, "test", "baseline")
            self.assertEqual(len(metadata["windows"]), 2)
            self.assertEqual(result["returned_unique_jobs"], 3)
            self.assertEqual(result["rejected_predictions"], 1)
            self.assertEqual(result["jobs_recovered_by_overlap"], 1)
            self.assertEqual(result["results"][0]["unreturned_job_links"], [])

    def test_complete_jobs_overlap_and_headings_survive(self):
        markdown = (
            "# Careers\n## Engineering\n"
            + "".join(
                f"### [Engineer {index}](https://example.com/jobs/{index})"
                for index in range(1, 5)
            )
            + "\nFooter text retained\n"
        )
        windows = make_windows(markdown, max_jobs=2, overlap_jobs=1, max_chars=1000)
        self.assertEqual(len(windows), 3)
        self.assertEqual(
            set(windows[0]["job_links"]) & set(windows[1]["job_links"]),
            {"https://example.com/jobs/2"},
        )
        self.assertEqual(
            {link for window in windows for link in window["job_links"]},
            set(markdown_job_links(markdown)),
        )
        self.assertTrue(
            all("## Engineering" in window["markdown"] for window in windows)
        )
        self.assertIn("Footer text retained", windows[-1]["markdown"])
        for window in windows:
            for link in window["job_links"]:
                self.assertIn(f"]({link})", window["markdown"])

    def test_long_atomic_listing_is_rejected_instead_of_truncated(self):
        with self.assertRaisesRegex(ValueError, "intact Markdown line"):
            make_windows(
                "### [" + "Long title " * 50 + "](https://example.com/jobs/123)",
                max_jobs=4,
                overlap_jobs=1,
                max_chars=100,
            )

    def test_plain_lever_section_context_survives_window_shift(self):
        markdown = "Core Functions\nFinance & Legal\n" + "\n".join(
            f"##### [Accountant {index}](https://example.com/jobs/{index})"
            for index in range(1, 5)
        )
        windows = make_windows(markdown, max_jobs=2, overlap_jobs=1, max_chars=1000)
        self.assertIn("Finance & Legal", windows[1]["context"])

    def test_overlap_deduplicates_urls_but_keeps_distinct_openings_with_same_title(
        self,
    ):
        a = sample_job()
        b = sample_job(job_url="https://example.com/jobs/456")
        jobs, conflicts = merge_predictions(
            [("w1", a), ("w2", a), ("w2", b)], sample_page().final_url
        )
        self.assertEqual(len(jobs), 2)
        self.assertEqual(conflicts, [])

    def test_conflicting_windows_keep_alternatives_without_duplicate_vote_inflation(
        self,
    ):
        wrong = sample_job(location="Paris")
        expected = sample_job()
        jobs, conflicts = merge_predictions(
            [("w1", wrong)] * 4 + [("w2", expected), ("w3", expected)],
            sample_page().final_url,
        )
        self.assertEqual(jobs[0].location, "London")
        self.assertEqual(len(conflicts), 1)
        self.assertEqual(len(conflicts[0]["alternatives"]), 2)
        self.assertEqual(conflicts[0]["alternatives"][0]["windows"], ["w2", "w3"])


class OpenRouterBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_model_and_schema_sent(self):
        def respond(request):
            body = json.loads(request.content)
            self.assertEqual(body["model"], OPENROUTER_MODEL)
            self.assertTrue(body["provider"]["require_parameters"])
            self.assertEqual(body["response_format"]["type"], "json_schema")
            self.assertNotIn("tools", body)
            return httpx.Response(
                200,
                json={
                    "choices": [
                        {
                            "finish_reason": "stop",
                            "message": {"content": '{"jobs": []}'},
                        }
                    ]
                },
            )

        async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
            result = await extract_openrouter(
                client, "source", api_key="test-key", attempts=1, max_tokens=100
            )
        self.assertEqual(result["attempts"], 1)

    async def test_http_errors_redact_key(self):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(401, text="bad test-secret")
            )
        ) as client:
            result = await extract_openrouter(
                client, "source", api_key="test-secret", attempts=1, max_tokens=100
            )
        self.assertNotIn("test-secret", result["error"])

    async def test_transient_error_retries_same_model(self):
        requests = []

        def respond(request):
            requests.append(json.loads(request.content))
            return (
                httpx.Response(429, headers={"retry-after": "1"})
                if len(requests) == 1
                else httpx.Response(200, json={"choices": []})
            )

        with patch("jobs_extraction_lab.extract.asyncio.sleep", new_callable=AsyncMock):
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(respond)
            ) as client:
                result = await extract_openrouter(
                    client, "source", api_key="test-key", attempts=2, max_tokens=100
                )
        self.assertEqual(result["attempts"], 2)
        self.assertEqual(requests[0], requests[1])

    async def test_changed_markdown_fails_before_model_call(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_json(root / "manifest.json", {"pages": [sample_page().model_dump()]})
            (root / "markdown").mkdir()
            (root / "markdown/sample.md").write_text("Changed input", encoding="utf-8")
            with patch(
                "jobs_extraction_lab.extract.extract_codex",
                new_callable=AsyncMock,
            ) as call:
                with self.assertRaisesRegex(ValueError, "Stored Markdown changed"):
                    await run_extractions(
                        root,
                        backend="codex",
                        run_id="test",
                        api_key=None,
                        concurrency=1,
                        timeout=1,
                        attempts=1,
                        max_tokens=100,
                        limit=None,
                        retry_failed=False,
                        codex_model_label="test",
                        codex_bin=None,
                    )
                call.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
