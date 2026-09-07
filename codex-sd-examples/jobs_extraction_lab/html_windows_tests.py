import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx

from jobs_extraction_lab.corpus import content_hash, write_json
from jobs_extraction_lab.format_benchmark import FORMAT_INSTRUCTIONS
from jobs_extraction_lab.html_windows import prepare_windows, split_html
from jobs_extraction_lab.html_windows_report import create_report
from jobs_extraction_lab.models import JobExtraction
from jobs_extraction_lab.partial_html import (
    merge_observations,
    retain_valid_records,
    run_experiment,
)
from jobs_extraction_lab.tests import sample_job
from jobs_extraction_lab.validated_html import page_link_urls


class HtmlWindowTests(unittest.IsolatedAsyncioTestCase):
    def test_source_ranges_preserve_all_markup_and_complete_links(self) -> None:
        cards = [
            f'<section data-unknown="{i}"><a href="/opportunity/{i}?x=1&amp;y=2"><h3>Builder {i} — R&amp;D</h3><p>{"Paris " * 12}</p></a></section>'
            for i in range(20)
        ]
        html = (
            '<main><h1>Careers</h1>\n<h2>Engineering</h2><img src="logo.svg"/>'
            + "".join(cards)
            + '<footer><a href="/privacy">Privacy</a></footer></main>'
        )
        units = split_html(
            html,
            "https://example.com",
            core_chars=1000,
            atom_chars=300,
            overlap_chars=300,
        )
        self.assertGreater(len(units), 2)
        self.assertEqual(
            "".join(html[u["core_start"] : u["core_end"]] for u in units), html
        )
        for card in cards:
            self.assertTrue(any(card in u["content"] for u in units))
        self.assertTrue(
            any(
                "<h2>Engineering</h2>" in u["content"] and u["core_start"] > 0
                for u in units
            )
        )
        self.assertEqual(
            set.union(*(set(u["core_links"]) for u in units)),
            page_link_urls(html, "https://example.com"),
        )
        self.assertTrue(any(u["source_overlap_chars"] >= 300 for u in units[1:]))

    def test_oversize_anchor_is_kept_whole_instead_of_truncated(self) -> None:
        anchor = (
            '<a href="/one"><h3>Engineer</h3><p>' + "Long location " * 200 + "</p></a>"
        )
        html = "<div>" + anchor + "</div>"
        units = split_html(
            html,
            "https://example.com",
            core_chars=1000,
            atom_chars=300,
            overlap_chars=200,
        )
        self.assertTrue(any(anchor in u["content"] for u in units))
        self.assertEqual(
            "".join(html[u["core_start"] : u["core_end"]] for u in units), html
        )

    def test_bad_records_do_not_discard_other_jobs(self) -> None:
        good = sample_job().model_dump()
        missing = {k: v for k, v in good.items() if k != "workplace_type"}
        bad_link = {**good, "job_url": "https://[broken"}
        result = retain_valid_records(
            json.dumps({"jobs": [good, missing, bad_link]}),
            "stop",
            "https://example.com",
            {good["job_url"]},
        )
        self.assertEqual([a["record_index"] for a in result["accepted"]], [0])
        self.assertEqual([a["record_index"] for a in result["rejected"]], [1, 2])
        self.assertEqual(result["accepted"][0]["job"], good)
        malformed = retain_valid_records(
            '{"jobs":[' + json.dumps(good),
            "stop",
            "https://example.com",
            {good["job_url"]},
        )
        self.assertEqual(malformed["accepted"], [])
        self.assertEqual(malformed["issues"][0]["type"], "json_invalid")

    def test_merge_uses_core_then_initial_attempt_and_retains_conflicts(self) -> None:
        job = sample_job().model_dump()
        base = {
            "unit_id": "overlap",
            "unit_index": 0,
            "attempt_number": 1,
            "record_index": 0,
            "core_url": False,
            "job": {**job, "title": "Overlap guess"},
        }
        observations = [
            base,
            {**base, "unit_id": "owner", "unit_index": 1, "core_url": True, "job": job},
            {
                **base,
                "unit_id": "owner",
                "unit_index": 1,
                "attempt_number": 2,
                "core_url": True,
                "job": {**job, "title": "Retry rewrite"},
            },
            {
                **base,
                "unit_id": "owner",
                "unit_index": 1,
                "core_url": True,
                "job": {**job, "job_url": job["job_url"] + "-different"},
            },
        ]
        result = merge_observations(observations, "https://example.com")
        self.assertEqual(len(result["jobs"]), 2)
        self.assertEqual(result["jobs"][0], job)
        self.assertEqual(result["conflicts"][0]["differing_fields"], ["title"])
        self.assertEqual(len(result["conflicts"][0]["alternatives"]), 2)

    async def test_correction_keeps_initial_good_record_and_resumes_without_calls(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "native"
            html = "<h1>Careers</h1>" + "".join(
                f'<a href="https://example.com/jobs/{i}"><h2>Engineer {i}</h2><p>Paris</p></a>'
                for i in range(3)
            )
            write_json(
                source / "manifest.json",
                {
                    "inputs": [
                        {
                            "page_id": "unfamiliar",
                            "source_url": "https://example.com/careers",
                            "format": "crawl4ai_cleaned_html",
                            "file": "page.html",
                            "sha256": content_hash(html),
                            "known_jobs": ["DO_NOT_USE_REFERENCE"],
                        }
                    ]
                },
            )
            (source / "page.html").write_text(html, encoding="utf-8")
            settings = {
                "manifest_sha256": content_hash(
                    (source / "manifest.json").read_text(encoding="utf-8")
                ),
                "model": "test",
                "provider": {"only": ["test"], "allow_fallbacks": False},
                "reasoning": {"effort": "low"},
                "max_tokens": 32768,
                "temperature": 0,
                "timeout": 5,
                "attempts": 1,
                "interval": 0,
                "instructions": FORMAT_INSTRUCTIONS,
                "schema": JobExtraction.model_json_schema(),
                "examples": "Synthetic examples",
            }
            write_json(source / "runs/baseline/settings.json", settings)
            output = root / "windows"
            manifest = prepare_windows(
                source, output, core_chars=1000, atom_chars=300, overlap_chars=200
            )
            (root / ".env").write_text(
                "OPENROUTER_API_KEY=fake-test-key\n", encoding="utf-8"
            )
            good = [
                sample_job()
                .model_copy(
                    update={
                        "title": f"Engineer {i}",
                        "job_url": f"https://example.com/jobs/{i}",
                    }
                )
                .model_dump()
                for i in range(3)
            ]
            first = {
                "jobs": [
                    good[0],
                    {**good[1], "job_url": "https://example.com/wrong"},
                    {k: v for k, v in good[2].items() if k != "workplace_type"},
                ]
            }
            second = {"jobs": [{**good[0], "title": "Rewritten"}, good[1], good[2]]}
            requests = []

            def respond(request: httpx.Request) -> httpx.Response:
                payload = json.loads(request.content)
                requests.append(payload)
                body = (
                    second
                    if "validation_errors" in payload["messages"][0]["content"]
                    else first
                )
                return httpx.Response(
                    200,
                    json={
                        "id": f"test-{len(requests)}",
                        "model": "test",
                        "provider": "test",
                        "choices": [
                            {
                                "finish_reason": "stop",
                                "message": {"content": json.dumps(body)},
                            }
                        ],
                        "usage": {"cost": 0.001},
                    },
                )

            for _ in range(2):
                with patch(
                    "jobs_extraction_lab.partial_html.httpx.AsyncClient",
                    return_value=httpx.AsyncClient(
                        transport=httpx.MockTransport(respond)
                    ),
                ):
                    await run_experiment(output, "test", "baseline", root / ".env", 1)
            self.assertEqual(len(requests), len(manifest["units"]) * 2)
            self.assertTrue(
                all(
                    "DO_NOT_USE_REFERENCE" not in r["messages"][0]["content"]
                    for r in requests
                )
            )
            for path in (output / "runs/test/units").glob("*.json"):
                result = json.loads(path.read_text(encoding="utf-8"))
                self.assertEqual(result["jobs"], good)
                self.assertEqual(len(result["conflicts"]), 1)
            attempts = [
                json.loads(p.read_text(encoding="utf-8"))
                for p in (output / "runs/test/attempts").glob("*/*.json")
            ]
            self.assertAlmostEqual(
                sum(a["usage"]["cost"] for a in attempts), len(requests) * 0.001
            )
            catalog = {"unfamiliar": {j["job_url"]: j["title"] for j in good}}
            write_json(
                source / "comparison.json",
                {
                    "manifest_sha256": settings["manifest_sha256"],
                    "reference_catalog_sha256": content_hash(
                        json.dumps(catalog, sort_keys=True)
                    ),
                    "negative_examples": [],
                },
            )
            labelled = root / "reference/full-pages/clean_html/unfamiliar.html"
            labelled.parent.mkdir(parents=True, exist_ok=True)
            labelled.write_text(
                "".join(
                    f'<article><h3>{j["title"]}</h3><a href="{j["job_url"]}">Apply</a></article>'
                    for j in good
                ),
                encoding="utf-8",
            )
            report = create_report(output, root / "reference", "test")
            for arm in report["arms"]:
                self.assertEqual(arm["retained"]["correct_titles"], 3)
                self.assertEqual(arm["last_valid_only"]["correct_titles"], 2)
                self.assertEqual(arm["retained"]["duplicate_predictions"], 0)
                self.assertEqual(arm["conflicted_jobs"], 1)
                self.assertAlmostEqual(arm["known_cost_usd"], 0.002)
            saved_path = next((output / "runs/test/units").glob("*.json"))
            saved = json.loads(saved_path.read_text(encoding="utf-8"))
            changed = {**saved, "jobs": []}
            write_json(saved_path, changed)
            with self.assertRaisesRegex(ValueError, "Saved retained records"):
                create_report(output, root / "reference", "test")
            write_json(saved_path, saved)
            (output / manifest["units"][-1]["file"]).write_text(
                "changed", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "Changed input"):
                await run_experiment(output, "fresh", "baseline", root / ".env", 1)


if __name__ == "__main__":
    unittest.main()
