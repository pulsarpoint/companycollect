import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ex3.candidates import PageCandidate
from ex3.crawler import save_model
from ex3.models import LlmCallStatus, ScoredUrl
from ex4.candidates import CandidateSet, load_candidate_set
from ex4.paths import DataDir
from ex4.prompts import load_prompts, render_prompt
from ex4.runner import (
    RunResult,
    RunSettings,
    cache_key,
    execute_run,
    picks_from_scored,
    plan_calls,
)
from ex4.sites import Site, SiteList, save_sites

PROMPT = "R:{requirements}\nL:{limit}\n{candidates}\n"


def _lab(directory: Path) -> DataDir:
    data = DataDir(directory)
    data.prompts.mkdir(parents=True)
    (data.prompts / "pa.md").write_text(PROMPT, encoding="utf-8")
    (data.prompts / "pb.md").write_text(PROMPT + "B", encoding="utf-8")
    save_sites(
        SiteList(
            sites=[
                Site(
                    domain="a.se",
                    start_url="https://www.a.se/",
                    country="SE",
                    size="mid",
                    sitemap_found=True,
                ),
                Site(
                    domain="b.se",
                    start_url="https://www.b.se/",
                    country="SE",
                    size="mid",
                    sitemap_found=True,
                ),
            ]
        ),
        data.sites_file,
    )
    for domain in ("a.se", "b.se"):
        save_model(
            CandidateSet(
                domain=domain,
                base_url=f"https://www.{domain}/",
                preferred_languages=["en"],
                built_at="t",
                inventory_urls=2,
                eligible_urls=2,
                excluded_urls=0,
                candidates=[
                    PageCandidate(
                        url=f"https://www.{domain}/", score=60.0, source="inventory"
                    ),
                    PageCandidate(
                        url=f"https://www.{domain}/about",
                        score=40.0,
                        source="inventory",
                    ),
                ],
            ),
            data.candidate_file(domain),
        )
    return data


class RunnerTest(unittest.IsolatedAsyncioTestCase):
    async def test_runs_the_matrix_writes_results_and_resumes_from_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _lab(Path(directory))
            calls: list[tuple[str, str]] = []

            async def fake_select(
                candidates,
                *,
                base_url,
                limit,
                timeout_seconds,
                prompt: str | None = None,
            ):
                assert prompt is not None
                calls.append((base_url, prompt[:12]))
                if base_url == "https://www.b.se/" and "B" in prompt:
                    return [], LlmCallStatus(
                        attempted=True, succeeded=False, error="timed out"
                    )
                return [
                    ScoredUrl(
                        url=candidates[1].url,
                        score=40.0,
                        reasons=[
                            "llm",
                            "about page",
                            "fields: description, founded_year",
                        ],
                    )
                ], LlmCallStatus(attempted=True, succeeded=True)

            settings = RunSettings(run_id="r1", data=data, limit=5, concurrency=2)
            planned = plan_calls(settings)
            self.assertEqual(len(planned), 4)
            self.assertFalse(any(p.cached for p in planned))

            with patch("ex4.runner.select_pages_with_llm", new=fake_select):
                executed, cached, failed = await execute_run(settings)
            self.assertEqual((executed, cached, failed), (4, 0, 1))
            self.assertTrue((data.run_dir("r1") / "manifest.json").exists())
            result = RunResult.model_validate_json(
                data.result_file("r1", "a.se", "pa", 1).read_text(encoding="utf-8")
            )
            self.assertEqual(result.picks[0].url, "https://www.a.se/about")
            self.assertEqual(
                result.picks[0].expected_fields, ["description", "founded_year"]
            )
            self.assertEqual(result.picks[0].reason, "about page")
            template = load_prompts(data.prompts, ["pa"])[0]
            rendered = render_prompt(
                template,
                base_url="https://www.a.se/",
                limit=5,
                candidate_set=load_candidate_set(data.candidate_file("a.se")),
            )
            self.assertEqual(
                result.cache_key, cache_key(rendered, result.candidates_hash, 5)
            )
            failed_result = RunResult.model_validate_json(
                data.result_file("r1", "b.se", "pb", 1).read_text(encoding="utf-8")
            )
            self.assertEqual(failed_result.llm.error, "timed out")

            # resume: successes are cached, the failure is re-run only with retry_failed
            calls.clear()
            with patch("ex4.runner.select_pages_with_llm", new=fake_select):
                self.assertEqual(await execute_run(settings), (0, 4, 0))
                self.assertEqual(
                    await execute_run(
                        RunSettings(run_id="r1", data=data, limit=5, retry_failed=True)
                    ),
                    (1, 3, 1),
                )
            self.assertEqual(len(calls), 1)

    async def test_dry_run_plans_without_calling(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = _lab(Path(directory))

            async def fake_select(*args, **kwargs):
                self.fail("dry run must not call the model")

            with patch("ex4.runner.select_pages_with_llm", new=fake_select):
                result = await execute_run(
                    RunSettings(
                        run_id="dry",
                        data=data,
                        prompt_names=("pa",),
                        domains=("a.se",),
                        repeats=2,
                        dry_run=True,
                    )
                )
            self.assertEqual(result, (0, 0, 0))
            self.assertEqual(
                len(
                    plan_calls(
                        RunSettings(
                            run_id="dry",
                            data=data,
                            prompt_names=("pa",),
                            domains=("a.se",),
                            repeats=2,
                        )
                    )
                ),
                2,
            )

    def test_pick_parsing_and_cache_key_stability(self) -> None:
        picks = picks_from_scored(
            [
                ScoredUrl(url="u", score=1.0, reasons=["llm", "why"]),
                ScoredUrl(url="v", score=1.0, reasons=["llm", "why", "fields: a, b"]),
            ]
        )
        self.assertEqual(
            [(p.url, p.reason, p.expected_fields) for p in picks],
            [("u", "why", []), ("v", "why", ["a", "b"])],
        )
        self.assertEqual(cache_key("p", "c", 20), cache_key("p", "c", 20))
        self.assertNotEqual(cache_key("p", "c", 20), cache_key("p", "c", 21))


if __name__ == "__main__":
    unittest.main()
