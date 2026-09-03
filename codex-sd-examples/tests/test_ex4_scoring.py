import tempfile
import unittest
from pathlib import Path

from ex1.models import AnalysisTokenUsage, TokenUsageBreakdown
from ex3.candidates import PageCandidate
from ex3.crawler import save_model
from ex3.models import LlmCallStatus
from ex4.candidates import CandidateSet
from ex4.gold import GoldSet
from ex4.paths import DataDir
from ex4.runner import Pick, RunResult
from ex4.scoring import score_result, score_run, summarize

BASE = "https://www.a.se/"


def _candidates() -> CandidateSet:
    return CandidateSet(
        domain="a.se",
        base_url=BASE,
        preferred_languages=["en"],
        built_at="t",
        inventory_urls=3,
        eligible_urls=3,
        excluded_urls=0,
        candidates=[
            PageCandidate(url=BASE, score=60.0, language="en", source="inventory"),
            PageCandidate(
                url=BASE + "about", score=40.0, language="en", source="inventory"
            ),
            PageCandidate(
                url=BASE + "sv/om-oss", score=10.0, language="sv", source="inventory"
            ),
            PageCandidate(
                url=BASE + "privacy", score=1.0, language="en", source="inventory"
            ),
        ],
    )


def _gold() -> GoldSet:
    return GoldSet(
        domain="a.se",
        base_url=BASE,
        must_have={
            "home": [BASE],
            "about": [BASE + "about"],
            "contact": [],
            "management": [BASE + "team"],
            "careers": [],
            "products_services": [],
            "group_structure": [],
            "legal_identity": [],
        },
        junk=[BASE + "privacy"],
    )


def _result(
    prompt: str,
    picks: list[str],
    *,
    repeat: int = 1,
    error: str | None = None,
    tokens: int = 100,
) -> RunResult:
    usage = AnalysisTokenUsage(
        last=TokenUsageBreakdown(
            input_tokens=tokens - 10, output_tokens=10, total_tokens=tokens
        ),
        thread_total=TokenUsageBreakdown(total_tokens=tokens),
    )
    return RunResult(
        domain="a.se",
        prompt_name=prompt,
        repeat=repeat,
        prompt_hash="p",
        candidates_hash="c",
        cache_key="k",
        limit=20,
        picks=[Pick(url=u, reason="r") for u in picks],
        llm=LlmCallStatus(
            attempted=True,
            succeeded=error is None,
            error=error,
            token_usage=usage,
            warnings=["Ignored unknown page: x", "Ignored duplicate page: y"],
        ),
        latency_ms=1500,
        started_at="t",
    )


class ScoreResultTest(unittest.TestCase):
    def test_computes_coverage_junk_language_and_warnings(self) -> None:
        score = score_result(
            _result(
                "pa", [BASE, BASE + "about/", BASE + "sv/om-oss", BASE + "privacy"]
            ),
            _gold(),
            _candidates(),
        )

        self.assertAlmostEqual(score.coverage, 2 / 3)
        self.assertEqual(score.covered, ["home", "about"])
        self.assertEqual(score.missed, ["management"])
        self.assertAlmostEqual(score.junk_rate, 0.25)
        self.assertAlmostEqual(score.other_language_rate, 0.25)
        self.assertEqual(score.warnings, {"unknown": 1, "duplicate": 1, "limit": 0})
        self.assertEqual(score.total_tokens, 100)

    def test_failed_calls_score_zero(self) -> None:
        score = score_result(
            _result("pa", [], error="timed out"), _gold(), _candidates()
        )
        self.assertEqual(
            (score.coverage, score.picks, score.error), (0.0, 0, "timed out")
        )


class SummaryTest(unittest.TestCase):
    def test_ranks_by_coverage_then_junk_then_tokens_and_reports_stability(
        self,
    ) -> None:
        gold, cands = _gold(), _candidates()
        results = [
            score_result(
                _result("pa", [BASE, BASE + "about"], tokens=300), gold, cands
            ),
            score_result(
                _result("pa", [BASE, BASE + "about"], repeat=2, tokens=300), gold, cands
            ),
            score_result(
                _result("pb", [BASE, BASE + "about", BASE + "privacy"], tokens=100),
                gold,
                cands,
            ),
            score_result(_result("pc", [BASE], tokens=50), gold, cands),
        ]
        picks = {
            ("a.se", "pa", 1): {BASE, BASE + "about"},
            ("a.se", "pa", 2): {BASE, BASE + "about"},
        }

        summaries = summarize(results, picks)

        self.assertEqual([s.prompt_name for s in summaries], ["pa", "pb", "pc"])
        self.assertEqual(summaries[0].stability, 1.0)
        self.assertIsNone(summaries[1].stability)
        self.assertEqual(summaries[2].missed_by_site, {"a.se": ["about", "management"]})


class ScoreRunTest(unittest.TestCase):
    def test_scores_a_run_directory_and_skips_sites_without_gold(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data = DataDir(Path(directory))
            save_model(_candidates(), data.candidate_file("a.se"))
            save_model(_gold(), data.gold_file("a.se"))
            save_model(_result("pa", [BASE]), data.result_file("r1", "a.se", "pa", 1))
            save_model(
                _result("pa", [BASE]).model_copy(update={"domain": "nogold.se"}),
                data.result_file("r1", "nogold.se", "pa", 1),
            )

            scores = score_run("r1", data)

        self.assertEqual(
            [(s.domain, s.prompt_name) for s in scores.results], [("a.se", "pa")]
        )
        self.assertEqual(scores.prompts[0].prompt_name, "pa")


if __name__ == "__main__":
    unittest.main()
