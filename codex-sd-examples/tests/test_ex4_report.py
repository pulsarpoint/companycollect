import unittest

from ex4.report import render_report
from ex4.scoring import PromptSummary, ResultScore, RunScores


class ReportTest(unittest.TestCase):
    def test_renders_ranking_matrix_and_missed_fields(self) -> None:
        scores = RunScores(
            run_id="r1",
            scored_at="t",
            results=[
                ResultScore(
                    domain="a.se",
                    prompt_name="pa",
                    repeat=1,
                    coverage=0.5,
                    covered=["home"],
                    missed=["about"],
                    applicable=["home", "about"],
                    junk_rate=0.0,
                    other_language_rate=0.0,
                    picks=2,
                    warnings={"unknown": 0, "duplicate": 0, "limit": 0},
                    input_tokens=90,
                    output_tokens=10,
                    total_tokens=100,
                    latency_ms=1000,
                )
            ],
            prompts=[
                PromptSummary(
                    prompt_name="pa",
                    sites=1,
                    mean_coverage=0.5,
                    min_coverage=0.5,
                    mean_junk_rate=0.0,
                    mean_other_language_rate=0.0,
                    mean_total_tokens=100.0,
                    mean_latency_ms=1000.0,
                    failures=0,
                    stability=None,
                    missed_by_site={"a.se": ["about"]},
                )
            ],
        )

        markdown = render_report(scores)

        self.assertIn("| pa |", markdown)
        self.assertIn("| a.se |", markdown)
        self.assertIn("0.50", markdown)
        self.assertIn("about", markdown)
        self.assertIn("# Run r1", markdown)
