import tempfile
import unittest
from pathlib import Path

from ex3.candidates import PageCandidate
from ex3.prompty import create_page_selection_prompt
from ex4.candidates import CandidateSet
from ex4.prompts import load_prompts, prompt_hash, render_prompt

PROMPTS_DIR = Path("experiments/page-selection/prompts")


def _candidate_set() -> CandidateSet:
    return CandidateSet(
        domain="example.se",
        base_url="https://www.example.se/en/",
        preferred_languages=["en"],
        built_at="t",
        inventory_urls=2,
        eligible_urls=2,
        excluded_urls=0,
        candidates=[
            PageCandidate(
                url="https://www.example.se/en/",
                score=68.0,
                title="Home",
                language="en",
                source="inventory",
            ),
            PageCandidate(
                url="https://www.example.se/en/about-us",
                score=44.0,
                title="About us",
                language="en",
                source="inventory",
            ),
        ],
    )


class PromptTemplateTest(unittest.TestCase):
    def test_production_template_renders_identically_to_ex3(self) -> None:
        template = next(
            t for t in load_prompts(PROMPTS_DIR) if t.name == "p0-production"
        )
        candidate_set = _candidate_set()

        rendered = render_prompt(
            template,
            base_url=candidate_set.base_url,
            limit=20,
            candidate_set=candidate_set,
        )

        self.assertEqual(
            rendered,
            create_page_selection_prompt(
                candidate_set.base_url, candidates=candidate_set.candidates, limit=20
            ),
        )

    def test_all_four_variants_load_and_render(self) -> None:
        templates = load_prompts(PROMPTS_DIR)
        self.assertEqual(
            [t.name for t in templates],
            [
                "p0-production",
                "p1-minimal",
                "p2-classify-then-select",
                "p3-coverage-quota",
            ],
        )
        for template in templates:
            rendered = render_prompt(
                template,
                base_url="https://www.example.se/en/",
                limit=20,
                candidate_set=_candidate_set(),
            )
            self.assertIn("- identifiers:", rendered)
            self.assertIn("at most 20", rendered)
            self.assertIn("https://www.example.se/en/about-us", rendered)
            self.assertIn("Never invent", rendered)
            self.assertIn("SECURITY", rendered)
        self.assertEqual(len({prompt_hash(t.text) for t in templates}), 4)

    def test_rejects_unknown_placeholders_and_names(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "bad.md").write_text(
                "Hello {nope} {limit}", encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "nope"):
                load_prompts(Path(directory))
        with self.assertRaisesRegex(ValueError, "missing"):
            load_prompts(PROMPTS_DIR, ["missing"])


if __name__ == "__main__":
    unittest.main()
