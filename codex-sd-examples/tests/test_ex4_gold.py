import unittest

from ex3.candidates import PageCandidate
from ex4.candidates import CandidateSet
from ex4.gold import GOLD_FIELDS, GoldSet, draft_gold, field_coverage, junk_hits


def _set(urls_titles: list[tuple[str, str | None]]) -> CandidateSet:
    return CandidateSet(
        domain="example.se",
        base_url="https://www.example.se/en/",
        preferred_languages=["en"],
        built_at="t",
        inventory_urls=len(urls_titles),
        eligible_urls=len(urls_titles),
        excluded_urls=0,
        candidates=[
            PageCandidate(url=u, score=float(10 - i), title=t, source="inventory")
            for i, (u, t) in enumerate(urls_titles)
        ],
    )


class GoldDraftTest(unittest.TestCase):
    def test_drafts_every_field_from_slugs_titles_and_negatives(self) -> None:
        candidate_set = _set(
            [
                ("https://www.example.se/en/", "Home"),
                ("https://www.example.se/en/about-us", "About us"),
                ("https://www.example.se/en/organisation", "Our management team"),
                ("https://www.example.se/en/jobs", None),
                ("https://www.example.se/en/products", None),
                ("https://www.example.se/en/group/subsidiaries", None),
                ("https://www.example.se/en/legal-notice", "Imprint"),
                ("https://www.example.se/en/contact", "Contact us"),
                ("https://www.example.se/en/privacy", None),
            ]
        )

        gold = draft_gold(candidate_set)

        self.assertEqual(tuple(gold.must_have), GOLD_FIELDS)
        self.assertEqual(gold.must_have["home"], ["https://www.example.se/en/"])
        self.assertIn("https://www.example.se/en/about-us", gold.must_have["about"])
        self.assertEqual(
            gold.must_have["about"][0], "https://www.example.se/en/about-us"
        )
        self.assertIn(
            "https://www.example.se/en/organisation", gold.must_have["management"]
        )
        self.assertEqual(gold.must_have["careers"], ["https://www.example.se/en/jobs"])
        self.assertEqual(
            gold.must_have["products_services"], ["https://www.example.se/en/products"]
        )
        self.assertEqual(
            gold.must_have["group_structure"],
            ["https://www.example.se/en/group/subsidiaries"],
        )
        self.assertEqual(
            gold.must_have["legal_identity"], ["https://www.example.se/en/legal-notice"]
        )
        self.assertEqual(gold.junk, ["https://www.example.se/en/privacy"])
        self.assertIn("https://www.example.se/en/contact", gold.must_have["contact"])
        self.assertIn(
            "https://www.example.se/en/legal-notice", gold.must_have["contact"]
        )


class GoldMatchingTest(unittest.TestCase):
    def test_coverage_ignores_empty_fields_and_matches_by_url_key(self) -> None:
        gold = GoldSet(
            domain="example.se",
            base_url="https://www.example.se/en/",
            must_have={
                "home": ["https://www.example.se/en/"],
                "about": ["https://www.example.se/en/about-us"],
                "contact": [],
                "management": ["https://www.example.se/en/team"],
                "careers": [],
                "products_services": [],
                "group_structure": [],
                "legal_identity": [],
            },
            junk=["https://www.example.se/en/privacy"],
        )

        covered, applicable = field_coverage(
            gold,
            ["https://example.se/en/about-us/", "https://www.example.se/en/privacy"],
        )

        self.assertEqual(applicable, ["home", "about", "management"])
        self.assertEqual(covered, ["about"])
        self.assertEqual(
            junk_hits(
                gold,
                ["https://www.example.se/en/privacy/", "https://www.example.se/en/x"],
            ),
            ["https://www.example.se/en/privacy/"],
        )


if __name__ == "__main__":
    unittest.main()
