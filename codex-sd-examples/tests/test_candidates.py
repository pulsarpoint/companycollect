import json
import tempfile
import unittest
from pathlib import Path

from ex3.candidates import (
    build_followup_candidates,
    build_selection_candidates,
    load_inventory_eligible,
)
from ex3.models import DiscoveredUrl, ScoredUrl
from ex3.seeding import HeadMetadata

BASE_URL = "https://www.example.se/en/"


class SelectionCandidatesTest(unittest.TestCase):
    def test_orders_by_score_attaches_heads_and_drops_excluded_urls(self) -> None:
        candidates = build_selection_candidates(
            inventory=[
                "https://www.example.se/en/reports/annual.pdf",
                "https://www.example.se/en/careers",
                "https://www.example.se/en/about-us",
                "https://www.example.com/en/about",
            ],
            base_url=BASE_URL,
            base_page_links=["https://www.example.se/en/careers"],
            heads={
                "https://www.example.se/en/about-us": HeadMetadata(
                    language="en", title="About us", description=None
                )
            },
            preferred_languages=frozenset({"en"}),
            limit=10,
        )

        urls = [candidate.url for candidate in candidates]
        self.assertEqual(urls[0], "https://www.example.se/en/")
        self.assertIn("https://www.example.se/en/about-us", urls)
        self.assertIn("https://www.example.se/en/careers", urls)
        self.assertNotIn("https://www.example.se/en/reports/annual.pdf", urls)
        self.assertNotIn("https://www.example.com/en/about", urls)
        about = next(c for c in candidates if c.url.endswith("about-us"))
        careers = next(c for c in candidates if c.url.endswith("careers"))
        self.assertEqual(about.title, "About us")
        self.assertEqual(about.language, "en")
        self.assertEqual(about.source, "inventory")
        self.assertEqual(careers.source, "base_page")

    def test_respects_the_limit(self) -> None:
        candidates = build_selection_candidates(
            inventory=[
                f"https://www.example.se/en/page-{index}" for index in range(30)
            ],
            base_url=BASE_URL,
            base_page_links=[],
            heads={},
            preferred_languages=frozenset({"en"}),
            limit=5,
        )

        self.assertEqual(len(candidates), 5)


class FollowupCandidatesTest(unittest.TestCase):
    def test_excludes_processed_pages_and_merges_inventory_leftovers(self) -> None:
        discovered = [
            DiscoveredUrl(
                url="https://www.example.se/en/about-us",
                domain="example.se",
                link_type="internal",
                labels=["About us"],
                source_urls=["https://www.example.se/en/"],
                occurrences=3,
            ),
            DiscoveredUrl(
                url="https://www.example.se/en/careers/",
                domain="example.se",
                link_type="internal",
                labels=["Careers"],
                source_urls=["https://www.example.se/en/"],
                occurrences=2,
            ),
            DiscoveredUrl(
                url="https://www.linkedin.com/company/example",
                domain="linkedin.com",
                link_type="external",
                labels=["LinkedIn"],
                source_urls=["https://www.example.se/en/"],
                occurrences=1,
            ),
        ]
        inventory_eligible = [
            ScoredUrl(
                url="https://www.example.se/en/management",
                score=44.0,
                reasons=["people"],
            ),
            ScoredUrl(
                url="https://www.example.se/en/careers", score=34.0, reasons=["careers"]
            ),
        ]

        candidates = build_followup_candidates(
            discovered_urls=discovered,
            inventory_eligible=inventory_eligible,
            processed_urls={
                "https://www.example.se/en/",
                "https://www.example.se/en/about-us",
            },
            base_url=BASE_URL,
            preferred_languages=frozenset({"en"}),
            limit=10,
        )

        urls = [candidate.url for candidate in candidates]
        self.assertEqual(
            set(urls),
            {
                "https://www.example.se/en/careers/",
                "https://www.example.se/en/management",
            },
        )
        careers = next(c for c in candidates if c.url.endswith("careers/"))
        self.assertEqual(careers.labels, ["Careers"])
        self.assertEqual(careers.occurrences, 2)
        self.assertEqual(careers.source, "discovered")


class InventoryLoadingTest(unittest.TestCase):
    def test_loads_eligible_urls_and_tolerates_a_missing_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "url-inventory.json"
            path.write_text(
                json.dumps(
                    {
                        "inventory_urls": 1,
                        "selected": [],
                        "eligible": [
                            {
                                "url": "https://www.example.se/en/x",
                                "score": 1.0,
                                "reasons": [],
                            }
                        ],
                        "excluded": [],
                    }
                ),
                encoding="utf-8",
            )

            loaded = load_inventory_eligible(path)
            missing = load_inventory_eligible(Path(directory) / "nope.json")

        self.assertEqual(
            [scored.url for scored in loaded], ["https://www.example.se/en/x"]
        )
        self.assertEqual(missing, [])
        self.assertEqual(load_inventory_eligible(None), [])


if __name__ == "__main__":
    unittest.main()
