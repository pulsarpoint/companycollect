import unittest
from unittest.mock import patch

from ex3.seeding import HeadMetadata, SeedingOutcome
from ex4.candidates import build_site_candidates, candidates_hash, candidates_payload
from ex4.sites import Site


class BuildCandidatesTest(unittest.IsolatedAsyncioTestCase):
    async def test_ranks_caps_and_attaches_heads_using_the_production_code(
        self,
    ) -> None:
        site = Site(
            domain="example.se",
            start_url="https://www.example.se/en/",
            country="SE",
            size="mid",
            base_language="sv",
            sitemap_found=True,
        )

        async def fake_seed(start_url, **kwargs) -> SeedingOutcome:
            return SeedingOutcome(
                urls=[
                    "https://www.example.se/en/about-us",
                    "https://www.example.se/en/about-us/",
                    "https://www.example.se/en/reports/annual.pdf",
                    "https://www.example.se/sv/kontakt",
                    "https://www.example.se/en/careers",
                    "https://www.example.com/x",
                ]
            )

        async def fake_heads(urls, **kwargs) -> dict[str, HeadMetadata]:
            return {
                "https://www.example.se/en/about-us": HeadMetadata(
                    language="en", title="About us", description=None
                )
            }

        with (
            patch("ex4.candidates.seed_sitemap_urls", new=fake_seed),
            patch("ex4.candidates.fetch_head_metadata", new=fake_heads),
        ):
            candidate_set = await build_site_candidates(
                site, limit=3, accept_language="en", seed_max_urls=100
            )

        urls = [candidate.url for candidate in candidate_set.candidates]
        self.assertEqual(urls[0], "https://www.example.se/en/")
        self.assertEqual(len(urls), 3)
        self.assertNotIn("https://www.example.se/en/reports/annual.pdf", urls)
        self.assertNotIn("https://www.example.com/x", urls)
        self.assertEqual(len({u.rstrip("/") for u in urls}), 3)
        about = next(c for c in candidate_set.candidates if c.url.endswith("about-us"))
        self.assertEqual(about.title, "About us")
        self.assertEqual(sorted(candidate_set.preferred_languages), ["en", "sv"])
        # "about-us" and "about-us/" share one url_key, so the 6 seeded URLs plus
        # the injected base URL collapse to 6 unique url_keys (not 7).
        self.assertEqual(candidate_set.inventory_urls, 6)
        self.assertEqual(candidate_set.excluded_urls, 2)
        payload = candidates_payload(candidate_set)
        self.assertEqual(
            set(payload[0]), {"url", "title", "language", "anchor_text", "source"}
        )
        self.assertEqual(candidates_hash(candidate_set), candidates_hash(candidate_set))

    async def test_inventory_urls_dedupes_base_url_and_www_variants_by_url_key(
        self,
    ) -> None:
        site = Site(
            domain="example.se",
            start_url="https://www.example.se/en/",
            country="SE",
            size="mid",
            base_language="sv",
            sitemap_found=True,
        )

        async def fake_seed(start_url, **kwargs) -> SeedingOutcome:
            return SeedingOutcome(
                urls=[
                    # The base URL itself, also present in the sitemap.
                    "https://www.example.se/en/",
                    "https://www.example.se/en/about-us",
                    # A "www."-less variant of the URL above.
                    "https://example.se/en/about-us",
                    "https://www.example.se/en/careers",
                ]
            )

        async def fake_heads(urls, **kwargs) -> dict[str, HeadMetadata]:
            return {}

        with (
            patch("ex4.candidates.seed_sitemap_urls", new=fake_seed),
            patch("ex4.candidates.fetch_head_metadata", new=fake_heads),
        ):
            candidate_set = await build_site_candidates(
                site, limit=10, accept_language="en", seed_max_urls=100
            )

        # 4 seeded URLs + the injected base URL collapse to 3 unique url_keys:
        # base/about-us/careers. The base-URL duplicate and the www-less
        # duplicate must not inflate the count.
        self.assertEqual(candidate_set.inventory_urls, 3)


if __name__ == "__main__":
    unittest.main()
