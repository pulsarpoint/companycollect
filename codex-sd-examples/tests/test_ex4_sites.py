import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ex3.seeding import HeadMetadata, SeedingOutcome
from ex4.paths import DataDir
from ex4.sites import Site, SiteList, load_sites, save_sites, select_sites, verify_site


def _site(domain: str = "example.se", **overrides) -> Site:
    values = {
        "domain": domain,
        "start_url": f"https://www.{domain}/en/",
        "country": "SE",
        "size": "mid",
    }
    values.update(overrides)
    return Site(**values)


class DataDirTest(unittest.TestCase):
    def test_derives_every_path_from_the_root(self) -> None:
        data = DataDir(Path("/tmp/lab"))

        self.assertEqual(data.sites_file, Path("/tmp/lab/sites.json"))
        self.assertEqual(
            data.candidate_file("example.se"),
            Path("/tmp/lab/candidates/example.se.json"),
        )
        self.assertEqual(
            data.gold_file("example.se"), Path("/tmp/lab/gold/example.se.json")
        )
        self.assertEqual(
            data.result_file("20260903-120000", "example.se", "p0-production", 1),
            Path("/tmp/lab/runs/20260903-120000/example.se/p0-production/1.json"),
        )
        self.assertEqual(data.scores_file("r1"), Path("/tmp/lab/results/r1.json"))
        self.assertEqual(data.report_file("r1"), Path("/tmp/lab/results/r1.md"))


class SiteListTest(unittest.TestCase):
    def test_round_trips_and_selects_verified_sites(self) -> None:
        site_list = SiteList(
            sites=[
                _site("a.se", sitemap_found=True),
                _site("b.se", sitemap_found=False),
                _site("c.se"),
            ]
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "sites.json"
            save_sites(site_list, path)
            loaded = load_sites(path)

        self.assertEqual(loaded, site_list)
        self.assertEqual([s.domain for s in select_sites(loaded, None)], ["a.se"])
        self.assertEqual([s.domain for s in select_sites(loaded, ["b.se"])], ["b.se"])
        with self.assertRaises(ValueError):
            select_sites(loaded, ["nope.se"])


class VerifySiteTest(unittest.IsolatedAsyncioTestCase):
    async def test_records_sitemap_size_and_base_language(self) -> None:
        async def fake_seed(start_url, **kwargs) -> SeedingOutcome:
            return SeedingOutcome(
                urls=["https://www.example.se/en/a", "https://www.example.se/en/b"]
            )

        async def fake_heads(urls, **kwargs) -> dict[str, HeadMetadata]:
            return {
                urls[0]: HeadMetadata(language="sv", title="Example", description=None)
            }

        with (
            patch("ex4.sites.seed_sitemap_urls", new=fake_seed),
            patch("ex4.sites.fetch_head_metadata", new=fake_heads),
        ):
            verified = await verify_site(
                _site(), seed_max_urls=100, accept_language="en"
            )

        self.assertTrue(verified.sitemap_found)
        self.assertEqual(verified.inventory_urls, 2)
        self.assertEqual(verified.base_language, "sv")
        self.assertIsNotNone(verified.verified_at)
        self.assertIsNone(verified.error)

    async def test_marks_sites_without_a_sitemap_as_failed_without_raising(
        self,
    ) -> None:
        async def fake_seed(start_url, **kwargs) -> SeedingOutcome:
            return SeedingOutcome(urls=[], error="RuntimeError: boom")

        async def fake_heads(urls, **kwargs) -> dict[str, HeadMetadata]:
            return {}

        with (
            patch("ex4.sites.seed_sitemap_urls", new=fake_seed),
            patch("ex4.sites.fetch_head_metadata", new=fake_heads),
        ):
            verified = await verify_site(
                _site(), seed_max_urls=100, accept_language="en"
            )

        self.assertFalse(verified.sitemap_found)
        self.assertEqual(verified.inventory_urls, 0)
        self.assertIn("boom", verified.error or "")


if __name__ == "__main__":
    unittest.main()
