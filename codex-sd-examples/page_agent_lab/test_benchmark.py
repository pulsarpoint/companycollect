"""Frozen custom datasets and single-arm reports keep their audit boundary."""

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from company_research.models import OBJECTIVES
from company_research.storage import content_hash, write_json

from page_agent_lab.agent import PageInput, validate_records
from page_agent_lab.audit import audit
from page_agent_lab.fixtures import prepare


class FrozenDatasetTests(unittest.TestCase):
    def test_single_arm_custom_dataset_and_tampered_source(self):
        with TemporaryDirectory() as directory:
            base = Path(directory)
            source = base / "corpus" / "company"
            source.mkdir(parents=True)
            html = '<h1>Example Labs</h1><a href="/jobs">Jobs</a>'
            (source / "source.html").write_text(html, encoding="utf-8")
            write_json(
                source / "pages.json",
                [
                    {
                        "page_id": "p0001",
                        "source_url": "https://example.test/",
                        "requested_url": "https://example.test/",
                        "fetched_at": "2026-09-16T00:00:00Z",
                        "html_file": "source.html",
                        "html_sha256": content_hash(html),
                        "earlier_model_analysis": "Must not be input",
                    }
                ],
            )
            dataset = base / "dataset.json"
            write_json(
                dataset,
                {
                    "sources": [["home", "company", "p0001", "https://example.test/"]],
                    "controls": {
                        "positive": [],
                        "negative": [
                            {"page": "home", "objective": "jobs", "forbidden": {}}
                        ],
                        "link_controls": [],
                    },
                },
            )
            root = base / "run"
            root.mkdir()
            manifest = prepare(root, base / "corpus", dataset)
            page = PageInput.load(root / "fixtures/home")
            self.assertEqual(page.html, html)
            self.assertNotIn("earlier_model_analysis", page.page)
            manifest.update(status="finished", modes=["one_pass"])
            write_json(root / "experiment.json", manifest)
            output = root / "one_pass"
            output.mkdir()
            write_json(
                output / "manifest.json", {"status": "finished", "wall_seconds": 0}
            )
            data = validate_records({o: [] for o in OBJECTIVES}, list(OBJECTIVES), page)
            data["processing"] = {
                "decisions": {},
                "routing_overrides": {},
                "responses": {},
            }
            write_json(
                output / "home.json",
                {
                    "data": data,
                    "links": [link | {"assessment": None} for link in page.links],
                },
            )
            report = audit(root)
            self.assertEqual(list(report["arms"]), ["one_pass"])
            self.assertEqual(report["routing_diagnostic"]["status"], "not_applicable")
            self.assertEqual(report["arms"]["one_pass"]["observed_links"], 1)
            self.assertFalse((root / "routed").exists())
            self.assertEqual(report["arms"]["one_pass"]["negative_passed"], 1)
            data["coverage"]["jobs"]["status"] = "failed"
            write_json(
                output / "home.json",
                {
                    "data": data,
                    "links": [link | {"assessment": None} for link in page.links],
                },
            )
            failed = audit(root)["arms"]["one_pass"]
            self.assertEqual(failed["negative_count"], 1)
            self.assertEqual(failed["negative_evaluated"], 0)
            self.assertEqual(failed["negative_passed"], 0)
            (root / "fixtures/home/page.html").write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Frozen input changed"):
                audit(root)


if __name__ == "__main__":
    unittest.main()
