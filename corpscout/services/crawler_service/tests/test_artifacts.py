"""Portable results preserve analysis inputs without retaining debug files."""

import asyncio
import json
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from test_crawl import browser_responses

from crawler_service.captures import open_crawl
from crawler_service.crawl import crawl_company
from crawler_service.page_agent import PageInput

URL = "https://example.test/careers"
HTML = '<h1>Careers</h1><a href="/jobs/engineer">Engineer</a>'


class ArtifactRetentionTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_modes_publish_replayable_results(self):
        for save_artifacts in (True, False):
            with (
                self.subTest(save_artifacts=save_artifacts),
                TemporaryDirectory() as temporary,
            ):
                output = Path(temporary) / "output"
                requested = []
                with patch(
                    "crawler_service.crawl.open_browser",
                    lambda requested=requested: browser_responses(
                        {URL: (HTML, [], 200, None)}, requested
                    ),
                ):
                    manifest = await crawl_company(
                        URL,
                        output_dir=output,
                        pages=[URL],
                        save_artifacts=save_artifacts,
                    )
                result = output / "result.json"
                original = result.read_bytes()
                self.assertEqual(manifest["artifacts_saved"], save_artifacts)
                self.assertEqual(json.loads(original)["crawl"], manifest)
                self.assertEqual(requested, [URL])
                if save_artifacts:
                    self.assertTrue((output / "pages/p0001/input.json").is_file())
                else:
                    self.assertEqual(
                        [path.name for path in output.iterdir()], ["result.json"]
                    )
                    self.assertNotIn("html_file", manifest["pages"][0])
                    self.assertNotIn("snapshot", manifest["pages"][0])
                with open_crawl(result) as (loaded, folders):
                    self.assertEqual(loaded, manifest)
                    page = PageInput.load(folders[0])
                    self.assertEqual(page.html, HTML)
                    self.assertEqual(
                        page.links[0]["url"], "https://example.test/jobs/engineer"
                    )
                    self.assertEqual(page.headings[0]["text"], "Careers")
                    self.assertEqual((folders[0] / "link-page.html").read_text(), HTML)
                    replay_folder = folders[0]
                self.assertFalse(replay_folder.exists())
                self.assertEqual(result.read_bytes(), original)

    async def test_modified_or_incomplete_portable_inputs_are_rejected(self):
        with TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            with patch(
                "crawler_service.crawl.open_browser",
                lambda: browser_responses({URL: (HTML, [], 200, None)}, []),
            ):
                await crawl_company(
                    URL, output_dir=output, pages=[URL], save_artifacts=False
                )
            path = output / "result.json"
            original = path.read_text()
            for fault in ("html", "rendered_html", "missing", "duplicate", "metadata"):
                with self.subTest(fault=fault):
                    result = json.loads(original)
                    if fault in {"html", "rendered_html"}:
                        result["documents"][0][fault] = "changed"
                    elif fault == "missing":
                        result["documents"] = []
                    elif fault == "duplicate":
                        result["documents"].append(result["documents"][0])
                    else:
                        result["documents"][0]["input"]["page"]["link_html_sha256"] = (
                            None
                        )
                    path.write_text(json.dumps(result))
                    with self.assertRaises(ValueError), open_crawl(output):
                        pass

    async def test_interruption_cleans_temporary_working_artifacts(self):
        @asynccontextmanager
        async def interrupted_browser():
            raise asyncio.CancelledError
            yield  # pragma: no cover

        with TemporaryDirectory() as temporary:
            output = Path(temporary) / "output"
            scratch = Path(temporary) / "scratch"
            scratch.mkdir()
            with (
                patch("crawler_service.crawl.open_browser", interrupted_browser),
                patch(
                    "crawler_service.crawl.TemporaryDirectory",
                    side_effect=lambda **kwargs: TemporaryDirectory(
                        dir=scratch, **kwargs
                    ),
                ),
            ):
                with self.assertRaises(asyncio.CancelledError):
                    await crawl_company(
                        URL, output_dir=output, pages=[URL], save_artifacts=False
                    )
            self.assertEqual(list(scratch.iterdir()), [])
            self.assertEqual([path.name for path in output.iterdir()], ["result.json"])
            self.assertEqual(
                json.loads((output / "result.json").read_text())["crawl"]["status"],
                "failed",
            )
