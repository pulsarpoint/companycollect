"""Exercise the actual S3 SDK over HTTP, including conditional writes."""

import gzip
import hashlib
import io
import json
import tarfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import unquote

from crawler_service.service import CrawlJob, CrawlRequest
from crawler_service.service_results import ResultDeliveryError, S3Results, S3Settings
from crawler_service.storage import write_json


class S3Server:
    def __init__(self):
        self.objects = {}
        self.puts = 0
        self.fail = False
        self.entered = threading.Event()
        self.release = threading.Event()
        self.release.set()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def do_PUT(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                fixture.puts += 1
                fixture.entered.set()
                fixture.release.wait(timeout=10)
                key = unquote(self.path)
                if fixture.fail:
                    self.send_response(403)
                elif self.headers.get("If-None-Match") != "*":
                    self.send_response(400)
                elif key in fixture.objects:
                    self.send_response(412)
                else:
                    fixture.objects[key] = (dict(self.headers), body)
                    self.send_response(200)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def do_HEAD(self):
                headers, body = fixture.objects[unquote(self.path)]
                self.send_response(200)
                for key, value in headers.items():
                    if key.lower().startswith("x-amz-meta-") or key.lower() in {
                        "content-type",
                        "content-encoding",
                    }:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.settings = S3Settings(
            bucket="test-crawls",
            prefix="runs",
            endpoint_url=f"http://127.0.0.1:{self.server.server_port}",
        )

    def storage(self):
        return S3Results(
            self.settings,
            {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test"},
        )

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


class S3ResultTests(unittest.TestCase):
    def setUp(self):
        self.server = S3Server()
        self.addCleanup(self.server.close)
        self.storage = self.server.storage()
        self.addCleanup(self.storage.client.close)
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.request = CrawlRequest(
            request_id="example",
            url="https://example.com",
            pages=["https://example.com"],
        )
        self.folder = self.root / "jobs/example"
        self.result_file = self.folder / "attempts/0001/result.json"
        self.result = {
            "schema_version": "company-crawl-result/1.1",
            "documents": [{"html": "<p>Test</p>"}],
        }
        write_json(self.folder / "request.json", self.request.model_dump())
        write_json(self.result_file, self.result)
        (self.result_file.parent / "page.html").write_text("<p>Test</p>")
        self.job = CrawlJob(
            request_id="example",
            source="jetstream",
            state="completed",
            submitted_at="now",
            finished_at="later",
            crawl_status="finished",
            result_file=str(self.result_file.relative_to(self.root)),
        )

    def test_bundled_json_artifacts_checksums_and_conditional_retry(self):
        event = self.storage.prepare_event(self.root, self.job)
        self.assertEqual(event, self.storage.prepare_event(self.root, self.job))
        self.assertEqual(self.server.puts, 4)
        self.assertEqual(len(self.server.objects), 2)
        result = event["result"]
        headers, body = self.server.objects[f"/{result['bucket']}/{result['key']}"]
        self.assertEqual(hashlib.sha256(body).hexdigest(), result["sha256"])
        self.assertEqual(len(body), result["bytes"])
        self.assertEqual(json.loads(gzip.decompress(body)), self.result)
        self.assertEqual(headers["Content-Encoding"], "gzip")
        artifact = event["artifacts"]
        _, archive = self.server.objects[f"/{artifact['bucket']}/{artifact['key']}"]
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as saved:
            self.assertEqual(saved.getnames(), ["page.html"])
            self.assertEqual(saved.extractfile("page.html").read(), b"<p>Test</p>")

    def test_existing_different_result_is_never_overwritten(self):
        self.storage.prepare_event(self.root, self.job)
        before = dict(self.server.objects)
        write_json(self.result_file, {"documents": []})
        with self.assertRaisesRegex(ResultDeliveryError, "different result"):
            self.storage.prepare_event(self.root, self.job)
        self.assertEqual(before, self.server.objects)

    def test_failed_job_preserves_available_evidence_even_when_artifacts_disabled(self):
        self.request.save_artifacts = False
        write_json(self.folder / "request.json", self.request.model_dump())
        self.job.state = "failed"
        self.job.crawl_status = None
        self.job.error = "Crawl execution failed (OSError)"
        write_json(
            self.result_file,
            {"schema_version": "company-crawl-error/1.0", "error": self.job.error},
        )
        event = self.storage.prepare_event(self.root, self.job)
        self.assertEqual(event["state"], "failed")
        self.assertEqual(event["page_count"], 0)
        self.assertIsNotNone(event["artifacts"])
        self.assertEqual(len(self.server.objects), 2)

    def test_s3_error_excludes_internal_response(self):
        self.server.fail = True
        with self.assertRaisesRegex(
            ResultDeliveryError, r"S3 delivery failed \(ClientError\)"
        ):
            self.storage.prepare_event(self.root, self.job)
        self.assertFalse(self.server.objects)

    def test_partial_skipped_and_review_statuses_are_preserved(self):
        for status in ("partial", "skip_crawling", "needs_review", "failed"):
            with self.subTest(status=status):
                self.job.crawl_status = status
                event = self.storage.prepare_event(self.root, self.job)
                self.assertEqual(event["crawl_status"], status)
                self.assertEqual(event["state"], "completed")

    def test_changed_request_cannot_reuse_existing_object(self):
        self.storage.prepare_event(self.root, self.job)
        self.request.instructions = "Only jobs"
        write_json(self.folder / "request.json", self.request.model_dump())
        with self.assertRaisesRegex(ResultDeliveryError, "different result"):
            self.storage.prepare_event(self.root, self.job)


if __name__ == "__main__":
    unittest.main()
