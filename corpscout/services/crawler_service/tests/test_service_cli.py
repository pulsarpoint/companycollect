"""Validate service storage configuration through the command-line boundary."""

import io
import os
import unittest
from contextlib import redirect_stderr
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from fastapi.testclient import TestClient

from crawler_service.service_cli import main


class ServiceCliTests(unittest.TestCase):
    def test_s3_environment_and_flag_override(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            env = root / ".env"
            env.write_text(
                "CRAWL_S3_BUCKET=test-crawls\nCRAWL_S3_ENDPOINT_URL=\nCRAWL_S3_PREFIX=from-env\nAWS_ACCESS_KEY_ID=test\nAWS_SECRET_ACCESS_KEY=test\n"
            )
            services = []

            def capture(service, *, api_token):
                services.append(service)
                return object()

            with (
                patch.dict(os.environ, {}, clear=True),
                patch(
                    "sys.argv",
                    [
                        "crawler-service",
                        "--output-dir",
                        str(root / "results"),
                        "--env-file",
                        str(env),
                        "--s3-prefix",
                        "from-flag",
                    ],
                ),
                patch("crawler_service.service_cli.create_app", capture),
                patch("crawler_service.service_cli.uvicorn.run") as run,
            ):
                main()
            run.assert_called_once()
            [service] = services
            self.assertEqual(
                service.results.settings.model_dump(),
                {
                    "bucket": "test-crawls",
                    "prefix": "from-flag",
                    "endpoint_url": None,
                    "region": "us-east-1",
                },
            )
            service.results.client.close()

    def test_invalid_endpoint_credentials_are_not_echoed(self):
        with TemporaryDirectory() as temporary:
            output = io.StringIO()
            with (
                patch.dict(os.environ, {}, clear=True),
                patch(
                    "sys.argv",
                    [
                        "crawler-service",
                        "--output-dir",
                        temporary,
                        "--s3-bucket",
                        "test-crawls",
                        "--s3-endpoint-url",
                        "http://user:secret-value@localhost:9000",
                    ],
                ),
                redirect_stderr(output),
                self.assertRaises(SystemExit) as stopped,
            ):
                main()
            self.assertEqual(stopped.exception.code, 2)
            self.assertNotIn("secret-value", output.getvalue())

    def test_nats_options_are_rejected(self):
        with TemporaryDirectory() as temporary:
            with (
                patch.dict(os.environ, {}, clear=True),
                patch(
                    "sys.argv",
                    [
                        "crawler-service",
                        "--output-dir",
                        temporary,
                        "--transport",
                        "nats",
                    ],
                ),
                redirect_stderr(io.StringIO()),
                self.assertRaises(SystemExit) as stopped,
            ):
                main()
            self.assertEqual(stopped.exception.code, 2)

    def test_health_needs_no_broker(self):
        with TemporaryDirectory() as temporary:
            apps = []
            with (
                patch.dict(
                    os.environ,
                    {
                        "CRAWL_HUMAN_ENABLED": "false",
                        "NATS_URL": "nats://192.0.2.1:4222",
                    },
                    clear=True,
                ),
                patch(
                    "sys.argv",
                    ["crawler-service", "--output-dir", temporary],
                ),
                patch(
                    "crawler_service.service_cli.uvicorn.run",
                    lambda app, **_: apps.append(app),
                ),
            ):
                main()
            with TestClient(apps[0]) as client:
                self.assertEqual(client.get("/healthz").status_code, 200)
