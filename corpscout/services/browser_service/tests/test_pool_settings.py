"""One persisted capacity limit; startup never preallocates Chromium."""

import os
import unittest
from tempfile import TemporaryDirectory
from unittest.mock import patch

from test_browser_api import BrowserAPITests

from browser_service.cli import main
from browser_service.runtime import BrowserRuntimeSettings, BrowserService


class RuntimeSettingsTests(BrowserAPITests):
    async def test_sqlite_precedence_and_reset(self):
        settings = dict(
            max_browsers=3, idle_timeout_seconds=180, session_retention_days=14
        )
        endpoint = "/v1/browser/settings"
        for method in ("GET", "PUT", "DELETE"):
            response = await self.http.request(
                method, endpoint, headers={"Authorization": "wrong"}, json=settings
            )
            self.assertEqual(response.status_code, 401)
        for value in (-1, 65, True, "2", 1.5):
            self.assertEqual(
                (
                    await self.http.put(
                        endpoint, json=settings | {"max_browsers": value}
                    )
                ).status_code,
                422,
            )
        for key in ("idle_timeout_seconds", "session_retention_days"):
            self.assertEqual(
                (await self.http.put(endpoint, json=settings | {key: 0})).status_code,
                422,
            )
        self.assertEqual(
            (await self.http.put(endpoint, json=settings)).status_code, 200
        )
        restored = BrowserService(
            self.service.root,
            settings=BrowserRuntimeSettings(
                max_browsers=1, idle_timeout_seconds=30, session_retention_days=1
            ),
        )
        try:
            self.assertEqual(restored.settings.model_dump(), settings)
            self.assertEqual(restored.runtime_configuration()["source"], "sqlite")
        finally:
            restored.store.close()
        self.assertEqual((await self.http.delete(endpoint)).json()["source"], "startup")

    async def test_capacity_reduction_does_not_evict_current_execution(self):
        identifier = await self.reserve("busy")
        session = self.service.get(identifier)
        generation = session.profile.generation
        self.service.configure_runtime(
            BrowserRuntimeSettings(
                max_browsers=0, idle_timeout_seconds=180, session_retention_days=7
            )
        )
        self.assertEqual(session.profile.generation, generation)
        response = await self.http.post(
            "/v1/browser/sessions", json={"requestId": "next", "domain": "next.test"}
        )
        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            self.service.runtime_configuration()["capacity"],
            {"occupied": 1, "available": 0},
        )
        self.service.touch(identifier)
        await self.service.release(identifier)
        self.assertEqual(self.service.active, {})

    async def test_timeout_change_applies_at_next_heartbeat(self):
        identifier = await self.reserve("timeout")
        before = self.service.store.get(identifier)["expires_at"]
        self.service.configure_runtime(
            BrowserRuntimeSettings(
                max_browsers=2, idle_timeout_seconds=180, session_retention_days=7
            )
        )
        self.assertEqual(self.service.store.get(identifier)["expires_at"], before)
        response = await self.http.post(
            f"/v1/browser/sessions/{identifier}/heartbeat",
            headers=self.execution_headers(identifier),
        )
        self.assertEqual(response.status_code, 200)
        self.assertGreater(
            self.service.store.get(identifier)["expires_at"], before + 59
        )


class StartupSettingsTests(unittest.TestCase):
    def test_cli_overrides_environment_and_migrated_sqlite_overrides_cli(self):
        with (
            TemporaryDirectory() as directory,
            patch.dict(
                os.environ,
                {"BROWSER_STATE_DIR": directory, "BROWSER_MAX_BROWSERS": "2"},
                clear=True,
            ),
            patch("browser_service.cli.uvicorn.run"),
            patch("browser_service.cli.create_app") as create,
        ):
            with patch("sys.argv", ["browser-service", "--max-browsers", "5"]):
                main()
            first = create.call_args.args[0]
            self.assertEqual(first.settings.max_browsers, 5)
            self.assertEqual(first.active, {})
            first.store.set_setting(
                "browser_pool", {"headless_count": 2, "headed_count": 1}
            )
            first.store.set_setting("browser_runtime", {"idle_timeout_seconds": 180})
            first.store.close()
            with patch("sys.argv", ["browser-service"]):
                main()
            second = create.call_args.args[0]
            self.assertEqual(second.startup_settings.max_browsers, 2)
            self.assertEqual(second.settings.max_browsers, 3)
            self.assertEqual(second.idle_timeout, 180)
            self.assertEqual(second.settings.session_retention_days, 7)
            self.assertIsNone(second.store.get_setting("browser_pool"))
            second.store.close()
