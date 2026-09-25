"""Encrypted profiles are authenticated before queuing and use the verified model."""

import asyncio
import base64
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import httpx
from browser_http_fixture import install_browser_api
from browser_http_fixture import original_send as browser_fixture_send
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import ValidationError
from test_crawl import browser_responses
from test_package import response
from test_site_info import COMPANY, HTML, SITE

from crawler_service.llm_profile import EncryptedLLMProfile, LLMProfileError
from crawler_service.service import CrawlRequest, CrawlService
from crawler_service.service_api import create_app

KEY = "19" * 32
API_KEY = "sk-private-crawler-test-secret"


def profile_payload(**overrides):
    profile = {
        "provider": "openrouter",
        "base_url": "https://llm-fixture/v1",
        "model": "selected-model",
    } | overrides
    aad = (
        "corpscout-crawler-llm:v1\0"
        + profile["provider"]
        + "\0"
        + profile["base_url"]
        + "\0"
        + profile["model"]
    ).encode("utf-8")
    nonce = b"0123456789ab"
    ciphertext = AESGCM(bytes.fromhex(KEY)).encrypt(nonce, API_KEY.encode(), aad)
    encoded = [
        base64.urlsafe_b64encode(value).decode().rstrip("=")
        for value in (nonce, ciphertext)
    ]
    return profile | {"api_key_encrypted": "v1." + ".".join(encoded)}


class EncryptedProfileTests(unittest.TestCase):
    def test_legacy_request_serialization_keeps_s3_request_hash_stable(self):
        request = CrawlRequest(url=SITE, pages=[SITE])
        self.assertNotIn("llm", request.model_dump())

    def test_decrypts_backoffice_node_encryption_fixture(self):
        fixture = json.loads(
            (Path(__file__).parent / "fixtures/crawl-llm-envelope.json").read_text(
                encoding="utf-8"
            )
        )
        profile = EncryptedLLMProfile.model_validate(fixture["llm"])
        self.assertEqual(
            profile.decrypt_api_key(
                {"CRAWLER_LLM_ENCRYPTION_KEY": fixture["shared_key"]}
            ),
            fixture["api_key"],
        )

    def test_authenticated_decryption_and_context_binding(self):
        payload = profile_payload()
        profile = EncryptedLLMProfile.model_validate(payload)
        self.assertEqual(
            profile.decrypt_api_key({"CRAWLER_LLM_ENCRYPTION_KEY": KEY}), API_KEY
        )
        self.assertNotIn(API_KEY, repr(profile))
        invalid_cases = [
            (payload, {"CRAWLER_LLM_ENCRYPTION_KEY": "20" * 32}),
            (payload, {"CRAWLER_LLM_ENCRYPTION_KEY": "short"}),
            (payload, {}),
            (payload | {"model": "other-model"}, {"CRAWLER_LLM_ENCRYPTION_KEY": KEY}),
            (payload | {"provider": "other"}, {"CRAWLER_LLM_ENCRYPTION_KEY": KEY}),
            (
                payload | {"base_url": "https://other.test/v1"},
                {"CRAWLER_LLM_ENCRYPTION_KEY": KEY},
            ),
            (
                payload
                | {"api_key_encrypted": payload["api_key_encrypted"][:-8] + "AAAAAAAA"},
                {"CRAWLER_LLM_ENCRYPTION_KEY": KEY},
            ),
            (
                payload | {"api_key_encrypted": "v1.a.a"},
                {"CRAWLER_LLM_ENCRYPTION_KEY": KEY},
            ),
            (
                payload | {"api_key_encrypted": "v2.aaa.bbb"},
                {"CRAWLER_LLM_ENCRYPTION_KEY": KEY},
            ),
        ]
        for candidate, environment in invalid_cases:
            with (
                self.subTest(candidate=candidate),
                self.assertRaises(LLMProfileError) as raised,
            ):
                EncryptedLLMProfile.model_validate(candidate).decrypt_api_key(
                    environment
                )
            self.assertNotIn(API_KEY, str(raised.exception))
            self.assertNotIn(KEY, str(raised.exception))

    def test_profile_rejects_plaintext_keys_and_unsafe_metadata(self):
        payload = profile_payload()
        invalid = [
            payload | {"api_key": API_KEY},
            payload | {"provider": "bad\0provider"},
        ]
        invalid += [
            payload | {"base_url": url}
            for url in (
                "file:///tmp/key",
                "https://user:password@provider.test",
                "https://provider.test?key=private",
                "https://provider.test#fragment",
                "https://provider.test:invalid",
                "https://provider.test/\npath",
            )
        ]
        for candidate in invalid:
            with self.subTest(candidate=candidate), self.assertRaises(ValidationError):
                EncryptedLLMProfile.model_validate(candidate)


class ProfileServiceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        install_browser_api(self)
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.service = CrawlService(
            self.root,
            {"CRAWLER_LLM_ENCRYPTION_KEY": KEY},
            concurrency=1,
            max_pending=2,
        )
        self.app = create_app(self.service, api_token="service-token")
        self.model_requests = []

    def llm_response(self, status=200, document=None):
        async def handle(client, request, **kwargs):
            if request.url.host not in {
                "llm-fixture",
                "api.deepseek.com",
                "openrouter.ai",
            }:
                return await browser_fixture_send(client, request, **kwargs)
            self.assertIn(
                request.url.path, {"/v1/chat/completions", "/api/v1/chat/completions"}
            )
            self.assertEqual(request.headers["authorization"], f"Bearer {API_KEY}")
            self.model_requests.append(json.loads(request.content))
            if status != 200:
                return httpx.Response(
                    status,
                    json={
                        "error": {
                            "message": f"No allowed providers for selected model ({API_KEY})"
                        }
                    },
                )
            return httpx.Response(
                200, json=response({"ok": True} if document is None else document)
            )

        return patch.object(httpx.AsyncClient, "send", handle)

    async def test_verification_distinguishes_permanent_and_transient_provider_failures(self):
        from crawler_service.llm_profile import verify_llm
        for status, kind in [(401, "configuration"), (403, "configuration"), (404, "configuration"), (429, "transient"), (503, "transient"), (400, "capability")]:
            with self.subTest(status=status), self.llm_response(status=status):
                result = await verify_llm(EncryptedLLMProfile.model_validate(profile_payload()), {"CRAWLER_LLM_ENCRYPTION_KEY": KEY})
                self.assertEqual(result["failure_kind"], kind)
                self.assertNotIn(API_KEY, json.dumps(result))

    async def test_verify_requires_auth_and_never_creates_a_job_or_artifact(self):
        async with self.app.router.lifespan_context(self.app):
            before = sorted(self.root.rglob("*"))
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(self.app), base_url="http://service"
            ) as client:
                with self.llm_response():
                    denied = await client.post(
                        "/v1/llm/verify", json={"llm": profile_payload()}
                    )
                    self.assertEqual(denied.status_code, 401)
                    self.assertEqual(self.model_requests, [])
                    client.headers["Authorization"] = "Bearer service-token"
                    for provider, base_url, api in (
                        ("GLM Flash", "https://openrouter.ai/api/v1", "openrouter"),
                        ("DS Flash", "https://api.deepseek.com/v1", "deepseek"),
                        ("custom-provider", "https://llm-fixture/v1", "openai"),
                    ):
                        result = await client.post(
                            "/v1/llm/verify",
                            json={
                                "llm": profile_payload(
                                    provider=provider, base_url=base_url
                                )
                            },
                        )
                        self.assertEqual(result.json(), {"ok": True})
                        request = self.model_requests[-1]
                        self.assertEqual(request["model"], "selected-model")
                        self.assertEqual(
                            request["response_format"]["type"],
                            "json_object" if api == "deepseek" else "json_schema",
                        )
                        self.assertNotIn("only", request.get("provider", {}))
                        self.assertEqual(request["max_tokens"], 2048)
                        for parameter in (
                            "reasoning",
                            "thinking",
                            "reasoning_effort",
                            "temperature",
                        ):
                            self.assertNotIn(parameter, request)
                        if provider == "custom-provider":
                            self.assertNotIn("provider", request)
                            self.assertNotIn("reasoning", request)
            self.assertEqual(self.service.jobs, {})
            self.assertEqual(sorted(self.root.rglob("*")), before)

    async def test_verify_reports_permanent_provider_failure_without_retry_or_secret(
        self,
    ):
        async with self.app.router.lifespan_context(self.app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(self.app),
                base_url="http://service",
                headers={"Authorization": "Bearer service-token"},
            ) as client:
                with self.llm_response(status=404):
                    result = await client.post(
                        "/v1/llm/verify", json={"llm": profile_payload()}
                    )
                self.assertEqual(result.status_code, 200)
                self.assertFalse(result.json()["ok"])
                self.assertIn("HTTP 404", result.json()["error"])
                self.assertIn("No allowed providers", result.json()["error"])
                self.assertNotIn(API_KEY, result.text)
                self.assertEqual(len(self.model_requests), 1)
                self.assertEqual(self.service.jobs, {})

    async def test_verify_rejects_invalid_json_and_has_a_total_deadline(self):
        actual_timeout = asyncio.timeout

        async def delayed(client, request, **kwargs):
            if request.url.host == "llm-fixture":
                self.model_requests.append(json.loads(request.content))
                await asyncio.sleep(1)
            return await browser_fixture_send(client, request, **kwargs)

        async with self.app.router.lifespan_context(self.app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(self.app),
                base_url="http://service",
                headers={"Authorization": "Bearer service-token"},
            ) as client:
                with self.llm_response(document={"wrong": True}):
                    invalid = await client.post(
                        "/v1/llm/verify", json={"llm": profile_payload()}
                    )
                self.assertFalse(invalid.json()["ok"])
                self.assertIn("required JSON", invalid.json()["error"])
                with (
                    patch.object(httpx.AsyncClient, "send", delayed),
                    patch(
                        "crawler_service.llm_profile.asyncio.timeout",
                        lambda _: actual_timeout(0.01),
                    ),
                ):
                    timed_out = await client.post(
                        "/v1/llm/verify", json={"llm": profile_payload()}
                    )
                self.assertFalse(timed_out.json()["ok"])
                self.assertIn("deadline", timed_out.json()["error"])
                self.assertEqual(len(self.model_requests), 2)
                self.assertEqual(self.service.jobs, {})

    async def test_invalid_encrypted_key_is_rejected_before_job_and_browser(self):
        payload = profile_payload() | {"model": "tampered-model"}
        async with self.app.router.lifespan_context(self.app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(self.app),
                base_url="http://service",
                headers={"Authorization": "Bearer service-token"},
            ) as client:
                verified = await client.post("/v1/llm/verify", json={"llm": payload})
                self.assertFalse(verified.json()["ok"])
                for endpoint in ("/v1/crawls", "/v1/crawls/validate"):
                    rejected = await client.post(
                        endpoint, json={"url": SITE, "site_info": True, "llm": payload}
                    )
                    self.assertEqual(rejected.status_code, 422)
                    self.assertIn("authenticated", rejected.text)
                rejected = await client.post(
                    "/v1/crawls",
                    json={"url": SITE, "llm": profile_payload() | {"api_key": API_KEY}},
                )
                self.assertEqual(rejected.status_code, 422)
                self.assertNotIn(API_KEY, rejected.text)
                self.assertEqual(self.service.jobs, {})
                self.assertFalse((self.root / "jobs").exists())

    async def test_crawl_uses_selected_profile_and_never_persists_plaintext_key(self):
        payload = profile_payload(provider="custom-provider")
        requested = []
        async with self.app.router.lifespan_context(self.app):
            with (
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda *_args, **_: browser_responses(
                        {SITE: (HTML, [], 200, None)}, requested
                    ),
                ),
                self.llm_response(document=COMPANY),
            ):
                self.service.submit(
                    CrawlRequest.model_validate(
                        {
                            "request_id": "encrypted-profile-crawl",
                            "url": SITE,
                            "site_info": True,
                            "llm": payload,
                            "config": {"model": "old-model", "provider": "baidu/fp8"},
                        }
                    ),
                    source="rest",
                )
                job = await asyncio.wait_for(
                    self.service.wait("encrypted-profile-crawl"), timeout=5
                )
            self.assertEqual(job.state, "completed")
            self.assertEqual(job.crawl_status, "finished")
            self.assertEqual(requested, [SITE])
            self.assertEqual(self.model_requests[0]["model"], "selected-model")
            self.assertNotIn("provider", self.model_requests[0])
            for parameter in (
                "reasoning",
                "thinking",
                "reasoning_effort",
                "temperature",
            ):
                self.assertNotIn(parameter, self.model_requests[0])
            result = json.loads(
                (self.root / job.result_file).read_text(encoding="utf-8")
            )
            self.assertEqual(result["crawl"]["config"]["model"], "selected-model")
            self.assertIsNone(result["crawl"]["config"]["provider"])
            saved_request = json.loads(
                (self.root / "jobs/encrypted-profile-crawl/request.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(saved_request["llm"], payload)
            for path in self.root.rglob("*"):
                if path.is_file():
                    self.assertNotIn(API_KEY.encode(), path.read_bytes(), str(path))
                    if path.name != "request.json":
                        self.assertNotIn(
                            payload["api_key_encrypted"].encode(),
                            path.read_bytes(),
                            str(path),
                        )

    async def test_mandatory_reasoning_model_defaults_are_shared_by_verify_and_crawl(
        self,
    ):
        payload = profile_payload(
            provider="GLM Flash", base_url="https://openrouter.ai/api/v1"
        )
        browser_requests = []

        async def mandatory_reasoning(client, request, **kwargs):
            if request.url.host != "openrouter.ai":
                return await browser_fixture_send(client, request, **kwargs)
            body = json.loads(request.content)
            self.model_requests.append(body)
            if "reasoning" in body:
                return httpx.Response(
                    400,
                    json={
                        "error": {
                            "message": "Reasoning is mandatory for this endpoint and cannot be disabled."
                        }
                    },
                )
            self.assertNotIn("temperature", body)
            self.assertNotIn("only", body["provider"])
            document = {"ok": True} if len(self.model_requests) == 1 else COMPANY
            return httpx.Response(200, json=response(document))

        async with self.app.router.lifespan_context(self.app):
            with (
                patch.object(httpx.AsyncClient, "send", mandatory_reasoning),
                patch(
                    "crawler_service.crawl.open_browser",
                    lambda *_args, **_: browser_responses(
                        {SITE: (HTML, [], 200, None)}, browser_requests
                    ),
                ),
            ):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(self.app),
                    base_url="http://service",
                    headers={"Authorization": "Bearer service-token"},
                ) as client:
                    verified = await client.post(
                        "/v1/llm/verify", json={"llm": payload}
                    )
                    self.assertEqual(verified.json(), {"ok": True})
                    submitted = await client.post(
                        "/v1/crawls",
                        json={
                            "request_id": "mandatory-reasoning",
                            "url": SITE,
                            "site_info": True,
                            "llm": payload,
                            "config": {
                                "reasoning_effort": "none",
                                "provider": "baidu/fp8",
                            },
                        },
                    )
                    self.assertEqual(submitted.status_code, 202)
                    job = await asyncio.wait_for(
                        self.service.wait("mandatory-reasoning"), timeout=5
                    )
            self.assertEqual(job.crawl_status, "finished")
            self.assertEqual(len(self.model_requests), 2)
            self.assertEqual(self.model_requests[0]["max_tokens"], 2048)
            self.assertEqual(browser_requests, [SITE])
