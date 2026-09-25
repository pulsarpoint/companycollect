"""Encrypted Brave profiles verify image support without using a browser lease."""

import asyncio
import base64
import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import ValidationError

from browser_service.api import create_app
from browser_service.brave import BraveAsk, BraveAskRequest
from browser_service.llm_profile import EncryptedLLMProfile, LLMProfileError

KEY = "19" * 32
API_KEY = "sk-private-browser-test-secret"


def profile_payload(**overrides):
    profile = {
        "provider": "openrouter",
        "base_url": "https://llm-fixture/v1",
        "model": "glm-requires-reasoning",
    } | overrides
    aad = (
        "corpscout-crawler-llm:v1\0"
        + profile["provider"]
        + "\0"
        + profile["base_url"]
        + "\0"
        + profile["model"]
    ).encode()
    nonce = b"0123456789ab"
    encrypted = AESGCM(bytes.fromhex(KEY)).encrypt(nonce, API_KEY.encode(), aad)
    return profile | {
        "api_key_encrypted": "v1."
        + ".".join(
            base64.urlsafe_b64encode(value).decode().rstrip("=")
            for value in (nonce, encrypted)
        )
    }


class EnvelopeTests(unittest.TestCase):
    def test_node_fixture_and_legacy_request_shape(self):
        fixture = json.loads(
            (Path(__file__).parent / "fixtures/crawl-llm-envelope.json").read_text()
        )
        profile = EncryptedLLMProfile.model_validate(fixture["llm"])
        self.assertEqual(
            profile.decrypt_api_key(fixture["shared_key"]), fixture["api_key"]
        )
        self.assertNotIn(
            "llm", BraveAskRequest(request_id="old", query="company").model_dump()
        )

    def test_envelope_is_authenticated_and_rejects_plaintext(self):
        payload = profile_payload()
        profile = EncryptedLLMProfile.model_validate(payload)
        self.assertEqual(profile.decrypt_api_key(KEY), API_KEY)
        self.assertNotIn(API_KEY, repr(profile))
        for changes, key in [
            ({}, None),
            ({}, "20" * 32),
            ({"model": "swapped"}, KEY),
            ({"provider": "swapped"}, KEY),
            ({"base_url": "https://other.test/v1"}, KEY),
            ({"api_key_encrypted": "v1.a.a"}, KEY),
            ({"api_key_encrypted": payload["api_key_encrypted"][:-4] + "aaaa"}, KEY),
        ]:
            with self.subTest(changes=changes), self.assertRaises(LLMProfileError):
                EncryptedLLMProfile.model_validate(payload | changes).decrypt_api_key(
                    key
                )
        for changes in [
            {"api_key": API_KEY},
            {"model": "bad\0model"},
            {"provider": ""},
            {"base_url": "https://user:secret@llm.test"},
            {"base_url": "https://llm.test?api_key=secret"},
        ]:
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                EncryptedLLMProfile.model_validate(payload | changes)


class ProfileApiTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.sessions = {}

        async def claim(**kwargs):
            session = SimpleNamespace(
                id=kwargs["identifier"],
                execution_id="execution-" + kwargs["request_id"],
                profile=SimpleNamespace(id=kwargs["identifier"]),
                lock=asyncio.Lock(),
                operation=None,
            )
            self.sessions[session.id] = session
            return session

        async def release(identifier, **kwargs):
            self.sessions.pop(identifier, None)

        self.service = SimpleNamespace(
            root=self.root,
            claim=AsyncMock(side_effect=claim),
            release=AsyncMock(side_effect=release),
        )
        self.app = create_app(
            self.service,
            api_token="service-token",
            deepseek_api_key="legacy-secret",
            llm_encryption_key=KEY,
        )
        self.model_requests = []

    def client(self, app=None, authenticated=True):
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app or self.app),
            base_url="http://test",
            headers={"Authorization": "Bearer service-token"} if authenticated else {},
        )

    def model_reply(self, *, action=None, status=200, error=None):
        original = httpx.AsyncClient.send

        async def send(client, request, **kwargs):
            if request.url.host != "llm-fixture":
                return await original(client, request, **kwargs)
            self.assertEqual(
                str(request.url), "https://llm-fixture/v1/chat/completions"
            )
            self.assertEqual(request.headers["authorization"], "Bearer " + API_KEY)
            body = json.loads(request.content)
            self.model_requests.append(body)
            # This endpoint's GLM rejects forced reasoning settings.
            for field in ("reasoning", "thinking", "temperature", "provider"):
                self.assertNotIn(field, body)
            return httpx.Response(
                status,
                request=request,
                json=(
                    {"error": {"message": error}}
                    if status != 200
                    else {
                        "choices": [
                            {
                                "finish_reason": "stop",
                                "message": {
                                    "content": json.dumps(
                                        action
                                        or {
                                            "action": "finish",
                                            "outcome": "appears_clear",
                                            "reason": "blue",
                                        }
                                    )
                                },
                            }
                        ]
                    }
                ),
            )

        return patch.object(httpx.AsyncClient, "send", send)

    async def test_verify_requires_auth_and_checks_image_action_without_artifacts(self):
        async with self.client(authenticated=False) as client:
            self.assertEqual(
                (
                    await client.post(
                        "/v1/brave/llm/verify", json={"llm": profile_payload()}
                    )
                ).status_code,
                401,
            )
        noauth_app = create_app(self.service, api_token=None, llm_encryption_key=KEY)
        async with self.client(noauth_app) as client:
            self.assertEqual(
                (
                    await client.post(
                        "/v1/brave/llm/verify", json={"llm": profile_payload()}
                    )
                ).status_code,
                503,
            )
        with self.model_reply():
            async with self.client() as client:
                response = await client.post(
                    "/v1/brave/llm/verify", json={"llm": profile_payload()}
                )
        self.assertEqual(response.json(), {"ok": True})
        body = self.model_requests[0]
        self.assertEqual(body["model"], "glm-requires-reasoning")
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertTrue(
            body["messages"][1]["content"][1]["image_url"]["url"].startswith(
                "data:image/png;base64,"
            )
        )
        self.service.claim.assert_not_awaited()
        self.assertEqual(list(self.root.rglob("*")), [])

    async def test_verify_rejects_text_only_models_and_sanitizes_provider_errors(self):
        for reply in [
            dict(action={"ok": True}),
            dict(
                action={"action": "finish", "outcome": "appears_clear", "reason": "red"}
            ),
            dict(status=401, error="Credential rejected " + API_KEY),
        ]:
            with self.model_reply(**reply):
                async with self.client() as client:
                    response = await client.post(
                        "/v1/brave/llm/verify", json={"llm": profile_payload()}
                    )
            self.assertFalse(response.json()["ok"])
            self.assertNotIn(API_KEY, response.text)
            self.assertNotIn(KEY, response.text)

    async def test_validation_never_echoes_accidental_plaintext_credentials(self):
        async with self.client() as client:
            for path, body in [
                (
                    "/v1/brave/llm/verify",
                    {"llm": profile_payload() | {"api_key": API_KEY}},
                ),
                (
                    "/v1/brave/ask",
                    {
                        "request_id": "bad",
                        "query": "company",
                        "llm": profile_payload() | {"api_key": API_KEY},
                    },
                ),
            ]:
                response = await client.post(path, json=body)
                self.assertEqual(response.status_code, 422)
                self.assertNotIn(API_KEY, response.text)
        self.service.claim.assert_not_awaited()

    async def test_brave_uses_explicit_profile_and_persists_only_encrypted_key(self):
        async def execute(brave):
            self.assertEqual(brave.api_key, API_KEY)
            self.assertEqual(brave.request.llm.model, "glm-requires-reasoning")
            return {"request_id": brave.request.request_id, "status": "success"}

        with patch.object(BraveAsk, "run", execute):
            async with self.client() as client:
                response = await client.post(
                    "/v1/brave/ask",
                    json={
                        "request_id": "selected",
                        "query": "company",
                        "llm": profile_payload(),
                    },
                )
        self.assertEqual(response.json()["status"], "success")
        request = (self.root / "brave-requests/selected/request.json").read_text()
        self.assertIn("api_key_encrypted", request)
        self.assertNotIn(API_KEY, request)
        self.assertNotIn(KEY, request)
        self.assertFalse(self.sessions)

    async def test_brave_challenge_uses_selected_endpoint_and_vision_runtime(self):
        page = SimpleNamespace(
            url="https://search.brave.com/ask",
            content=AsyncMock(return_value="fixture"),
            screenshot=AsyncMock(return_value=b"fixture-image"),
            bring_to_front=AsyncMock(),
            evaluate=AsyncMock(return_value={"width": 800, "height": 600}),
            context=SimpleNamespace(
                new_cdp_session=AsyncMock(
                    return_value=SimpleNamespace(
                        send=AsyncMock(),
                        detach=AsyncMock(),
                    )
                )
            ),
        )
        session = await self.service.claim(identifier="a" * 32, request_id="runtime")
        session.profile.generation = "one"
        self.service.tab = lambda *_: SimpleNamespace(page=page)
        self.service.get = lambda *_: session
        self.service.touch = lambda *_: None
        directory = self.root / "request"
        directory.mkdir()
        brave = BraveAsk(
            self.service,
            session,
            BraveAskRequest(
                request_id="runtime",
                query="company",
                llm=profile_payload(),
            ),
            directory,
            API_KEY,
        )
        brave.tab = SimpleNamespace(page=page, document_status=200)
        brave.access_problem = AsyncMock(side_effect=["captcha", None])
        with self.model_reply(
            action={
                "action": "finish",
                "outcome": "appears_clear",
                "reason": "Fixture is clear " + API_KEY,
            }
        ):
            await brave.solve_challenge()
        self.assertEqual(brave.runs[0]["model"], "glm-requires-reasoning")
        self.assertIsNone(brave.runs[0]["reasoningEffort"])
        self.assertEqual(brave.runs[0]["reason"], "Fixture is clear [REDACTED]")
        self.assertEqual(self.model_requests[0]["model"], "glm-requires-reasoning")
        for path in self.root.rglob("*.json"):
            self.assertNotIn(API_KEY, path.read_text())

    async def test_cancel_only_target_request_persists_terminal_result_and_releases(
        self,
    ):
        started = {name: asyncio.Event() for name in ("target", "other")}
        finish_other = asyncio.Event()

        async def wait(brave):
            started[brave.request.request_id].set()
            await finish_other.wait()
            return {"request_id": brave.request.request_id, "status": "success"}

        with patch.object(BraveAsk, "run", wait):
            async with self.client() as client:
                pending = {
                    name: asyncio.create_task(
                        client.post(
                            "/v1/brave/ask", json={"request_id": name, "query": name}
                        )
                    )
                    for name in started
                }
                await asyncio.gather(*(event.wait() for event in started.values()))
                response = await client.post("/v1/brave/requests/target/cancel")
                self.assertEqual(response.json()["error_type"], "Cancelled")
                with self.assertRaises(asyncio.CancelledError):
                    await pending["target"]
                self.assertFalse(pending["other"].done())
                self.assertEqual(len(self.sessions), 1)
                replay = await client.post(
                    "/v1/brave/ask", json={"request_id": "target", "query": "target"}
                )
                self.assertEqual(replay.json()["error_type"], "Cancelled")
                finish_other.set()
                await pending["other"]
        self.assertFalse(self.sessions)
        self.assertEqual(
            json.loads((self.root / "brave-requests/target/result.json").read_text())[
                "error_type"
            ],
            "Cancelled",
        )
