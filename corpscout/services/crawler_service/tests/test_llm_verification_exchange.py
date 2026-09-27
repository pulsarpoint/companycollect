"""The settings preview matches the wire request and exposes safe, complete responses."""

import json
import unittest
from unittest.mock import patch

import httpx

from crawler_service.llm_profile import EncryptedLLMProfile, verify_llm
from test_llm_profile import API_KEY, KEY, profile_payload


class VerificationExchangeTests(unittest.IsolatedAsyncioTestCase):
    async def test_preview_makes_no_call_and_matches_each_provider_request(self):
        for model, base, effort in [
            ("deepseek-flash", "https://api.deepseek.com/v1", "max"),
            ("z-ai/glm-5.3-flash", "https://openrouter.ai/api/v1", "none"),
            ("local-model", "http://local-model/v1", None),
            ("typesafe/jev-1.13", "https://openrouter.ai/api/v1", None),
        ]:
            with self.subTest(model=model):
                payload = profile_payload(model=model, base_url=base, reasoning_effort=effort)
                profile = EncryptedLLMProfile.model_validate(payload)
                requests = []
                body = {"answers": {"status": {"type": "choice", "choice": "ready"}}} if profile.is_decision_model else {
                    "choices": [{"finish_reason": "stop", "message": {"content": '{"ok":true}', "reasoning_content": "Details " * 1000}}],
                    "usage": {"prompt_tokens": 123, "completion_tokens": 456},
                    "provider_metadata": {"value": [1, 2, 3]},
                }

                async def respond(_client, request):
                    requests.append(request)
                    return httpx.Response(200, json=body, request=request)

                with patch.object(httpx.AsyncClient, "_send_single_request", respond):
                    preview = await verify_llm(profile, {"CRAWLER_LLM_ENCRYPTION_KEY": KEY}, preview_only=True, include_exchange=True)
                    self.assertEqual(requests, [])
                    self.assertIsNone(preview["exchange"]["response"])
                    result = await verify_llm(profile, {"CRAWLER_LLM_ENCRYPTION_KEY": KEY}, include_exchange=True)
                self.assertTrue(result["ok"])
                self.assertEqual(len(requests), 1)
                self.assertEqual(result["exchange"]["request"], preview["exchange"]["request"])
                self.assertEqual(result["exchange"]["request"]["body"], json.loads(requests[0].content))
                self.assertEqual(json.loads(result["exchange"]["response"]["body"]), body)
                self.assertEqual(result["exchange"]["response"]["status"], 200)
                self.assertGreaterEqual(result["exchange"]["elapsed_ms"], 0)
                self.assertNotIn(API_KEY, json.dumps(result))
                self.assertNotIn(payload["api_key_encrypted"], json.dumps(result))
                self.assertNotIn("Authorization", json.dumps(result))

    async def test_errors_keep_full_response_and_redact_echoed_credentials(self):
        for status, content_type, body, kind in [
            (401, "application/json", json.dumps({"error": {"message": "Invalid " + API_KEY, "details": "x" * 5000}}), "configuration"),
            (200, "text/html", "<html>Unavailable " + API_KEY + "</html>", "capability"),
            (503, "text/plain", "Service unavailable " + API_KEY, "transient"),
        ]:
            async def respond(_client, request):
                return httpx.Response(status, content=body.encode(), headers={"Content-Type": content_type}, request=request)
            with self.subTest(status=status), patch.object(httpx.AsyncClient, "_send_single_request", respond):
                result = await verify_llm(EncryptedLLMProfile.model_validate(profile_payload()), {"CRAWLER_LLM_ENCRYPTION_KEY": KEY}, include_exchange=True)
            self.assertFalse(result["ok"])
            self.assertEqual(result["failure_kind"], kind)
            self.assertEqual(result["exchange"]["response"]["body"], body.replace(API_KEY, "[REDACTED]"))
            self.assertNotIn(API_KEY, json.dumps(result))

    async def test_transport_failure_retains_request_without_fabricating_response(self):
        async def fail(_client, request):
            raise httpx.ConnectError("private transport details", request=request)
        with patch.object(httpx.AsyncClient, "_send_single_request", fail):
            result = await verify_llm(EncryptedLLMProfile.model_validate(profile_payload()), {"CRAWLER_LLM_ENCRYPTION_KEY": KEY}, include_exchange=True)
        self.assertFalse(result["ok"])
        self.assertEqual(result["failure_kind"], "transient")
        self.assertIsNotNone(result["exchange"]["request"])
        self.assertIsNone(result["exchange"]["response"])
        self.assertNotIn("private transport details", json.dumps(result))
