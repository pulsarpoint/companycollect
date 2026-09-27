"""Saved effort is shared by CAPTCHA verification and actual assistant requests."""

import json
import unittest
from unittest.mock import patch

import httpx
from browser_service.challenge_agent import completion_payload
from browser_service.llm_profile import EncryptedLLMProfile, verify_llm
from test_llm_profile import KEY, profile_payload


class ModelReasoningTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_payloads_and_verification(self):
        for base, effort, expected in [
            ('https://api.deepseek.com', 'none', {'thinking': {'type': 'disabled'}}),
            ('https://api.deepseek.com', 'max', {'thinking': {'type': 'enabled'}, 'reasoning_effort': 'max'}),
            ('https://openrouter.ai/api/v1', 'high', {'reasoning': {'enabled': True, 'effort': 'high'}, 'provider': {'require_parameters': True}}),
            ('https://openrouter.ai/api/v1', 'none', {'reasoning': {'enabled': False}, 'provider': {'require_parameters': True}}),
        ]:
            async def send(client, request, **kwargs):
                body = json.loads(request.content)
                for key, value in expected.items():
                    self.assertEqual(body[key], value)
                return httpx.Response(200, json={'choices': [{'finish_reason': 'stop', 'message': {'content': '{"action":"finish","outcome":"appears_clear","reason":"blue"}'}}]}, request=request)
            with self.subTest(base=base, effort=effort):
                body = completion_payload('model', [], explicit_profile=True, reasoning_effort=effort, base_url=base)
                for key, value in expected.items():
                    self.assertEqual(body[key], value)
                profile = EncryptedLLMProfile.model_validate(profile_payload(base_url=base, reasoning_effort=effort))
                with patch.object(httpx.AsyncClient, 'send', send):
                    result = await verify_llm(profile, KEY)
                    self.assertTrue(result['ok'], result)

    async def test_jev_is_not_a_browser_assistant(self):
        profile = EncryptedLLMProfile.model_validate(profile_payload(base_url='https://openrouter.ai/api/v1', model='typesafe/jev-1.13'))
        with patch.object(httpx.AsyncClient, 'post') as post:
            self.assertFalse((await verify_llm(profile, KEY))['ok'])
            post.assert_not_called()
