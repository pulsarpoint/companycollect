"""Saved effort affects actual provider requests and Jev is verified as typed decisions."""

import json
import unittest
from unittest.mock import patch

import httpx
from pydantic import ValidationError

from crawler_service.llm_profile import EncryptedLLMProfile, verify_llm
from crawler_service.models import ResearchConfig
from crawler_service.service import CrawlRequest
from test_llm_profile import KEY, profile_payload
from test_package import response


class ModelReasoningTests(unittest.IsolatedAsyncioTestCase):
    async def test_verification_uses_selected_effort(self):
        for base, effort, expected in [
            ('https://api.deepseek.com', 'none', {'thinking': {'type': 'disabled'}}),
            ('https://api.deepseek.com', 'max', {'thinking': {'type': 'enabled'}, 'reasoning_effort': 'max'}),
            ('https://openrouter.ai/api/v1', 'high', {'reasoning': {'enabled': True, 'exclude': True, 'effort': 'high'}}),
            ('https://openrouter.ai/api/v1', 'none', {'reasoning': {'enabled': False}}),
        ]:
            seen = []
            async def send(client, request, **kwargs):
                seen.append(json.loads(request.content))
                return httpx.Response(200, json=response({'ok': True}), request=request)
            profile = EncryptedLLMProfile.model_validate(profile_payload(base_url=base, reasoning_effort=effort))
            self.assertEqual(profile.crawl_config(ResearchConfig(reasoning_effort='low')).reasoning_effort, effort)
            with self.subTest(base=base, effort=effort), patch.object(httpx.AsyncClient, 'send', send):
                self.assertEqual(await verify_llm(profile, {'CRAWLER_LLM_ENCRYPTION_KEY': KEY}), {'ok': True})
                self.assertEqual(len(seen), 1)
                for key, value in expected.items():
                    self.assertEqual(seen[0][key], value)
                if base.endswith('deepseek.com') and effort == 'none':
                    self.assertNotIn('reasoning_effort', seen[0])

    async def test_jev_typed_verification_and_response_validation(self):
        profile = EncryptedLLMProfile.model_validate(profile_payload(base_url='https://openrouter.ai/api/v1', model='typesafe/jev-1.13'))
        for choice, ok in [('ready', True), ('not_ready', False)]:
            async def send(client, request, **kwargs):
                self.assertEqual(str(request.url), 'https://openrouter.ai/api/alpha/decisions')
                body = json.loads(request.content)
                self.assertEqual(body['model'], 'typesafe/jev-1.13')
                self.assertEqual(body['questions']['status']['type'], 'choice')
                self.assertNotIn('reasoning', body)
                self.assertNotIn('messages', body)
                return httpx.Response(200, json={'answers': {'status': {'type': 'choice', 'choice': choice}}}, request=request)
            with patch.object(httpx.AsyncClient, 'send', send):
                self.assertEqual((await verify_llm(profile, {'CRAWLER_LLM_ENCRYPTION_KEY': KEY}))['ok'], ok)
        with self.assertRaises(ValidationError):
            CrawlRequest(url='https://novelic.com', site_info=True, llm=profile)

    async def test_jev_invalid_key_does_not_make_provider_request(self):
        profile = EncryptedLLMProfile.model_validate(profile_payload(base_url='https://openrouter.ai/api/v1', model='typesafe/jev-1.13'))
        with patch.object(httpx.AsyncClient, 'post') as post:
            self.assertFalse((await verify_llm(profile, {}))['ok'])
            post.assert_not_called()
