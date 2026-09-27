"""Saved credentials cross Node/Python without provider keys in run configuration."""

import json
import base64
from pathlib import Path
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from openai import OpenAIError

from dagster_v3.defs.common.encrypted_llm import EncryptedLLMConfig
from dagster_v3.defs.esef_filings import llm_enrichment_assets as esef
from dagster_v3.defs.se_company.basic_info.llm import LlmSuggestionProfile
from dagster_v3.defs.se_company.domain.verification import DomainVerificationProfile, verify_domain
from dagster_v3.defs.se_company.info import build_llm_client
from dagster_v3.defs.se_company.person.match import PersonMatchProfile

FIXTURE = json.loads((Path(__file__).parent / 'fixtures/encrypted-llm-envelope.json').read_text(encoding='utf-8'))


def test_decrypts_backoffice_node_envelope(monkeypatch):
    monkeypatch.setenv('CRAWLER_LLM_ENCRYPTION_KEY', FIXTURE['shared_key'])
    assert EncryptedLLMConfig(**FIXTURE['llm']).decrypt_api_key() == FIXTURE['api_key']


def test_keyless_saved_profiles_create_unauthenticated_clients_without_env_fallback(monkeypatch):
    monkeypatch.setenv('CRAWLER_LLM_ENCRYPTION_KEY', FIXTURE['shared_key'])
    monkeypatch.setenv('OPENAI_API_KEY', 'must-not-use-host-secret')
    data = FIXTURE['llm'].copy()
    nonce = b'0123456789ab'
    aad = f"corpscout-crawler-llm:v1\0{data['provider']}\0{data['base_url']}\0{data['model']}".encode()
    encrypted = AESGCM(bytes.fromhex(FIXTURE['shared_key'])).encrypt(nonce, b'', aad)
    data['api_key_encrypted'] = 'v1.' + '.'.join(base64.urlsafe_b64encode(part).decode().rstrip('=') for part in (nonce, encrypted))
    assert EncryptedLLMConfig(**data).decrypt_api_key() == ''
    with build_llm_client(PersonMatchProfile(**data), timeout_seconds=10) as client:
        assert client.api_key == ''
        assert 'Authorization' not in client.auth_headers
    with esef.build_esef_llm_client(esef.EsefLlmEnrichmentConfig(**data)) as client:
        assert client.api_key == ''
        assert 'Authorization' not in client.auth_headers


@pytest.mark.parametrize('change', [
    {'model': 'different-model'}, {'base_url': 'https://different.example/v1'},
    {'provider': 'different-provider'},
])
def test_cannot_retarget_encrypted_credentials(monkeypatch, change):
    monkeypatch.setenv('CRAWLER_LLM_ENCRYPTION_KEY', FIXTURE['shared_key'])
    with pytest.raises(ValueError, match='authenticated') as caught:
        EncryptedLLMConfig(**(FIXTURE['llm'] | change)).decrypt_api_key()
    assert FIXTURE['api_key'] not in str(caught.value)


@pytest.mark.parametrize('profile_type,extra', [
    (PersonMatchProfile, {}), (LlmSuggestionProfile, {}),
    (DomainVerificationProfile, {'system_prompt': 'Verify supplied company evidence', 'prompt_version': 'test-v1'}),
])
def test_company_clients_use_saved_key_instead_of_host_provider_key(monkeypatch, profile_type, extra):
    monkeypatch.setenv('CRAWLER_LLM_ENCRYPTION_KEY', FIXTURE['shared_key'])
    monkeypatch.setenv('UNRELATED_KEY', 'wrong-host-key')
    profile = profile_type(**FIXTURE['llm'], **extra)
    with build_llm_client(profile, timeout_seconds=10, api_key_environment_variable='UNRELATED_KEY') as client:
        assert client.api_key == FIXTURE['api_key']
        assert str(client.base_url).rstrip('/') == FIXTURE['llm']['base_url'].rstrip('/')
    assert FIXTURE['api_key'] not in json.dumps(profile.model_dump())


def test_invalid_encrypted_key_never_falls_back_to_host_key(monkeypatch):
    monkeypatch.setenv('CRAWLER_LLM_ENCRYPTION_KEY', 'ff' * 32)
    monkeypatch.setenv('FALLBACK_KEY', 'available-host-key')
    with pytest.raises(ValueError, match='authenticated'):
        build_llm_client(PersonMatchProfile(**FIXTURE['llm']), timeout_seconds=10, api_key_environment_variable='FALLBACK_KEY')


def test_esef_client_uses_same_encrypted_profile(monkeypatch):
    monkeypatch.setenv('CRAWLER_LLM_ENCRYPTION_KEY', FIXTURE['shared_key'])
    with esef.build_esef_llm_client(esef.EsefLlmEnrichmentConfig(**FIXTURE['llm'])) as client:
        assert client.api_key == FIXTURE['api_key']


def test_provider_errors_do_not_persist_plaintext_keys():
    def fail(*args, **kwargs):
        raise OpenAIError('Provider echoed sensitive-key')
    client = SimpleNamespace(api_key='sensitive-key', chat=SimpleNamespace(completions=SimpleNamespace(create=fail)))
    profile = DomainVerificationProfile(provider='test', model='test', base_url='https://example.test/v1', system_prompt='Verify', prompt_version='test-v1')
    row = verify_domain(client, company_id='company', domain='example.test', payload='{}', profile=profile, hashes={}, run_id='run')
    assert 'sensitive-key' not in row['error']
    outcome = esef._request_prepared_enrichment(SimpleNamespace(evidence_input=None, request_payload={}), client=client, request=fail)
    assert 'sensitive-key' not in outcome.failure_message
    assert outcome.failure_kind == 'http_error'


def test_esef_reprocessing_does_not_construct_a_client(monkeypatch):
    captured = {}
    def no_client(*args, **kwargs):
        pytest.fail('Saved response reprocessing must not resolve credentials')
    def run(**kwargs):
        captured.update(kwargs)
        return {}
    monkeypatch.setattr(esef, 'build_esef_llm_client', no_client)
    monkeypatch.setattr(esef, 'run_esef_llm_enrichment', run)
    config = esef.EsefLlmEnrichmentConfig(provider='test', model='test', reprocess_existing_without_model=True)
    esef.esef_document_company_information_clickhouse.op.compute_fn.decorated_fn(
        SimpleNamespace(run_id='run', log=SimpleNamespace(info=lambda *args: None)), config, None, None,
    )
    assert captured['client'] is None
    assert captured['reprocess_existing_without_model'] is True


@pytest.mark.parametrize('base,effort,fields', [
    ('https://api.deepseek.com', 'none', {'extra_body': {'thinking': {'type': 'disabled'}}}),
    ('https://api.deepseek.com', 'max', {'extra_body': {'thinking': {'type': 'enabled'}, 'reasoning_effort': 'max'}}),
    ('https://openrouter.ai/api/v1', 'high', {'extra_body': {'reasoning': {'enabled': True, 'effort': 'high'}, 'provider': {'require_parameters': True}}}),
    ('https://openrouter.ai/api/v1', 'none', {'extra_body': {'reasoning': {'enabled': False}, 'provider': {'require_parameters': True}}}),
])
def test_reasoning_reaches_direct_requests_and_changes_cache_identity(base, effort, fields):
    from dagster_v3.defs.se_company.domain.verification import fingerprints
    from dagster_v3.defs.common.domain_relationships import model_settings
    from dagster_v3.defs.se_company.basic_info.llm import CompanyContext, build_suggestion_request
    from dagster_v3.defs.common.encrypted_llm import EncryptedLLMConfig
    seen = {}
    def call(**kwargs):
        seen.update(kwargs)
        raise OpenAIError('fixture')
    client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=call)))
    profile = DomainVerificationProfile(provider='test', model='test', base_url=base,
        profile_id='saved-profile', reasoning_effort=effort, system_prompt='Verify', prompt_version='test-v1')
    verify_domain(client, company_id='company', domain='example.test', payload='{}', profile=profile, hashes={}, run_id='run')
    for key, value in fields.items():
        assert seen[key] == value
    default = profile.model_copy(update={'reasoning_effort': None})
    assert fingerprints('{}', profile)['model_hash'] != fingerprints('{}', default)['model_hash']
    assert model_settings(profile) != model_settings(default)
    context = CompanyContext(company_id='company', legal_name='Example', sni_code=None, texts=())
    request = build_suggestion_request(context, LlmSuggestionProfile(provider="test", model="test", base_url=base, reasoning_effort=effort))
    for key, value in fields.items():
        assert request[key] == value
    assert EncryptedLLMConfig(**(FIXTURE['llm'] | {'reasoning_effort': effort})).model_dump()['reasoning_effort'] == effort


def test_esef_forwards_reasoning_options_to_saved_request_pipeline(monkeypatch):
    captured = {}
    monkeypatch.setattr(esef, 'build_esef_llm_client', lambda config: None)
    monkeypatch.setattr(esef, 'run_esef_llm_enrichment', lambda **kwargs: captured.update(kwargs) or {})
    config = esef.EsefLlmEnrichmentConfig(**(FIXTURE['llm'] | {'reasoning_effort': 'high'}))
    esef.esef_document_company_information_clickhouse.op.compute_fn.decorated_fn(
        SimpleNamespace(run_id='run', log=SimpleNamespace(info=lambda *args: None)), config, None, None,
    )
    from dagster_v3.defs.common.llm_reasoning import reasoning_options
    assert captured['model_request_options'] == reasoning_options(config.base_url, 'high')
