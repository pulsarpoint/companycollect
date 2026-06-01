# Corpscout Translation Service

Standalone OpenAI-compatible translation API for Corpscout enrichment pipelines.

## Run

```bash
make sync
make run
```

NATS worker mode:

```bash
NATS_URL=nats://localhost:4222 \
TRANSLATION_NATS_SUBJECT=brreg.translation.translate \
TRANSLATION_NATS_QUEUE=brreg-translation \
corpscout-translation-service worker
```

Health check:

```bash
curl http://localhost:8095/healthz
```

BRREG translation:

```bash
curl -X POST 'http://localhost:8095/v1/translate/brreg-records?provider=default&model=qwen3:6b' \
  -H 'content-type: application/json' \
  -d '{"records":[{"record_id":"record-1","organization_number":"810202572","raw_payload":{"organisasjonsnummer":"810202572","navn":"BORTIGARD AS","organisasjonsform":{"kode":"AS","beskrivelse":"Aksjeselskap"}}}]}'
```

Term-batch translation:

```bash
curl -X POST 'http://localhost:8095/v1/translate/terms?provider=default&model=qwen3:6b' \
  -H 'content-type: application/json' \
  -d '{"provider":"default","model":"default","prompt_version":"v1","source_lang":"no","target_lang":"en","items":[{"id":"org_form:example","category":"org_form","text":"Aksjeselskap"}]}'
```

## LLM Configuration

The service supports multiple OpenAI-compatible providers selected per request with query parameters or the JSON `llm` object:

- `provider`
- `model`
- `prompt_version`

BRREG requests may also send:

```json
{
  "llm": {
    "provider": "deepseek-v4-flash",
    "model": "deepseek-v4-flash",
    "base_url": "https://api.deepseek.com",
    "api_key": "..."
  }
}
```

When `llm.provider=default`, the service uses its env default. When
`llm.base_url` or `llm.api_key` is present, those request values override the
provider env values for that one request.

Provider env vars:

```bash
TRANSLATION_DEFAULT_PROVIDER=local
TRANSLATION_DEFAULT_MODEL=qwen3:6b
TRANSLATION_PROVIDER_LOCAL_BASE_URL=http://100.77.62.33:8888
TRANSLATION_PROVIDER_LOCAL_MODEL=qwen3:6b
# Local LLM does not require a password/API key.
# TRANSLATION_PROVIDER_LOCAL_API_KEY=
TRANSLATION_PROVIDER_DEEPSEEK_V4_FLASH_BASE_URL=https://api.deepseek.com
TRANSLATION_PROVIDER_DEEPSEEK_V4_FLASH_MODEL=deepseek-v4-flash
TRANSLATION_PROVIDER_DEEPSEEK_V4_FLASH_API_KEY=...
```

Never pass API keys in query parameters.

Both translation endpoints accept `max_retries`; the default is `3`. Retries
only resend missing term IDs, so already translated terms are kept.

## Tests

Normal test suite uses fake LLMs:

```bash
make test
```

Direct service smoke tests:

```bash
LLM_API_KEY=... uv run python scripts/smoke_translation_http.py \
  --url http://companycollect:8095 \
  --provider deepseek-v4-flash \
  --model deepseek-v4-flash \
  --base-url https://api.deepseek.com

LLM_API_KEY=... uv run python scripts/smoke_translation_nats.py \
  --nats-url nats://companycollect:4222 \
  --provider deepseek-v4-flash \
  --model deepseek-v4-flash \
  --base-url https://api.deepseek.com
```

The repository includes `tests/data/brreg_raw_records_300.json`, exported from `brreg_workflow.raw_records`, so the real LLM test uses actual BRREG payloads rather than synthetic records.

Refresh the fixture from a database:

```bash
CORPSCOUT_DATABASE_URL=postgresql://... \
uv run scripts/export_brreg_raw_records_fixture.py --limit 300
```

Opt-in real LLM stress test against the local LLM:

```bash
TRANSLATION_SERVICE_RUN_REAL_LLM_TESTS=1 \
TRANSLATION_SERVICE_TEST_PROVIDER=default \
TRANSLATION_SERVICE_TEST_MODEL=qwen3:6b \
TRANSLATION_LLM_BASE_URL=http://100.77.62.33:8888 \
make test-real
```
