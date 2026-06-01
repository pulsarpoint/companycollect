# Currency Conversion Service Design

Date: 2026-05-31

Status: Proposed for review.

## Summary

Create a standalone Go HTTP service for deterministic currency conversion used by Corpscout enrichment workflows.

The service converts source amounts from one ISO 4217 currency to another for a requested date, using an official rate provider and an in-memory cache. It returns converted decimal amounts, target minor units, the effective rate date, and metadata that downstream systems can store with financial evidence.

Initial consumers are BRREG financial enrichment tasks. The service should also be generic enough for future source pipelines that need date-aware currency conversion.

```text
BRREG financial fetch
  -> scheduler Temporal activity
  -> currency-service
  -> brreg_workflow.financial_results
  -> brreg_source_financials
```

## Why A Go Service

Currency conversion is deterministic infrastructure code, not LLM or browser work. A Go binary is a better fit than Python/FastAPI for this service because:

- existing FX logic already lives in Go in Corpscout and data-pipelines;
- scheduler and Temporal activities are Go, so typed clients and shared tests are straightforward;
- deployment is a small static HTTP binary with no Python runtime;
- conversion logic should be strict about decimal parsing, rounding, and error behavior;
- the service can follow the project Go conventions for `slog` logging and `github.com/cockroachdb/errors` wrapping.

The service should live separately from the Temporal Go worker because it is an HTTP service, not a worker process:

```text
data-pipelines/services/currency-service/
  cmd/currency-service/main.go
  internal/httpapi/
  internal/service/
  internal/rates/
  internal/cache/
  internal/conversion/
```

## Goals

- Convert source amounts to a requested target currency for a requested date.
- Support batch conversion because a financial statement has multiple fields.
- Cache fetched rate sheets by provider and effective date in memory.
- Return stable metadata suitable for audit evidence.
- Use decimal-safe money handling; do not use `float64` for amounts or conversion results.
- Keep the service stateless from Corpscout's perspective.
- Make persistent caching possible later without changing the API contract.

## Non-Goals

- Do not fetch BRREG financial statements in this service.
- Do not write to Corpscout Postgres from this service.
- Do not decide which financial facts are product-worthy.
- Do not implement paid FX providers in v1.
- Do not add persistent Redis/Postgres cache in v1.
- Do not replace the translation or crawl services.

## API

### Health

```http
GET /healthz
```

Response:

```json
{"status":"ok"}
```

### Batch Conversion

```http
POST /v1/convert
```

Request:

```json
{
  "provider": "ecb",
  "date_policy": "latest_on_or_before",
  "items": [
    {
      "id": "brreg-923609016-2024-revenue",
      "amount": "72543000000.00",
      "source_currency": "USD",
      "target_currency": "USD",
      "date": "2024-12-31"
    },
    {
      "id": "brreg-810202572-2024-revenue",
      "amount": "11825000.00",
      "source_currency": "NOK",
      "target_currency": "USD",
      "date": "2024-12-31"
    }
  ]
}
```

Rules:

- `provider` defaults to `ecb`.
- `date_policy` defaults to `latest_on_or_before`.
- `items` must contain 1 to 1000 items.
- `id` is caller-defined and echoed back.
- `amount` is a decimal string.
- currencies are ISO 4217 uppercase after normalization.
- `date` is `YYYY-MM-DD`.

Response:

```json
{
  "schema_version": "currency-service.convert.v1",
  "provider": "ecb",
  "date_policy": "latest_on_or_before",
  "items_seen": 2,
  "items_completed": 2,
  "items_failed": 0,
  "results": [
    {
      "id": "brreg-923609016-2024-revenue",
      "status": "succeeded",
      "amount": "72543000000.00",
      "source_currency": "USD",
      "target_currency": "USD",
      "requested_date": "2024-12-31",
      "rate_date": "2024-12-31",
      "rate": "1",
      "converted_amount": "72543000000.00",
      "converted_minor_units": 7254300000000,
      "target_minor_unit": 2,
      "metadata": {
        "provider": "ecb",
        "identity_conversion": true
      }
    },
    {
      "id": "brreg-810202572-2024-revenue",
      "status": "succeeded",
      "amount": "11825000.00",
      "source_currency": "NOK",
      "target_currency": "USD",
      "requested_date": "2024-12-31",
      "rate_date": "2024-12-31",
      "rate": "0.08842",
      "converted_amount": "1045678.50",
      "converted_minor_units": 104567850,
      "target_minor_unit": 2,
      "metadata": {
        "provider": "ecb",
        "base_currency": "EUR"
      }
    }
  ],
  "duration_ms": 42
}
```

Each item succeeds or fails independently. A bad currency or invalid amount should not fail the whole batch.

Failed item example:

```json
{
  "id": "bad-row",
  "status": "failed",
  "amount": "12.00",
  "source_currency": "XYZ",
  "target_currency": "USD",
  "requested_date": "2024-12-31",
  "error": {
    "code": "unsupported_currency",
    "message": "currency XYZ is not available from provider ecb",
    "category": "validation",
    "retry_strategy": "do_not_retry"
  }
}
```

### Rate Lookup

```http
GET /v1/rates?source=NOK&target=USD&date=2024-12-31&provider=ecb&date_policy=latest_on_or_before
```

Response:

```json
{
  "schema_version": "currency-service.rates.v1",
  "provider": "ecb",
  "date_policy": "latest_on_or_before",
  "source_currency": "NOK",
  "target_currency": "USD",
  "requested_date": "2024-12-31",
  "rate_date": "2024-12-31",
  "rate": "0.08842",
  "base_currency": "EUR",
  "cache": {
    "hit": true,
    "key": "ecb:2024-12-31:latest_on_or_before"
  }
}
```

This endpoint is for diagnostics, tests, and callers that need rate metadata without conversion.

## Provider Behavior

V1 provider is ECB.

ECB publishes rates as currency units per EUR. Conversion between two non-EUR currencies should be derived through EUR:

```text
source -> EUR -> target
```

For `latest_on_or_before`, if the requested date is a weekend, holiday, or otherwise missing from the provider feed, the service selects the most recent available provider rate on or before the requested date. The response must always expose both `requested_date` and `rate_date`.

If there is no rate on or before the requested date, the item fails with `rate_not_found`.

## Cache Design

V1 uses process-local in-memory cache.

Cache key:

```text
provider + requested_date + date_policy
```

Cache value:

```text
effective rate_date
base currency
map[currency]rate
fetched_at
source_url
```

The cache should store full rate sheets, not individual pairs. Many batch items for the same date can then reuse one provider fetch.

Cache policy:

- historical dates may be cached indefinitely for the process lifetime;
- today's date should have a configurable TTL because the daily feed can update;
- cache misses are synchronized per key so concurrent requests do not stampede the provider;
- a future persistent cache can implement the same interface.

## Conversion Rules

Amounts are parsed from decimal strings into exact decimal values.

Output:

- `converted_amount` is a decimal string rounded to the target currency minor unit;
- `converted_minor_units` is the integer minor-unit value for storage and sorting;
- `target_minor_unit` is usually 2, but must come from a currency metadata table;
- unknown currencies fail before provider lookup.

Rounding:

- use half-up rounding to the target minor unit for financial display/storage;
- preserve the original source amount in caller-owned storage;
- do not return binary floating-point numbers in API responses.

Identity conversion:

- if source and target currencies are equal, return rate `1`;
- no provider fetch is required;
- still return `requested_date` as `rate_date`.

## Corpscout Integration

Add scheduler config:

```text
CORPSCOUT_CURRENCY_SERVICE_URL=http://currency-service:8097
```

Add a Go HTTP client in scheduler:

```text
scheduler/internal/currencyclient
```

`ConvertBrregFinancials` should:

1. Fetch BRREG annual-account facts through the BRREG financial lookup path.
2. Build one conversion item per typed monetary field that exists.
3. Call `currency-service` with target `USD`.
4. Store original amounts and original currency exactly as received.
5. Store `*_usd_cents` from `converted_minor_units`.
6. Store `fx_metadata` containing provider, requested date, effective rate date, source currency, target currency, and rate.

If currency conversion fails for a statement because of provider or network errors, the financial task should be retryable. If it fails because of an unsupported source currency or invalid amount, the task should record a terminal failure or partial result with a safe error summary.

## Deployment

Add service to `data-pipelines/services/docker-compose.yml`:

```text
currency-service:8097
```

Add image:

```text
ghcr.io/pulsarpoint/corpscout-currency-service:${SERVICES_IMAGE_TAG:-latest}
```

The service should expose:

- `CURRENCY_SERVICE_LISTEN_ADDR`, default `:8097`;
- `CURRENCY_SERVICE_PROVIDER`, default `ecb`;
- `CURRENCY_SERVICE_TODAY_TTL`, default `6h`;
- `CURRENCY_SERVICE_REQUEST_TIMEOUT`, default `30s`;
- `CURRENCY_SERVICE_MAX_BATCH_SIZE`, default `1000`.

## Error Handling And Logging

Repository/external provider layer wraps and returns errors.

HTTP boundary logs unexpected request-level failures once with `slog`. Per-item validation failures are returned in the response and do not need error-level logs.

Do not log request bodies wholesale. Amounts and currencies are not secrets, but full request payload logging can become noisy and may later include caller metadata.

Provider failures should return safe messages:

- `provider_unavailable`
- `rate_not_found`
- `unsupported_currency`
- `invalid_amount`
- `invalid_date`
- `batch_too_large`

## Testing

Unit tests:

- decimal parsing and minor-unit conversion;
- identity conversion;
- NOK to USD via EUR;
- USD to NOK via EUR;
- weekend/date fallback to latest rate on or before requested date;
- unsupported currency;
- invalid amount;
- per-item partial failure behavior;
- cache hit/miss behavior.

HTTP tests:

- `GET /healthz`;
- valid batch conversion;
- mixed success/failure batch;
- `GET /v1/rates`;
- max batch size validation.

Provider tests:

- ECB daily XML parsing;
- ECB historical XML date selection;
- provider HTTP 5xx handling;
- provider timeout handling.

## Open Follow-Ups

- Decide whether the first BRREG financial implementation should use fiscal period end date or workflow-supplied FX date as `requested_date`. Recommendation: use `period_end` for annual accounts, and allow explicit override only for reprocessing.
- Decide whether to keep a tiny shared Go package for currency metadata if scheduler also needs minor-unit definitions. Recommendation: keep the metadata in the service first and only duplicate the response structs in scheduler.
- Decide when persistent cache is worth adding. Recommendation: defer until provider latency or restart churn becomes visible.
