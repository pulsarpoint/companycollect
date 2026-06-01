# BRREG Financial Service Design

Date: 2026-05-31

Status: Proposed for review.

## Summary

Create a focused BRREG financial service that fetches structured financial key figures from the official BRREG Regnskapsregister API, normalizes them into Corpscout-friendly fields, and returns enough evidence for replay and review.

The service exists to support Corpscout company segmentation, especially filtering companies by revenue and size when analyzing technology usage. It is not intended to model complete annual accounts.

```text
brreg_workflow.raw_records
  -> ConvertBrregFinancials Temporal activity
  -> brreg-financial-service
  -> currency-service
  -> brreg_workflow.financial_results
  -> brreg_source_financials
  -> Corpscout filtering and suggestions
```

## Why This Service

The current `ConvertBrregFinancials` workflow path only derives registered capital from the raw Enhetsregister company payload. That is not enough for the product goal: filtering and segmenting companies by revenue and rough financial size.

BRREG's open Regnskapsregister API provides structured annual-account key figures for many companies. It is a better source than PDFs for MVP because it is already JSON, fast to parse, and sufficient for revenue-based filtering.

The service should be deterministic source-specific parsing code. It should not use LLMs, browser crawling, or PDF extraction.

## Official API Inputs

Use the official BRREG Regnskapsregister endpoints:

```text
GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgNummer}
GET https://data.brreg.no/regnskapsregisteret/regnskap/{orgNummer}/{id}
GET https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/{orgnr}/aar
```

The first endpoint returns structured key figures. In observed samples it returns one latest submitted annual account record for regular companies. The second endpoint returns the same record by BRREG accounting record id and is useful as an evidence URL. The third endpoint returns the years for which annual-account PDFs are available, without downloading the PDFs.

The service should not download PDFs in v1. PDF downloads can be large and are not required for typed revenue filtering.

## Sample Findings

The API was sampled on 2026-05-31 against several organization numbers:

| Organization | Example | Result |
|---|---|---|
| `923609016` | EQUINOR ASA | JSON key figures available, USD, 2024 |
| `989795848` | AKER BP ASA | JSON key figures available, USD, 2024 |
| `917082308` | BANE NOR SF | JSON key figures available, NOK, 2024 |
| `915501680` | NEL ASA | JSON key figures available, NOK, 2024 |
| `810202572` | BORTIGARD AS | JSON key figures available, NOK, small-company flag |
| `984851006` | DNB BANK ASA | PDF years available, JSON endpoint returns unsupported `BANK` statement plan |
| `974760673` | REGISTERENHETEN I BRONNOYSUND | No annual-account key figures available |

Fields observed across supported companies:

- `id`
- `journalnr`
- `regnskapstype`
- `virksomhet.organisasjonsnummer`
- `virksomhet.organisasjonsform`
- `virksomhet.morselskap`
- `regnskapsperiode.fraDato`
- `regnskapsperiode.tilDato`
- `valuta`
- `oppstillingsplan`
- `revisjon.ikkeRevidertAarsregnskap`
- `revisjon.fravalgRevisjon`
- `regnkapsprinsipper.smaaForetak`
- `regnkapsprinsipper.regnskapsregler`
- `resultatregnskapResultat.*`
- `eiendeler.*`
- `egenkapitalGjeld.*`

Unsupported or missing data must be first-class outcomes, not exceptional crashes.

## Goals

- Fetch BRREG financial key figures by organization number.
- Normalize enough financial data for Corpscout revenue and size filtering.
- Preserve raw payload evidence and official source URLs.
- Return original source currency amounts only.
- Leave currency conversion to `currency-service`.
- Batch records so Temporal activities can process many BRREG rows efficiently.
- Provide structured per-record statuses for partial batches.

## Non-Goals

- Do not download or parse annual-account PDFs in v1.
- Do not call currency conversion from this service.
- Do not write to Corpscout Postgres from this service.
- Do not create company suggestions directly.
- Do not infer missing fields.
- Do not support full paid XML/SFTP annual-account subscriptions in v1.
- Do not build a generic cross-country financial statement parser.

## Runtime Shape

Implement as a standalone Go HTTP binary:

```text
data-pipelines/services/brreg-financial-service/
  cmd/brreg-financial-service/main.go
  internal/httpapi/
  internal/service/
  internal/brregclient/
  internal/parser/
  internal/models/
```

Go is preferred because the Corpscout scheduler and Temporal worker are Go, this service is deterministic parsing logic, and strong typed tests are useful for money and source-schema handling.

## API

### Health

```http
GET /healthz
```

Response:

```json
{"status":"ok"}
```

### BRREG Financial Lookup

```http
POST /v1/brreg/financials/lookup
```

Request:

```json
{
  "records": [
    {
      "record_id": "workflow-raw-record-id",
      "organization_number": "923609016",
      "organization_name": "EQUINOR ASA",
      "last_annual_report_year": 2024
    }
  ],
  "include_pdf_metadata": true,
  "include_raw_payload": true
}
```

Rules:

- `records` must contain 1 to 1000 records.
- `record_id` is caller-defined and echoed back.
- `organization_number` is required and must be normalized to Norwegian organization-number digits.
- `organization_name` is optional context for diagnostics only.
- `last_annual_report_year` is optional context from Enhetsregisteret and should not be used as a hard filter in v1.
- `include_pdf_metadata` defaults to `true`.
- `include_raw_payload` defaults to `true` for internal Corpscout usage.

Response:

```json
{
  "schema_version": "brreg-financial-service.lookup.v1",
  "status": "succeeded",
  "records_seen": 1,
  "records_completed": 1,
  "records_failed": 0,
  "duration_ms": 145,
  "results": [
    {
      "record_id": "workflow-raw-record-id",
      "organization_number": "923609016",
      "status": "succeeded",
      "statements": [
        {
          "source_record_id": "5667197",
          "journal_number": "2025428073",
          "fiscal_year": 2024,
          "period_start": "2024-01-01",
          "period_end": "2024-12-31",
          "statement_type": "company",
          "original_currency": "USD",
          "revenue_original_amount": "72543000000.00",
          "sales_revenue_original_amount": null,
          "operating_profit_original_amount": "10347000000.00",
          "profit_before_tax_original_amount": "8168000000.00",
          "tax_expense_original_amount": null,
          "net_income_original_amount": "8141000000.00",
          "total_result_original_amount": null,
          "total_assets_original_amount": "109150000000.00",
          "current_assets_original_amount": "45079000000.00",
          "fixed_assets_original_amount": "64071000000.00",
          "total_equity_original_amount": "41090000000.00",
          "total_liabilities_original_amount": "68060000000.00",
          "short_term_liabilities_original_amount": "42024000000.00",
          "long_term_liabilities_original_amount": "26036000000.00",
          "facts": {
            "finance_result_original_amount": "-2179000000.00",
            "financial_income_original_amount": "516000000.00",
            "financial_cost_original_amount": "2695000000.00"
          },
          "metadata": {
            "organization_form": "ASA",
            "is_parent_company": true,
            "statement_plan": "store",
            "accounting_rules": "forenkletAnvendelseIFRS",
            "small_company": false,
            "not_audited": false,
            "audit_opt_out": false,
            "liquidation_accounts": false
          },
          "evidence": {
            "source": "brreg_regnskapsregisteret",
            "source_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016",
            "detail_url": "https://data.brreg.no/regnskapsregisteret/regnskap/923609016/5667197",
            "raw_payload_hash": "sha256:..."
          },
          "raw_payload": {}
        }
      ],
      "pdf_metadata": {
        "available_years": ["2011", "2012", "2024"],
        "download_url_template": "https://data.brreg.no/regnskapsregisteret/regnskap/aarsregnskap/kopi/923609016/{year}"
      },
      "warnings": []
    }
  ]
}
```

Batch status:

- `succeeded` when every record succeeded or returned a clean `not_available`.
- `partial` when some records succeeded and some failed.
- `failed` when every record failed due to retryable provider/service failures.

Record statuses:

- `succeeded`
- `not_available`
- `unsupported_statement_plan`
- `failed`

## Normalized Field Mapping

Typed financial columns should be the fields Corpscout needs to filter, sort, or display.

| Output field | BRREG path |
|---|---|
| `source_record_id` | `id` |
| `journal_number` | `journalnr` |
| `statement_type` | `regnskapstype` mapped to `company`, `group`, or source value |
| `fiscal_year` | year from `regnskapsperiode.tilDato` |
| `period_start` | `regnskapsperiode.fraDato` |
| `period_end` | `regnskapsperiode.tilDato` |
| `original_currency` | `valuta` |
| `revenue_original_amount` | `resultatregnskapResultat.driftsresultat.driftsinntekter.sumDriftsinntekter` |
| `sales_revenue_original_amount` | `resultatregnskapResultat.driftsresultat.driftsinntekter.salgsinntekter` |
| `operating_profit_original_amount` | `resultatregnskapResultat.driftsresultat.driftsresultat` |
| `profit_before_tax_original_amount` | `resultatregnskapResultat.ordinaertResultatFoerSkattekostnad` |
| `tax_expense_original_amount` | `resultatregnskapResultat.ordinaertResultatSkattekostnad` |
| `net_income_original_amount` | `resultatregnskapResultat.aarsresultat` |
| `total_result_original_amount` | `resultatregnskapResultat.totalresultat` |
| `total_assets_original_amount` | `eiendeler.sumEiendeler` |
| `current_assets_original_amount` | `eiendeler.omloepsmidler.sumOmloepsmidler` |
| `fixed_assets_original_amount` | `eiendeler.anleggsmidler.sumAnleggsmidler` |
| `total_equity_original_amount` | `egenkapitalGjeld.egenkapital.sumEgenkapital` |
| `total_liabilities_original_amount` | `egenkapitalGjeld.gjeldOversikt.sumGjeld` |
| `short_term_liabilities_original_amount` | `egenkapitalGjeld.gjeldOversikt.kortsiktigGjeld.sumKortsiktigGjeld` |
| `long_term_liabilities_original_amount` | `egenkapitalGjeld.gjeldOversikt.langsiktigGjeld.sumLangsiktigGjeld` |

Additional useful fields should go into `facts` until Corpscout has a clear filter/sort requirement for them.

## Amount Handling

The BRREG JSON decoder must preserve numbers as decimal strings or exact decimal values. Do not parse BRREG financial amounts through `float64`.

The service returns original-currency decimal strings. It does not return cents, USD, or converted values.

Currency conversion happens later through `currency-service` using `period_end` as the recommended requested FX date.

## Error And Edge Case Handling

### No Data

If `GET /regnskapsregisteret/regnskap/{orgNummer}` returns `404`, return:

```json
{
  "status": "not_available",
  "statements": [],
  "warnings": [
    {
      "code": "financials_not_found",
      "message": "No BRREG annual-account key figures are available"
    }
  ]
}
```

This is not a retryable failure.

### Unsupported Statement Plan

If BRREG returns an error such as unsupported `BANK` statement plan, return:

```json
{
  "status": "unsupported_statement_plan",
  "statements": [],
  "warnings": [
    {
      "code": "unsupported_statement_plan",
      "message": "BRREG key-figure API does not support this statement plan",
      "detail": {
        "statement_plan": "BANK"
      }
    }
  ]
}
```

This is not retryable for v1. PDF metadata may still be returned when available.

### Provider Failure

Network errors, timeouts, HTTP 429, and HTTP 5xx without known unsupported-plan semantics should return per-record `failed` with retry metadata:

```json
{
  "code": "provider_unavailable",
  "category": "external_service",
  "retry_strategy": "retry_with_backoff"
}
```

### Partial Statement Data

Missing individual fields should be `null`, not a failed record. A record with revenue missing but assets present can still be useful and should be returned as `succeeded` with a warning when appropriate.

## Corpscout Integration

Add scheduler config:

```text
CORPSCOUT_BRREG_FINANCIAL_SERVICE_URL=http://brreg-financial-service:8098
```

Add a Go HTTP client:

```text
corpscout/scheduler/internal/brregfinancialclient
```

The existing `ConvertBrregFinancials` Temporal activity should change from raw capital conversion to:

1. Claim BRREG workflow rows.
2. Build a lookup request from raw record id, organization number, organization name, and optional `sisteInnsendteAarsregnskap`.
3. Call `brreg-financial-service`.
4. For each returned statement, build currency conversion items for typed monetary fields.
5. Call `currency-service`.
6. Store original payload, USD payload, FX metadata, source URI, and safe status in `brreg_workflow.financial_results`.

For MVP, `brreg_workflow.financial_results.original_payload` should contain:

```json
{
  "schema_version": "brreg-financial-service.lookup.v1",
  "statements": [],
  "pdf_metadata": {},
  "warnings": []
}
```

`usd_payload` should contain the converted fields keyed by the same statement identifiers.

The enhanced-record builder and source-table unpacker should map the latest usable statement into `brreg_source_financials`, especially `revenue_original_amount` and `revenue_usd_cents`.

## Product Use

Corpscout should use this data for broad company-size filtering:

- revenue thresholds;
- rough enterprise size;
- technology usage by revenue band;
- prioritization of companies for outreach or analysis.

The UI should not present these values as audited financial analysis beyond what the official BRREG key-figure API provides. Evidence links should point back to the official BRREG JSON/detail URL and optional PDF availability.

## Deployment

Add service to `data-pipelines/services/docker-compose.yml`:

```text
brreg-financial-service:8098
```

Image:

```text
ghcr.io/pulsarpoint/corpscout-brreg-financial-service:${SERVICES_IMAGE_TAG:-latest}
```

Environment:

- `BRREG_FINANCIAL_SERVICE_LISTEN_ADDR`, default `:8098`;
- `BRREG_FINANCIAL_SERVICE_BASE_URL`, default `https://data.brreg.no`;
- `BRREG_FINANCIAL_SERVICE_REQUEST_TIMEOUT`, default `30s`;
- `BRREG_FINANCIAL_SERVICE_MAX_BATCH_SIZE`, default `1000`;
- `BRREG_FINANCIAL_SERVICE_INCLUDE_RAW_PAYLOAD_DEFAULT`, default `true`.

## Logging And Error Handling

Lower layers wrap and return errors with context. HTTP boundary logs unexpected request-level failures once with `slog`.

Per-record `not_available` and `unsupported_statement_plan` are normal outcomes and should not be error-level logs.

Do not log full BRREG raw payloads or full batch request bodies. Log counts, organization number, status, duration, and safe error codes.

## Testing

Unit tests:

- BRREG JSON parser maps supported sample records into typed output.
- Decimal amounts preserve exact string values.
- Fiscal year derives from `regnskapsperiode.tilDato`.
- Missing financial fields become `null`, not errors.
- Unsupported `BANK` response maps to `unsupported_statement_plan`.
- `404` maps to `not_available`.
- PDF year list maps to `pdf_metadata.available_years`.

HTTP tests:

- `GET /healthz`.
- Single successful lookup.
- Mixed batch with success, no-data, unsupported-plan, and provider failure.
- Max batch size validation.
- Invalid organization number validation.

Provider tests:

- BRREG API 200 list response.
- BRREG API 200 detail response evidence URL.
- BRREG API 404 response.
- BRREG API 429/5xx retryable failures.
- Timeout handling.

Integration tests can use local HTTP fixtures rather than the live BRREG API.

## Implementation Boundaries

This spec only covers the BRREG financial lookup service. A later implementation plan should separately cover:

1. Creating the service binary and tests.
2. Adding it to `data-pipelines/services/docker-compose.yml`.
3. Adding scheduler client/config.
4. Reworking `ConvertBrregFinancials` to call the service and `currency-service`.
5. Updating enhanced-record/unpacker logic if needed for `brreg_source_financials`.
