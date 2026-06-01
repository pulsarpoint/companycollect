# BRREG Financial Service — Implementation Design

Date: 2026-05-31

Status: Approved for implementation planning.

## Scope

This document covers Phase 1 only: the standalone `brreg-financial-service` Go HTTP binary.
Scheduler integration (brregfinancialclient, activity rewrite, enhanced-record builder) is a
separate future plan.

Requirements spec: `2026-05-31-brreg-financial-service-design.md`

## Module and Directory

```
data-pipelines/services/brreg-financial-service/
  cmd/brreg-financial-service/main.go
  internal/models/       # request/response types shared across layers
  internal/parser/       # pure BRREG JSON → Statement (no I/O)
  internal/brregclient/  # HTTP calls to data.brreg.no
  internal/service/      # orchestrates brregclient + parser, decides per-record status
  internal/httpapi/      # stdlib mux handler
  go.mod                 # module github.com/pulsarpoint/brreg-financial-service
  Makefile               # GOWORK=off build/test/run
  Dockerfile
```

Structural model: `data-pipelines/services/currency-service` (same layout, same patterns).

## Technical Decisions

### Amount Handling

BRREG amounts arrive as JSON float literals (e.g., `109150000000.0`). All financial amounts
must be decoded with `json.Decoder.UseNumber()` and stored as `shopspring/decimal` values,
then serialised as decimal strings in the response. Float64 must never touch financial figures.

### Unsupported Plan Detection

The BRREG API returns HTTP 500 with a JSON body when the statement plan is not supported:

```json
{
  "status": "500",
  "message": "Regnskapet inneholder en oppstillingsplan som ikke er stottet (BANK)"
}
```

The parser should extract the plan name from the message using a simple string match on the
parenthesised suffix. Any 500 response with this message pattern is `unsupported_statement_plan`.
Any other 500 is `failed` (retryable).

### Organization Number Validation

Strip all non-digit characters, then validate exactly 9 digits. Invalid inputs return HTTP 400
before any BRREG call is made.

### Not-Available Detection

`GET /regnskap/{orgNummer}` returns `HTTP 404` with an empty body for companies with no data.
There is no "empty array" case — the API always uses 404 for missing records. Both `404`
and a response with an empty JSON array `[]` should map to `not_available` for robustness.

### raw_payload_hash

Compute SHA-256 of the raw BRREG JSON response bytes and format as `"sha256:<hex>"`.
Use Go's `crypto/sha256` package. Hash is computed before any JSON parsing so it reflects
the exact bytes received.

### facts Field Mapping

The following BRREG fields go into `facts` (not primary columns) per the spec:

| facts key | BRREG path |
|---|---|
| `finance_result_original_amount` | `resultatregnskapResultat.finansresultat.nettoFinans` |
| `financial_income_original_amount` | `resultatregnskapResultat.finansresultat.finansinntekt.sumFinansinntekter` |
| `financial_cost_original_amount` | `resultatregnskapResultat.finansresultat.finanskostnad.sumFinanskostnad` |

All three are optional. Missing values are omitted from `facts` (not set to null).

### List vs Detail Endpoint Shape

The list endpoint (`GET /regnskap/{orgNummer}`) returns a JSON array `[{...}]`.
The detail endpoint (`GET /regnskap/{orgNummer}/{id}`) returns a single object `{...}`.
The parser handles both shapes. The detail URL is used for `evidence.detail_url` only; the
service fetches the list endpoint for key figures.

### PDF Years

The years endpoint (`GET /regnskap/aarsregnskap/kopi/{orgnr}/aar`) returns a plain JSON
array of year strings: `["2011", "2012", ...]`. PDF metadata is fetched for all records
when `include_pdf_metadata` is true, independently of whether the key-figure fetch succeeded.
A DNB-style unsupported-plan record still gets `pdf_metadata` if years are available.

### regnskapstype Mapping

| BRREG value | Output statement_type |
|---|---|
| `SELSKAP` | `company` |
| `KONSERN` | `group` |
| anything else | source value (pass-through) |

## Real API Fixture Inventory

These fixtures were fetched from the live BRREG API on 2026-05-31 and must be saved as
static JSON files under `internal/parser/testdata/` and `internal/brregclient/testdata/`.

| Fixture file | Org number | Company | Coverage |
|---|---|---|---|
| `equinor_list.json` | 923609016 | EQUINOR ASA | USD, large, IFRS simplified, morselskap=true, no salgsinntekter, no totalresultat |
| `akerbp_list.json` | 989795848 | AKER BP ASA | USD, large, full IFRS, has `totalresultat`, negative opptjentEgenkapital |
| `banenor_list.json` | 917082308 | BANE NOR SF | NOK, large, standard rules, SF org form, operating loss |
| `bortigard_list.json` | 810202572 | BORTIGARD AS | NOK, small company (smaaForetak=true), AS org form, small amounts |
| `nel_list.json` | 915501680 | NEL ASA | NOK, tech company, negative aarsresultat, negative ordinaertResultatFoerSkattekostnad |
| `mowi_list.json` | 964118191 | MOWI ASA | EUR currency (not NOK/USD), medium-large, standard rules |
| `dnb_500.json` | 984851006 | DNB BANK ASA | HTTP 500, unsupported plan BANK |
| `storebrand_500.json` | 930553506 | STOREBRAND ASA | HTTP 500, unsupported plan SKADE |
| `equinor_detail.json` | 923609016 / id 5667197 | EQUINOR ASA | Single-object detail endpoint shape |
| `equinor_pdf_years.json` | 923609016 | EQUINOR ASA | PDF years list (14 years, 2011-2024) |
| `dnb_pdf_years.json` | 984851006 | DNB BANK ASA | PDF years when key-figure plan is unsupported |

Synthetic fixtures (no real API equivalent needed):
| Fixture file | Coverage |
|---|---|
| `konsern_list.json` | regnskapstype=KONSERN → statement_type=group |
| `no_revenue_list.json` | All resultatregnskap fields null/missing → all null in output |
| `audit_optout_list.json` | fravalgRevisjon=true |
| `liquidation_list.json` | avviklingsregnskap=true |

404 case (`974760673` — REGISTERENHETEN I BRONNOYSUND): no file needed; brregclient test
uses a local HTTP server that returns 404 with empty body.

## Deliverables

1. Go module with all internal packages (models, parser, brregclient, service, httpapi)
2. Unit tests for parser covering all fixture cases above
3. HTTP handler tests (health, single success, mixed batch, max-batch validation, bad org number)
4. Provider (brregclient) tests using local httptest.Server fixtures
5. `Dockerfile` and `Makefile` (GOWORK=off)
6. Entry in `data-pipelines/services/docker-compose.yml` at port 8098
7. Service runnable locally and reachable at `GET /healthz` → `{"status":"ok"}`
