# Zefix Public REST API Field Catalog

> **CREDENTIALED (free).** All endpoints require HTTP Basic auth (`Zefix-
> Credentials`) and returned **HTTP 401** on this run. Fields are from the public
> OpenAPI 2.7 spec, **documented-but-unverified**. No records retrieved. Same open
> data as LINDAS, plus status/capital/SOGC. Credentials are free on request.

## Source Summary

- Country: Switzerland
- Source type: official_registry
- Organization: EHRA
- URL: https://www.zefix.admin.ch/ZefixPublicREST/api/v1/
- License: OGD / Open use (attribution)
- Access: restricted (free HTTP Basic credentials)
- Freshness: daily
- Record shape: JSON company object
- Primary keys: `uid`
- Join keys: `uid`, `chid`, `ehraid`

## Endpoints (from OpenAPI 2.7)

`POST /company/search` · `GET /company/uid|ehraid|chid/{id}` · `GET /sogc/{id}` ·
`GET /sogc/bydate/{date}` · `GET /legalForm` · `GET /registryOfCommerce` ·
`GET /community`.

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| status | status | Entity status | string | status | fills LINDAS status gap |
| legalSeat | legalSeat | Registered seat | string | geography | |
| capitalNominal/Currency | capital | Registered capital | decimal | financial | NOT financial statements |
| purpose | purpose | Business purpose | string | metadata | |
| sogcPublications[] | sogcPublications | Gazette refs | array | filing | see sogc_shab |
| legalFormId | legalFormId | Legal-form code | string | legal_form | /legalForm classifier |

## Interpretation Notes

- The same open Zefix data as LINDAS, exposed per-entity with **status**,
  **registered capital**, and **SOGC publication links**. The route to **status**
  and **mutation history** that LINDAS lacks.
- **Gated by free Basic-auth credentials** — request from EHRA; do not bypass the
  401. Mark **blocked_by_authentication** until credentials are configured.
- `capital` is the **registered nominal capital**, not annual-account financials
  (which Switzerland does not publish for private companies).
