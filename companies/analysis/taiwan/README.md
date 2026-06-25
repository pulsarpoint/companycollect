# Company data sources for Taiwan

## Status

- Official bulk data: found (open JSON APIs; query-by-ID + full listed arrays)
- Official API: found — **GCIS OpenData**, **TWSE OpenAPI**, **TPEx OpenAPI** (all open)
- Open data portal: found (data.gov.tw indexes the above)
- License: open (Open Government Data License, Taiwan)
- Recommended ingestion path: **API** (GCIS by 統一編號 for all companies; TWSE/TPEx full arrays for listed)

## Best source

**MOEA GCIS — Company Registration Basic Data OpenData API**
(`data.gcis.nat.gov.tw`). The official register of **all** Taiwanese companies, keyed on
the 8-digit **統一編號 (Unified Business Number / Business_Accounting_NO)**. Fully open
JSON REST API, no auth or payment — verified live (TSMC `22099131` returns name, status,
capital, paid-in capital, responsible person, address, registering authority, setup and
last-change dates). For **listed** companies, the **TWSE OpenAPI** (`openapi.twse.com.tw`,
`t187ap03_L`, 1,089 companies) and **TPEx OpenAPI** (`tpex.org.tw/openapi`,
`mopsfin_t187ap03_O`, 890 OTC companies) add rich disclosure fields and **join to GCIS on
the same 統一編號**.

## Next action

Implement GCIS by-統一編號 lookups for the universal company layer, and ingest the TWSE +
TPEx full listed arrays for the disclosure/listing layer. Join all three on the unified
business number. Handle ROC/Minguo dates (GCIS) vs Gregorian (TWSE/TPEx). Redact
responsible-person / chairman / GM / spokesperson / auditor names (PDPA) in stored profiles.
