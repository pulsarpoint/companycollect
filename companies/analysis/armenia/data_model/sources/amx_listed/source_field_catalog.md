# Armenia Securities Exchange (AMX) — listed securities Field Catalog

## Source Summary

- Country: Armenia
- Source type: financial_disclosure
- Organization: Armenia Securities Exchange (AMX)
- URL: https://amx.am/en
- License: public disclosure
- Access: **public via browser; JavaScript SPA** (no clean public JSON API found)
- Freshness: event-driven
- Record shape: listed-securities JS SPA
- Primary keys: isin
- Join keys: isin, issuer_name

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| isin | ISIN | Securities id | string | identifier | AM... | Armenian ISINs begin AM |
| instrument_name | Instrument / Security Name | Security name | string | legal_name |  | listed only |
| issuer_name | Issuer | Issuing company | string | legal_name |  | join by name/TIN |

## Interpretation Notes

- The **Armenia Securities Exchange** site is a **JavaScript SPA**: static fetches return an
  empty ~3 KB shell, and **no clean public JSON API was found** (guessed `api.amx.am` and
  `amx.am/api/instruments` returned 404 or the shell). Listed-instrument data is loaded
  client-side. **Browser-public but not cleanly automatable** from this environment.
- **Scope**: **listed securities only**; small market (equities + bonds). **Join**: ISIN
  (`AMxxxxxxxxxx`) keys the security; the page does **not** publish the TIN, so listed issuers
  join to the State Register by **name**.
- No `sample_record.json`: only an empty SPA shell was retrieved (no structured data).
