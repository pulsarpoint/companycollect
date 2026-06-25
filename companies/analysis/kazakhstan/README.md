# Company data sources for Kazakhstan

## Status

- Official bulk data: found (open legal-entities register dataset; free API key required)
- Official API: found — data.egov.kz API (gbd_ul); requires a free API key
- Open data portal: found (data.egov.kz)
- License: open (data.egov.kz terms); KGD restricted
- Recommended ingestion path: API (gbd_ul) after registering a free API key

## Best source

**data.egov.kz — State Database of Legal Entities (ГБД ЮЛ / `gbd_ul`)** is the authoritative
**open company register**: it holds, per legal entity, the **BIN** (12-digit Business
Identification Number), name (RU/KZ), registration date, legal address, activity (**OKED**),
and director name. It is served via the data.egov.kz API
(`https://data.egov.kz/api/v4/gbd_ul/<version>?apiKey=…`) — verified to return **HTTP 403
"API key is required"** without a key, so a **free API key (registration on data.egov.kz)**
is needed. **KGD** (State Revenue Committee) adds tax/VAT status by BIN; **KASE** covers
listed companies.

## Next action

Register a free data.egov.kz API key, then pull `gbd_ul` via the API (JSON/XML/CSV) keyed on
**BIN**. Use KGD for tax/VAT status and KASE for listed companies. Convert dates; redact the
director's name (personal data). Confirm the current dataset version string before ingesting.
