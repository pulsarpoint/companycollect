# Finland — Schema Notes (PRH YTJ API v3)

Source: `https://avoindata.prh.fi/opendata-ytj-api/v3/companies`
Encoding: UTF-8. Dates: `YYYY-MM-DD`. Many fields are multilingual arrays
(`languageCode`: `1`=Finnish, `2`=Swedish, `3`=English).

## Response envelope

```json
{ "totalResults": 819096, "companies": [ { ...company... } ] }
```

- `totalResults` present only when `?totalResults=true`.
- `companies` is the page array (100 per page).

## Company object (observed fields)

| Field | Description |
|---|---|
| `businessId.value` | Business ID / **Y-tunnus** (e.g. `0100002-9`) — the national company identifier. |
| `businessId.registrationDate` | Date the business ID was registered. |
| `businessId.source` | Source/authority code. |
| `names[]` | Name history. Each: `name`, `type` (1 = primary trade name; others = parallel/auxiliary), `registrationDate`, `endDate` (null = current), `version`, `source`. |
| `mainBusinessLine` | Industry. `type` = code, `descriptions[]` (fi/sv/en), `typeCodeSet` (e.g. `TOIMI2` ≈ TOL/NACE), `registrationDate`. |
| `companyForms[]` | Legal form. `type` code + `descriptions[]` (e.g. "Limited company" / "Osakeyhtiö" / "Aktiebolag"), `endDate`, `version`. |
| `companySituations[]` | Special situations (bankruptcy, liquidation, etc.); often empty. |
| `registeredEntries[]` | Register membership entries: `type`, `descriptions[]` (Registered/Ceased), `register` code, `authority`, `registrationDate`, `endDate`. |
| `addresses[]` | Postal/visiting addresses (street, post code, post office, type, source). May be empty for ceased entities. |
| `tradeRegisterStatus` | Trade Register status code. |
| `status` | Overall status code (`1` ≈ active, `2` ≈ inactive/ceased — interpret with care). |
| `registrationDate` | Entity registration date. |
| `endDate` | Entity end/cessation date (null = still active). |

### Code interpretation cautions
- `status`, `type`, `register`, `authority`, `source` are **numeric codes**; rely on the
  embedded `descriptions[]` (English = `languageCode:"3"`) rather than hard-coding code
  meanings. Code lists can evolve.
- "Current" name/form = the array element with `endDate == null`.

## Mapping to internal company model

| Internal field | From PRH |
|---|---|
| `company_id` | `businessId.value` |
| `registration_number` | `businessId.value` |
| `tax_id` | `businessId.value` (Y-tunnus is also the tax number) |
| `vat_id` | `"FI" + businessId.value without the dash` (Finnish VAT = `FI` + 8 digits) |
| `legal_name` | current `names[]` (`endDate == null`, `type == 1`) → `name` |
| `normalized_name` | derive (lowercase, strip form suffix like Oy/Ab/Ky) |
| `company_type` | current `companyForms[]` English description |
| `status` | derived from `status` + `endDate` (active if `endDate` null) |
| `incorporation_date` | `registrationDate` |
| `dissolution_date` | `endDate` |
| `registered_address` | current `addresses[]` (English description preferred) |
| `municipality` / `region` | from address (post office / region) when present |
| `country` | constant `Finland` |
| `source_name` | `PRH Open Data YTJ API v3` |
| `source_url` | request URL |
| `source_retrieved_at` | crawl timestamp (UTC) |
| `raw_record` | full company JSON object (keep verbatim) |

See `normalized/companies.sample.jsonl` and `.csv` for 100 worked examples produced
from `raw/api/prh_ytj_v3_companies_sample.json`.

## Not available in open data
Sole traders, email, phone, municipalities, wellbeing services counties, tax partnerships.
