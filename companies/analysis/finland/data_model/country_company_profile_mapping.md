# Finland Company Profile — Mapping Report

Maps each field of `country_company_profile.schema.json` to its source path in the
PRH YTJ API v3 records. Single primary source, so precedence is mostly "derive
deterministically from PRH"; the precedence column flags the few derived/ambiguous
cases.

- **Primary source:** `prh_ytj_v3` — PRH Open Data YTJ API v3
- **License/access:** CC-BY-4.0, public, no auth
- **Freshness:** daily
- **Join key:** `business_id` (Y-tunnus); secondary `eu_id` (BRIS EUID)

| Profile path | Source | Source path | Join key | Precedence / derivation | Notes |
|---|---|---|---|---|---|
| registration.business_id | prh_ytj_v3 | businessId.value | yes | direct | Primary key |
| registration.business_id_registration_date | prh_ytj_v3 | businessId.registrationDate | | direct | |
| registration.eu_id | prh_ytj_v3 | euId.value | yes | direct | 18/100 present (limited cos) |
| registration.vat_id | (derived) | businessId.value | | `FI`+digits, dash removed | Confirm liability via tax_registrations.vat |
| legal_identity.legal_name | prh_ytj_v3 | names[?type=1 && endDate=null].name | | pick current | If multiple, take latest registrationDate |
| legal_identity.auxiliary_names | prh_ytj_v3 | names[?type in (2,3) && endDate=null].name | | filter | Alternate brands |
| legal_identity.name_history | prh_ytj_v3 | names[] | | array verbatim | Full history |
| legal_identity.legal_form.* | prh_ytj_v3 | companyForms[?endDate=null] | | pick current | label_en = descriptions[langCode=3] |
| status.is_active | (derived) | tradeRegisterStatus + endDate | | `tradeRegisterStatus=='1' && endDate==null` | **Do NOT use `status`** |
| status.trade_register_status_code | prh_ytj_v3 | tradeRegisterStatus | | direct | 1=active,4=ceased,3=intermediate |
| status.raw_status_code | prh_ytj_v3 | status | | verbatim | Constant '2'; unclear meaning |
| status.incorporation_date | prh_ytj_v3 | registrationDate | | direct | 94/100 present |
| status.dissolution_date | prh_ytj_v3 | endDate | | direct | null = active |
| status.special_situations | prh_ytj_v3 | companySituations[] | | array verbatim | Empty in sample; low confidence |
| activity.* | prh_ytj_v3 | mainBusinessLine | | direct | TOL/NACE; code_set = typeCodeSet |
| addresses[] | prh_ytj_v3 | addresses[] | municipality_code | reshape | type 1=visiting, 2=postal |
| addresses[].city / city_sv | prh_ytj_v3 | addresses[].postOffices[?langCode=1/2].city | | pick by language | bilingual |
| addresses[].municipality_code | prh_ytj_v3 | addresses[].postOffices[].municipalityCode | yes | direct | Statistics Finland geo key |
| tax_registrations.vat | (derived) | registeredEntries[?register=6 && endDate=null] | | derive | type 80/82/83/V80 |
| tax_registrations.employer | (derived) | registeredEntries[?register=5 && endDate=null] | | derive | registered employer |
| tax_registrations.prepayment_register | (derived) | registeredEntries[?register=7 && endDate=null] | | derive | ennakkoperintärekisteri |
| register_entries[] | prh_ytj_v3 | registeredEntries[] | | array verbatim | Keep raw history |
| online_presence.website | prh_ytj_v3 | website.url | | direct | 6/100; normalize URL |
| financial_statements[] | prh_financial_statements | (separate API) | business_id | **planning-only** | Not in companies endpoint |
| record_metadata.last_modified | prh_ytj_v3 | lastModified | | direct | Delta-crawl key |
| source_provenance[] | (system) | — | | stamped at ingest | source + retrieved_at |

## Source precedence

There is effectively **one authoritative source** (PRH YTJ v3) for everything in
this profile, so there are no cross-source conflicts to resolve. Precedence rules
that matter:

1. **Liveness:** always derive from `tradeRegisterStatus` (+ `endDate`). Never use
   the raw `status` field — it is a constant `'2'` and is not a liveness indicator.
2. **Current vs historical:** for `names` and `companyForms`, the current value is
   the array element with a null `endDate`; if more than one qualifies, take the
   latest `registrationDate`.
3. **Tax registration flags:** "registered" = an entry exists in that register with
   a null `endDate`. A historical (ended) entry means the company *was* registered
   but no longer is.
4. **Future source (financials):** when the PRH digital financial statement API is
   added, join on `business_id`; it does not override any registry field, it only
   fills `financial_statements[]`.

## Missing-data notes

- `addresses`, `website`, `euId`, `mainBusinessLine` are not present on every record
  (ceased entities frequently lack address/website). Treat as nullable.
- `companySituations` was empty in the entire sample — its element sub-schema is
  unconfirmed; validate against a known bankrupt/liquidating entity before relying
  on `status.special_situations`.
- Several numeric code fields (`source`, `authority`, register codes) have no inline
  documentation; meanings here are inferred from sample descriptions and should be
  confirmed against the official PRH code lists.
