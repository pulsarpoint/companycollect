# Company Data Analysis For Kazakhstan

## Summary

Kazakhstan has a **genuine open company register** — the strongest since Taiwan/Pakistan —
keyed on the **12-digit BIN (Business Identification Number)**, gated only by a **free API
key**. The authoritative open source is **`gbd_ul`** (State Database of Legal Entities) on the
national open-data portal **data.egov.kz**: per legal entity it carries the **BIN**, name
(RU/KZ), **registration date**, **legal address**, **OKED activity**, and **director name**.
It is served via the data.egov.kz API (`/api/v4/gbd_ul/<version>?apiKey=…`) — **verified to
return HTTP 403 "API key is required"** without a key, so a **free API key (registration)** is
needed. The **State Revenue Committee (KGD)** adds **tax/VAT status** by BIN/IIN (browser-
public search + lists), and **KASE** provides the listed layer by **ISIN**. A rich profile is
buildable once the free key is obtained; no per-company values were captured here (no key) and
none were fabricated.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| egov_gbd_ul | data.egov.kz gbd_ul (State DB of Legal Entities) | blocked_authentication (free key) | open API, free API key required | data.egov.kz open terms | Open register: BIN, name, reg date, address, OKED, director |
| kgd_taxpayer | State Revenue Committee (KGD) | insufficient_transport_info | browser-public search/lists | restricted | Tax/VAT status by BIN/IIN |
| kase_listed | Kazakhstan Stock Exchange (KASE) | insufficient_transport_info | browser-public SPA | public disclosure | Listed companies (ISIN) |

(`stat_gov_kz` is statistics, not a per-company register — not modeled.)

## What Each Source Contributes

- **gbd_ul** — the authoritative open register: BIN, name (RU/KZ), registration date, legal
  address, OKED activity, director name (personal data — redact). Covers legal entities,
  branches, and representative offices. data.egov.kz API; **free API key required**.
- **KGD** — **tax/VAT status**: BIN/IIN, taxpayer name, VAT (НДС) registration, taxpayer
  status (active/inactive/pseudo-enterprise/debtor) via browser-public search + published
  XLSX lists. Covers individuals too (personal data).
- **KASE** — listed-company **ISINs** (`KZxxxxxxxxxx`), tickers, issuer names; browser-public
  SPA, no clean API confirmed; listed only.

## Proposed Country Company Profile

A BIN-keyed object with sections: `registration` (bin), `legal_identity` (name), `status`
(registration_date + KGD taxpayer_status + vat_registration), `activity` (OKED),
`registered_location` (legal_address), `officers` (director, redacted), and `listing` (KASE
ISIN), each with `source_provenance`. The example is anchored on the BIN-keyed model with
gbd_ul fields null (API key not obtained) and the director `[REDACTED-PII]`.

## Join And Precedence Rules

- **Primary key**: BIN, shared by gbd_ul and KGD. **Join** gbd_ul ↔ KGD on the BIN; **KASE**
  joins by **name** (no BIN on the page).
- **Precedence**: gbd_ul authoritative for registration identity/activity/address/officers
  (free-key-gated); KGD for tax/VAT status; KASE for listing.
- **Keep two statuses distinct**: gbd_ul **registration data** vs KGD **taxpayer_status** (tax).
- **Language** Russian (+ Kazakh); **currency** KZT; **activity** OKED classifier.

## Missing Or Restricted Data

- **`gbd_ul` requires a free API key** → no values captured here (obtain a key to ingest); the
  dataset is genuinely open, just registration-gated.
- **KGD** is **per-BIN search / per-list** (no single clean API) and includes **individuals**
  (personal data — redact).
- **KASE** populated listings are not cleanly available (SPA).
- **legal_form**, **owners/founders**, **financials**, **dissolution_date** are not in the
  described gbd_ul fields (legal form inferable from name; deregistration via KGD lists).
- **Director name** (gbd_ul) and **individual taxpayers** (KGD) are personal data — redact.

## Common Mapper Notes

`company_id` / `registration_number` / `tax_id` → the **BIN** (gbd_ul, free-key-gated);
`vat_id` is a **status** via KGD; `activity_code` → OKED; `officers` (director) redacted.
`legal_form`, `owners`, `financials` are `not_available_in_open_sources`. gbd_ul is
`blocked_authentication` (free key); KGD and KASE are `insufficient_transport_info`.
