# Company Data Analysis For Denmark

## Summary

Denmark is one of the richest and easiest open-company-data jurisdictions. A single publisher —
**Erhvervsstyrelsen (Danish Business Authority)** — exposes everything through one distribution
host, `distribution.virk.dk`, in two layers:

- **Base register (CVR / `cvr-permanent`)** — the authoritative register of all 2,194,982 Danish
  legal entities (plus 2,787,126 production units and 1,772,344 participants). Comprehensive
  identity, status, legal form, address, industry, capital, employment, lifecycle, and
  ownership/management. **Free, but behind HTTP Basic credentials** (request by email + sign a
  protected-data declaration). No payment.
- **Financial statements (`offentliggoerelser` + the XBRL documents)** — **completely open, no
  auth.** 6,295,759 published filings, each linking to machine-readable **XBRL / Inline XBRL**
  annual reports built on the Danish **DCCA taxonomy** (with IFRS/ESEF for listed groups), so the
  actual income-statement and balance-sheet figures are extractable, not just document links.

A full Danish company profile can therefore be built end-to-end. The financial half is buildable
today with zero credentials; the base half needs a one-time free credential request. The
universal join key is the **8-digit `cvrNummer`**, and the Danish VAT number is simply
`'DK' + cvrNummer`.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| cvr_permanent | CVR-permanent (Det Centrale Virksomhedsregister) | recommended | public_with_free_credentials | Free reuse (CVR-loven) | Authoritative base register: identity, status, form, address, industry, capital, employment, ownership |
| cvr_offentliggoerelser | Offentliggørelser / Regnskaber (filing metadata) | recommended | **public (no auth)** | Free / open | Financial-filing discovery + document URLs |
| cvr_regnskab_xbrl | Regnskaber XBRL/iXBRL documents (DCCA) | recommended | **public (no auth)** | Free / open | Parsed financial-statement facts, board members, reporting class |
| cvr_registreringstekster | Registreringstekster (change texts) | useful_secondary_source | public_with_free_credentials | Free reuse | Registration/change-event history (planning-only) |
| (datahub.virk.dk) | Virk Data open-data catalog | useful_secondary_source | public | varies | Catalog/metadata only — confirms publisher & route; no company fields, not cataloged separately |
| (cvr.dev / cvrapi.dk / apicvr.dk) | Third-party REST wrappers | useful_secondary_source | varies | provider terms | Convenience mirrors of CVR for ad-hoc lookups; not used for ingestion, not cataloged separately |

## What Each Source Contributes

- **cvr_permanent** — the backbone: `cvrNummer`, name history (`navne`), legal form
  (`virksomhedsform`), status (`virksomhedsstatus`), addresses (`beliggenhedsadresse` /
  `postadresse`), DB07 industries (`hovedbranche` + `bibranche`), capital and purpose
  (`attributter`), employment bands (`aarsbeskaeftigelse` / `erstMaanedsbeskaeftigelse`),
  lifecycle (`livsforloeb`), and ownership/management (`deltagerRelation` → `deltager`,
  incl. beneficial owners). Most attributes are period-stamped; the `virksomhedMetadata` rollup
  pre-computes the newest values.
- **cvr_offentliggoerelser** — open discovery of every published filing: CVR, accounting period,
  publication/update timestamps, and a `dokumenter[]` array of typed documents (AARSRAPPORT,
  DELAARSRAPPORT, ESEF) with direct URLs. Real samples observed (CVR 25313763 and Maersk
  22756214).
- **cvr_regnskab_xbrl** — the figures: parse the linked XBRL to get identity (`gsd:`), reporting
  class (`fsa:ClassOfReportingEntity`), period, audit status, board/executive members (`cmn:`),
  and the monetary `fsa:` line items (revenue/profit/assets/equity). Each fact must be read with
  its `xbrli:context` (period + consolidated/solo) and `xbrli:unit` (ISO-4217 currency). A real
  Maersk Q1-2026 instance was downloaded and decompressed.
- **cvr_registreringstekster** — narrative change history, same credentials as the base register;
  documented planning-only (schema not yet inspected).

## Proposed Country Company Profile

`country_company_profile.schema.json` is country-first and grouped by real CVR concepts:
`registration` (cvr_nummer, vat_id, lei, incorporation), `legal_identity` (name + history, form,
purpose, capital), `status` (status, dissolution, **advertising_protected**), `activity` (DB07
primary/secondary), `registered_location` (structured address + municipality), `contact`,
`employment` (bands), `production_units` (P-numbers), `participants` (owners/board — personal
data), `financial_filings` (discovery), `financial_statements` (parsed XBRL facts + board), and
`change_history` (planning-only), each carrying `source_provenance`.
`country_company_profile.example.json` instantiates it for A.P. Møller - Mærsk A/S, with the
financial/XBRL sections populated from real downloaded data and the base-register sections shown
as illustrative.

## Join And Precedence Rules

- **`cvrNummer`** (8-digit) joins all layers; **`pNummer`** links production units; **`filing_id`**
  links filing metadata to parsed XBRL; **LEI** optionally links listed filers to GLEIF.
- Precedence: base register (`cvr_permanent`) is authoritative for identity/status/structure;
  parsed XBRL (`cvr_regnskab_xbrl`) is authoritative for financial figures; `cvr_offentliggoerelser`
  is metadata only. Third-party wrappers never preferred for ingestion.
- Period resolution: pick the array entry whose `periode.gyldigTil` is null for "current" values,
  or read `virksomhedMetadata`.
- Freshness: both layers are near real-time; refresh financials incrementally on `sidstOpdateret`,
  and the base register via scroll re-loads (no public delta feed documented).

## Missing Or Restricted Data

- **Behind free credentials (one-time email + declaration):** the entire base register
  (`cvr_permanent`) and `registreringstekster`. Until obtained, identity beyond what XBRL exposes,
  status, legal form, activity, employment, ownership and production units are unavailable — but a
  useful partial profile (id, vat, lei, name/address, full financials) can still be built from the
  open layer.
- **Personal data (GDPR + address protection):** `deltager` / beneficial owners and XBRL board
  names. Handle under GDPR; honour address protection.
- **Advertising protection (`reklamebeskyttelse`):** a license obligation — flag protected entities
  and gate any marketing use.
- **Unstructured legacy filings:** pre-digital reports are image/TIFF or PDF with no extractable
  figures.
- **No paid data required**; no plain-CSV bulk dump (scroll API is the bulk path).

## Common Mapper Notes

Denmark maps cleanly to a cross-country schema: `company_id`/`registration_number`/`tax_id` all =
`cvrNummer`; `vat_id` = `'DK'+cvrNummer`; `legal_form` via virksomhedsform codes; `activity_code`
via DB07→NACE; `financials` from DCCA `fsa:` facts. Caveats: employee counts are interval bands
(approximate); a tax_id distinct from the company number is `not_available_in_open_sources`; full
owners/officers need CVR credentials (a partial officer view is open via XBRL board tags). See
`common_field_mapping_suggestions.md`.
