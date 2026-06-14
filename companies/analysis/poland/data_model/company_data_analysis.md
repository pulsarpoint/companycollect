# Company Data Analysis For Poland

## Summary

Poland supports one of the **richest, fully-open** company profiles analysed — on par with Norway/France,
and notably the **only one of the recent set with open beneficial ownership and free structured
financials**. The **KRS API** (free, no-auth JSON) is the spine; the **VAT white list** bridges the three
identifiers (**KRS ↔ NIP ↔ REGON**) and adds VAT status + bank accounts; the **RDF** provides **free
machine-readable financial statements** (e-Sprawozdania XML); **CEIDG** adds sole proprietors; and **CRBR**
exposes beneficial owners — all free. The only real constraints are **PII handling** (CEIDG names, CRBR
PESEL) and the lack of a single full bulk file (enumerate KRS numbers / seed from the white-list flat file).

## Sources Analyzed

| Slug | Source name | Status | Access | License | Role in profile |
|---|---|---|---|---|---|
| krs_api | KRS API | recommended | free, no auth | open | **Spine** (identity/activity/status/capital/officers) |
| krs_rdf_financials | RDF financial statements | recommended | free per-company | open | **Financials** (structured XML) |
| vat_whitelist | Biała lista VAT | recommended | free, no auth | open | **Bridge** NIP↔REGON↔KRS + VAT + bank accounts |
| ceidg | CEIDG | recommended | free token | open | Sole proprietors (completeness) |
| crbr_beneficial_ownership | CRBR | recommended | free, no auth | open | **Beneficial ownership** (open) |

Also in `source_inventory.json`: REGON/GUS BIR1 (all-entity cross-reference, free key), dane.gov.pl
(catalog), commercial aggregators (Rejestr.io/MGBI — resell the open data; optional).

## What Each Source Contributes

- **krs_api (spine).** Free JSON with numerKRS, NIP, REGON, nazwa, forma prawna, address + website,
  share capital, PKD activity, board (anonymized), fiscal year, liquidation/bankruptcy, and **mentions of
  filed financial statements** (the trigger for RDF). Verified live (PKO BP, 59 KB). Companies only.
- **krs_rdf_financials (financials).** Free per-company **e-Sprawozdania finansowe XML** (MF schema):
  **bilans** (suma bilansowa, kapitał własny, zobowiązania) + **rachunek zysków i strat** (przychody netto,
  zysk/strata netto) + employees. Open and machine-readable — Poland's standout vs DE/IT/ES.
- **vat_whitelist (bridge).** One lookup returns **NIP + REGON + KRS**, VAT status (Czynny/Zwolniony),
  **bank accounts**, address, representatives. Verified live. The canonical identifier bridge and a
  population seed (daily flat file).
- **ceidg (sole traders).** The millions of individual entrepreneurs the KRS does not cover; free token;
  keyed on NIP. Entrepreneur PII.
- **crbr_beneficial_ownership (owners).** Free public beneficial owners by NIP — open ownership Poland
  offers and DE/IT do not. Sensitive PII (PESEL) → minimize.

## Proposed Country Company Profile

`country_company_profile.schema.json` (+ `.example.json`, built from real PKO BP open data) models a
Poland-specific object with `entity_kind` (company vs sole_proprietor), `registration` (KRS + NIP + REGON +
rejestr), `legal_identity`, `status` (+ vat_status), `activity` (PKD), `registered_location`, `capital`,
`contact` (website + bank_accounts), `officers[]` (anonymized), `beneficial_owners[]` (CRBR, PII-minimized),
`financial_statements[]` (open RDF XML), `filing_signals` (KRS wzmianki), and `source_provenance[]`.

## Join And Precedence Rules

- **Clean multi-key, all open**: KRS (companies), NIP (universal), REGON — the **white list** returns all
  three together. Sole traders (CEIDG) key on NIP.
- **Authority**: KRS authoritative for legal data; white list for VAT/accounts/bridge; RDF for financials;
  CRBR for owners.
- **Build order**: KRS spine → white list (bridge) → RDF (financials, via KRS wzmianki) → CRBR (owners) →
  CEIDG (sole traders). Freshness: KRS/CRBR continuous, white list/CEIDG daily, RDF annual.
- **Normalization**: REGON 14→9 digit; financial XML schema versions + units (whole vs thousands) + two P&L
  variants; PL decimal comma.

## Missing Or Restricted Data

- Very little is missing — identity, financials, ownership, VAT, bank accounts are **all open**.
- **PII handling** is the constraint: CEIDG entrepreneur names + **CRBR beneficial owners (incl. PESEL)** —
  GDPR minimization required; KRS board members are anonymized by the source.
- **No single full bulk** — enumerate KRS numbers or seed from the white-list daily flat file; mass RDF
  access needs the PRS-eKRS registration.
- **Financial XML** schema is versioned yearly + has entity-type variants; some filings are PDF-only/XBRL.

## Common Mapper Notes

See `common_field_mapping_suggestions.md`. Poland is a **top-tier open case**: a cross-country mapper gets
identity + **financials** + **beneficial ownership** + VAT + bank accounts for free. Prefer KRS for
companies / NIP for sole traders; PKD activity is open; financials need schema-version + unit handling;
PII (CEIDG/CRBR) is the real constraint, not availability.
