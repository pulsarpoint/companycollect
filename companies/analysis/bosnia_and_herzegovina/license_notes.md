# License & terms — Bosnia and Herzegovina

## Summary

No source publishes an explicit open-data/reuse license. All registers are
public to consult **per company**; redistribution terms are **not stated**.
Treat reuse terms as **uncertain**, and treat personal data carefully.

## Per source

### RS Business Register (`bizreg.esrpska.com`, APIF / RS courts)
- Public per-company search; structured JSON returned by the site's own AJAX
  endpoint. No stated bulk/reuse license. Free.
- Access for identity verification; no documented permission for bulk
  redistribution. Query per company; do not scrape aggressively.

### FBiH & Brčko register (`bizreg.pravosudje.ba`, VSTV/HJPC)
- Public per-company court-register search (Oracle APEX). No open bulk; no stated
  reuse license. Free to consult.

### APIF — RFI / Registar boniteta (RS financial statements)
- Annual financial statements and bonitet reports are **paid per company**
  (naknada in KM) under RS law on the unified register of financial statements.
  No open bulk. Catalogued from public documentation only — **no raw financial
  values copied** into this repo.

### FIA (FBiH financial statements)
- FBiH counterpart; **paid per-company** reports / bonitet. No open bulk.
  Catalogued from public docs only.

### UINO (JIB / PDV VAT)
- State indirect-tax authority; per-company taxpayer/PDV verification. No open
  bulk taxpayer list. Free per-company.

## Personal data

BiH has a **Law on the Protection of Personal Data** (Zakon o zaštiti ličnih
podataka). Company registers expose **founders/owners (Osnivači)**, the
**responsible person (Odgovorno lice)**, and authorised representatives — these
are **personal data when natural persons**. They are **redacted** in committed
samples (`[REDACTED-PII...]`). Only company-level fields (JIB, MBS, name,
address, activity, status) and founders that are themselves legal entities are
kept.

## Practical guidance

- Prefer per-company lookups (RS JSON / FBiH APEX) keyed on JIB/name.
- Do not redistribute bulk extracts without confirming terms with APIF / the
  courts / FIA.
- Financials: obtain per company from APIF RFI / FIA (paid); do not assume reuse
  rights.
- Currency BAM (KM); dates dd.mm.yyyy.
