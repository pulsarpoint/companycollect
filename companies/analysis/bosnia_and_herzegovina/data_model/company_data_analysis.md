# Company Data Analysis For Bosnia and Herzegovina

## Summary

Bosnia and Herzegovina has **no single national company register** — registration
is done by the **entity court registers**. A practical company profile is built by
routing on the entity and joining on the **JIB** (13-digit unique id = company id =
tax id):

- **Republika Srpska**: `bizreg.esrpska.com` — a **public JSON per-company search**
  (`/Home/SearchPoslovniSubjekt`) returning JIB, MBS, MB, name, address, activity,
  status, founders, plus per-company **PDF official extracts**. This is the best,
  structured, open source. Verified live.
- **Federation of BiH + Brčko District**: `bizreg.pravosudje.ba` — an Oracle APEX
  per-company search portal (no JSON/bulk confirmed).

Financial statements (**bilans stanja / bilans uspjeha**, BAM) are filed with
**APIF (RFI)** in RS and **FIA** in FBiH, accessible **per company for a fee**
(planning-only). VAT (**PDV broj**, 12-digit) is assigned by **UINO**, separate
from the JIB. There is **no open bulk** and **no working national open-data
portal**.

## Sources Analyzed

| Source slug | Name | Status | Access | License | Role |
|---|---|---|---|---|---|
| rs_business_register | RS Business Register (bizreg.esrpska.com) | recommended | public JSON search | public per-company | Primary identity (RS) |
| fbih_brcko_court_register | FBiH & Brčko register (bizreg.pravosudje.ba) | insufficient_transport_info | public APEX search | public per-company | Identity (FBiH/Brčko) |
| apif_rfi_financials | APIF RFI / FIA financial statements | blocked_payment | paid per company | paid | Financials (planning-only) |

(UINO PDV and the APIF Registar boniteta are recorded as per-company secondary
enrichments in the discovery inventory.)

## What Each Source Contributes

- **rs_business_register** — the richest open structured data: a single AJAX query
  (`term=`) yields JIB, MBS, MB, `PoslovnoIme`, `Sjediste`, `PreteznaDjelatnost`,
  `StatusPoslovniSubjekatOpis`, `Osnivaci`, `PoslovneJedinice`, contacts; plus a
  per-company PDF extract and sub-lists (representatives, branches, procura). RS
  only.
- **fbih_brcko_court_register** — covers the rest of BiH (FBiH cantons + Brčko).
  Per-company APEX search by Naziv/JIB/MBS; same logical fields; transport not yet
  reverse-engineered.
- **apif_rfi_financials** — annual balance sheet / income statement (BAM) and
  creditworthiness; paid per company; planning-only (no raw values copied).

## Proposed Country Company Profile

`country_company_profile.schema.json` keys on **JIB** with sections:
`registration` (jib/mbs/mb/entity), `tax_identifiers` (tax_id=JIB, pdv_broj),
`legal_identity`, `status`, `activity`, `registered_location` (incl. branch_count),
`owners` (redacted), `officers` (redacted), `financial_statements[]`
(planning-only, BAM), and `source_provenance[]`. The example record is the real
**"Nova banka" a.d. Banja Luka** (JIB 4400374890002).

## Join And Precedence Rules

- **JIB** is the universal join key across court, financial, and tax registers.
- The **entity court register** holding the company is authoritative for identity.
  RS via the JSON source; FBiH/Brčko via the APEX portal.
- **Financials** join on JIB from APIF RFI (RS) / FIA (FBiH); **VAT** from UINO.
- All sources are **live** except financials (**annual**, paid).

## Missing Or Restricted Data

- **No open bulk** anywhere; everything is per-company.
- **No national open-data portal** (`data.gov.ba` did not resolve).
- **FBiH/Brčko transport unconfirmed** (Oracle APEX) — implementation blocked
  pending request/response reverse-engineering.
- **Financials paid** (APIF RFI / FIA) — planning-only.
- **Incorporation/dissolution dates** appear only on per-company extracts/PDFs, not
  in the search envelope.
- **Founders/representatives** are personal data — redacted in committed samples.

## Common Mapper Notes

`company_id == tax_id == JIB`; `vat_id` (PDV broj) is separate. Route by entity but
merge on JIB. Only the RS source is open + structured; the rest are per-company
APEX or paid. See `common_field_mapping_suggestions.md`.
