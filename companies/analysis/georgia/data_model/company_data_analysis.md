# Company Data Analysis For Georgia

## Summary

Georgia's company data centres on one universal key — the **9-digit identification code
(საიდენტიფიკაციო კოდი)**, which is both the **registration number** and the **tax id** —
but, contrary to Georgia's open-registry reputation, the sources are gated from this
environment. The authoritative **NAPR e-registry** (`enreg.reestri.gov.ge`) gates its public
search/extract behind a **CAPTCHA**, and `api.napr.gov.ge` returns **Access Denied**. The
browser-public **SARAS Reporting Portal** (`reportal.ge`) is the practical open source for
**annual financial statements** (search is anti-forgery-token-gated for automation). The
**Georgian Stock Exchange** lists securities by **ISIN**. **data.gov.ge** was firewalled /
cert-broken from the investigation environment. A company profile can be **modelled** around
the identification code, but no registry per-company values were captured (and none were
fabricated).

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| napr_enreg | NAPR e-registry (enreg) | blocked_authentication | public search, CAPTCHA-gated | restricted | Authoritative registry: identity, status, address, director, partners |
| reportal_saras | SARAS Reporting Portal (reportal.ge) | insufficient_transport_info | browser-public, token-gated | public disclosure (unconfirmed) | Annual financial statements + management reports; NACE |
| gse_listed | Georgian Stock Exchange | insufficient_transport_info | browser-public | public disclosure | Listed securities (ISIN) |

(`data_gov_ge` was firewalled from this environment and is not modeled as a data source.)

## What Each Source Contributes

- **NAPR e-registry** — the authoritative registry: identification code, legal name, legal
  form (შპს/სს/ი.მ), status, registration date, registered address, director, and partners/
  shareholders (in the extract). CAPTCHA-gated; API Access Denied; planning-only. Director/
  partners are personal data — redact.
- **reportal.ge (SARAS)** — annual **financial statements** + management reports (PDF, GEL)
  and **NACE** activity codes, keyed on the identification code. Browser-public; automation
  needs the anti-forgery token.
- **GSE** — listed-security **ISINs** (`GExxxxxxxxxx`; 32 observed); listed companies only;
  HTML page, join to the register by name.

## Proposed Country Company Profile

An identification-code-keyed object with sections: `registration`, `legal_identity` (name,
legal form), `status` (+ registration date), `activity` (NACE, reportal), `registered_
location` (NAPR), `officers` + `owners` (NAPR, redacted), `financial_statements` (reportal,
PDF/GEL), and `listing` (GSE ISIN), each with `source_provenance`. The example is anchored on
a real observed identification code (`110782780`, a შპს/LLC) with NAPR-gated fields null and
personal data `[REDACTED-PII]`.

## Join And Precedence Rules

- **Primary key**: identification code (NAPR == Revenue Service == reportal). **Join** NAPR ⟷
  reportal on it; **GSE** joins by **name** (no ID code on the securities page).
- **Precedence**: NAPR authoritative for identity/status/address/officers/owners; reportal
  for financials + NACE; GSE for listing.
- **Currency** GEL; **language** Georgian (Mkhedruli) + English; dates Gregorian.

## Missing Or Restricted Data

- **NAPR is CAPTCHA-gated** (API Access Denied) → registry fields are planning-only; nothing
  captured.
- **reportal financials** are inside **PDFs** (not structured); search token-gated.
- **Director / partners** are personal data (Law on Personal Data Protection) — redact.
- **data.gov.ge** was firewalled from this environment — re-check elsewhere for a possible
  open company dataset.
- No separate VAT id — the **identification code** is the tax id.

## Common Mapper Notes

`company_id` / `registration_number` / `tax_id` / `vat_id` all map to the **identification
code**; `financials` → reportal PDFs (GEL); `officers`/`owners` → NAPR (redacted, gated);
`activity_code` → reportal NACE. NAPR is `blocked_authentication`; reportal and GSE are
`insufficient_transport_info`. All registry mappings are planning-only until the CAPTCHA / an
official data channel is resolved.
