# Georgia Company Profile — Mapping

Georgia's company data hinges on a single key — the **9-digit identification code
(საიდენტიფიკაციო კოდი)**, which is both the **registration number** and the **tax id** —
but the sources are gated. The authoritative **NAPR e-registry** is **CAPTCHA-gated** (its
API is Access Denied); the **SARAS Reporting Portal** (reportal.ge) is browser-public for
**financial statements** but anti-forgery-token-gated for automation; the **GSE** lists
securities by **ISIN**. data.gov.ge was firewalled from the investigation environment. No
registry per-company values were captured.

## Identifiers

- **Identification code** — 9-digit; registration number + tax id; universal key
  (NAPR == Revenue Service == reportal).
- **ISIN** — `GExxxxxxxxxx`; GSE listed securities (listed only).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.identification_code | napr_enreg / reportal_saras | identification_code | yes | NAPR > reportal | CAPTCHA (NAPR) / token (reportal) |
| legal_identity.legal_name | napr_enreg | legal_name | no | NAPR > reportal | reportal orgName as alt |
| legal_identity.legal_form | napr_enreg | legal_form | no | NAPR / reportal | შპს/სს/ი.მ |
| status.status_text | napr_enreg | status | no | NAPR | active/liquidated |
| status.registration_date | napr_enreg | registration_date | no | NAPR | Gregorian |
| activity.nace_codes | reportal_saras | naceCodes | no | reportal | NACE Rev.2 |
| registered_location.registered_address | napr_enreg | registered_address | no | NAPR | CAPTCHA-gated |
| officers[] | napr_enreg | director | no | NAPR | **PERSONAL DATA — REDACT** |
| owners[] | napr_enreg | partners | no | NAPR | **PERSONAL DATA — REDACT** |
| financial_statements[] | reportal_saras | financial_statements_pdf | no | reportal | PDF; GEL; token-gated |
| listing.isin | gse_listed | isin | no | GSE | listed only |
| listing.security_name | gse_listed | security_name | no | GSE | join by name |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Identity / registration / status / address / officers / owners**: **NAPR** is
  authoritative (CAPTCHA-gated). **Activity (NACE) + financial statements**: **reportal.ge**
  (browser/token). **Listing**: **GSE** (by ISIN).
- **Join**: NAPR ⟷ reportal on the **identification code** (both use it). **GSE** joins by
  **name** (no identification code published on the securities page).
- **Currency** GEL. **Language** Georgian (Mkhedruli) + English. Dates Gregorian.

## Missing / restricted

- **NAPR is CAPTCHA-gated** and its API is Access Denied → no registry values captured; all
  NAPR fields are planning-only.
- **reportal financials** are inside filed **PDFs** (not structured fields); search is
  token-gated for automation.
- **Director / partners** are personal data under the **Law on Personal Data Protection** —
  redact.
- **data.gov.ge** was firewalled from the investigation environment (re-check elsewhere).
- The **identification code** itself is public (company/tax id), not personal data.
