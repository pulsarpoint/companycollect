# Company data sources for Malaysia

## Status

- Official bulk data: **not open** — SSM sells company data; no open bulk register
- Official API: **not open** — company profiles/financials are paid products
- Open data portal: **working** (`data.gov.my` / OpenDOSM) but **no company
  register** (DOSM statistics only)
- License: SSM data is commercially sold; reuse restricted
- Recommended ingestion path: **paid SSM products** (e-Info / MyData-SSM) per company;
  free e-Search for basic existence only

## Best source

**SSM — Suruhanjaya Syarikat Malaysia** (Companies Commission of Malaysia). It is
the official registry for companies (Sdn. Bhd. / Bhd.), LLPs (PLT), and businesses
(ROB). Company and financial data are sold as **paid products** through two
official channels:

- **SSM e-Info** (`ssm-einfo.my`, SSM IDP / SAML login) — **Company Profile**,
  **Business Profile**, **LLP Profile**, **Audit Firm Profile**, **Financial
  Comparison** (2/3/5/10 years), **Financial Historical** (sample PDFs published).
- **MyData-SSM** (`mydata-ssm.com.my`) — "Buy SSM Report": **Company Profile** and
  **Company Financial Report**.

A free **e-Search** gives basic existence/name/number verification, but full
profiles and financials are **paid (per document, MYR)**.

## Financial data — paid

Company **financial statements** (filed annually with SSM) are sold via the
**Financial Comparison / Financial Historical** products on e-Info and the
**Company Financial Report** on MyData-SSM — **paid**, per company. **Bursa
Malaysia** (`bursamalaysia.com`) publishes **listed-company** financials (WAF-
blocked from this environment). There is **no open financial dataset**.

## Identifiers & tax

- **Company registration number** — **new 12-digit format** since 2019 (e.g.
  201901000005) or **old format** (e.g. 1234567-A). Issued by SSM.
- **ROB business registration number** — for sole proprietorships / partnerships.
- **TIN (Nombor Pengenalan Cukai)** — income tax number (LHDN/HASIL); companies
  prefixed `C`.
- **SST registration number** — Sales & Service Tax (Malaysia replaced GST with SST
  in 2018); **no VAT/GST**.
- Currency **MYR**. Languages: Malay + English.

## Next action

Buy **SSM** company profiles / financial reports per company (e-Info / MyData-SSM)
for identity + financials; use the free e-Search for existence checks and **Bursa**
for listed financials. There is **no open bulk register and no open financials**.
Directors/shareholders are personal data (PDPA 2010) — redact if obtained.
