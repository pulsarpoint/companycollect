# Portugal — Company Open Data Investigation

## Conclusion

Portugal is a **partial-open / paid + automation-blocked** country. The authoritative register, the **Registo
Comercial** (IRN / Ministério da Justiça), holds rich per-company data (name, sede, CAE, legal form, capital,
**sócios**, **gerência**) but it is accessed via the **paid certidão permanente** — there is **no open bulk/API**.
The free publication of company **acts** (`publicacoes.mj.pt`) is **reCAPTCHA-protected** (manual only). **IES**
financial statements are **not openly published** per company. The open data portals carry only **statistical
aggregates**. Everything joins on the **NIPC** (9-digit collective-entity number = the company's NIF/tax number;
VAT = `PT` + NIPC).

## What was verified (live, with real downloads)

- **dados.justica.gov.pt** (CKAN) — `package_search?q=empresas` → 12 datasets: `empresas`, `eol`, `fcpc`
  (Ficheiro Central de Pessoas Coletivas), `insolvencia`, `enh`, etc. Inspected `empresas`, `fcpc`, `insolvencia`
  and the `rco` dataset — **all are STATISTICAL aggregates** (e.g. "N.º de Constituições e Extinções de
  sociedades", "N.º de certificados de admissibilidade", insolvency processes per quarter). **None is a
  per-company register.**
- **RCO (Registo Comercial Online)** `rco.csv` → HTTP 200, downloaded; it is a **120-row monthly time series**
  (online registrations by transcription/deposit + accumulated totals), columns: `Year-Month`, `Registos por
  transcrição requeridos online`, `Acum. …`, `Registos por depósito …`, `EoMonth`. **Statistical, not
  per-company.** License **CC-BY-SA**.
- **publicacoes.mj.pt** `pesquisa.aspx` → HTTP 200; the search page references **reCAPTCHA** and a **NIPC** field
  → free per-company company-acts search, but **reCAPTCHA-protected** (automation blocked).
- **Certidão permanente** (eportugal.gov.pt) → HTTP 200 (paid service). **Banco de Portugal** → HTTP 403 (WAF).

## Identifiers

- **NIPC** (Número de Identificação de Pessoa Coletiva) — 9 digits; the company id and join key. For companies it
  **equals the NIF** (tax number). **VAT** = `PT` + NIPC.
- **CAE** (Classificação Portuguesa das Atividades Económicas, Rev.3) — activity code (NACE-aligned).
- **Número de matrícula** — commercial-registration number (often = NIPC).
- Language: **Portuguese**.

## Financial data

- The **IES (Informação Empresarial Simplificada)** is a single annual filing combining **accounting (SNC),
  tax (AT), statistics (INE) and Banco de Portugal** data — including **balanço** (balance sheet), **demonstração
  de resultados** (income statement), anexo, and employee counts. It is the deposit of accounts ("depósito de
  contas") for the commercial register.
- The financial statements are **not openly published per company**. Banco de Portugal publishes only **aggregate
  sector statistics** (Central de Balanços) + a per-company comparison report to the company itself. Structured
  financials at scale therefore need a **commercial provider** or the **paid register**. Currency **EUR**.

## Recommended ingestion

No lawful open bulk/automation path. Options: (a) **manual** lookups — publicacoes.mj.pt (company acts) + Racius
(free basic search); (b) the **paid certidão permanente** for identified register data; (c) a **commercial
provider** (Racius, Informa D&B/einforma, Iberinform) for the register + parsed IES financials at scale. Validate
VAT via **VIES** (= PT + NIPC). Use **dados.justica.gov.pt** for statistics/context.

## Risks / open questions

- **Paid register**: per-company identified data via the paid certidão permanente.
- **Automation blocked**: publicacoes.mj.pt search is reCAPTCHA-protected — must not be bypassed; no open API.
- **Financials not open**: IES statements not published per company (AT/INE/Banco de Portugal).
- **No per-company open bulk**: dados.justica/dados.gov.pt are statistical aggregates only.
- **License**: open datasets are CC-BY-SA / CC-BY; register reuse terms unclear — confirm before redistribution.
- **RCBE** (beneficial ownership) restricted (post-CJEU). Officer/shareholder/owner data = GDPR.
