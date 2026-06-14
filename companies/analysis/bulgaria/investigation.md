# Bulgaria — Company Data Investigation

Date: 2026-06-14
Investigator: company-open-data-discovery skill (Claude)
Country code: BG | Languages: Bulgarian (Cyrillic), English

## Goal

Find official/reliable public sources for Bulgarian company data — prioritising bulk-ingestible open data
and **financial data** (ГФО) — and leave a reproducible trail with samples and licensing.

## Summary of findings

Bulgaria is **partial-open**: the **company register** is open-ish (free public search + **CC-BY daily
publications** on data.egov.bg + an official web service behind registration; a full bulk needs a
data-sharing agreement). The **financials (ГФО)** are **public but document-based** (PDF in the register),
**not** structured open data.

### 1. Търговски регистър (Commercial Register / ТРРЮЛНЦ) — authoritative
- Publisher: **Агенция по вписванията** (Registry Agency), under the Ministry of Justice. Official.
- The Commercial Register & Register of Non-Profit Legal Entities — keyed on the **ЕИК** (Unified
  Identification Code).
- Access:
  - **Free public search** — `https://portal.registryagency.bg/CR/en` — by name, EIK, legal form, status,
    registered seat, management body. No account for single lookups.
  - **Official web service / API** — for system integration; typically needs **registration/contract**
    (an administrative e-service). Single company lookups are unrestricted; **bulk extraction for a
    commercial database requires a data-sharing agreement**.
  - **Open data** — the Registry Agency's **daily publications** are published on **data.egov.bg** under
    **CC-BY** (the "Търговски регистър" dataset, dozens of files). This is an **event/publication stream**
    (registered acts/changes per day) — reusable CC-BY, but not a single clean full master snapshot.
- Fields (public search / publications): EIK, наименование (name), правна форма (legal form), седалище и
  адрес (seat/address), статус (status), предмет на дейност (object of activity), управители/съвет
  (managers/board), капитал (capital).

### 2. ГФО — Annual Financial Statements — public but DOCUMENT-based
- **Годишни финансови отчети (ГФО)** are filed to the Commercial Register and must be **public by 30 June**
  of the following year (aligned with the tax-return deadline).
- Accessible as **filed documents (mostly PDF/scanned)** via the register — **not** a structured open bulk
  (no XBRL/CSV open data like Belgium or Poland). Each document contains the **balance sheet (баланс)** and
  **income statement (отчет за приходите и разходите / ОПР)**.
- Extracting figures requires **OCR/PDF parsing**, or a **commercial provider** (CompanyBook, APIS) that
  already parses balance sheets + income statements (typically 2022+).

### 3. data.egov.bg — national open data portal (CC-BY)
- Bulgaria's open-data portal; hosts the Registry Agency daily publications and many other datasets, in
  machine-readable formats under per-dataset licenses (Registry Agency = **CC-BY**).
- API exists (api_key for resource data). **WAF-blocked from automated access in this environment** (HTTP
  403 regardless of User-Agent) — resolve resources via the portal UI / with an api_key.

### 4. CompanyBook.BG + commercial — third-party
- **CompanyBook.BG** — free **non-financial** company data + a REST API; the **full dataset on request**;
  **financials are a paid subscription** (balance sheets, income statements, ratios 2022+).
- **APIS Register+ / Регистър API** — commercial integration of register + financial data.

### 5. Other official
- **Регистър БУЛСТАТ** (bulstat.bg) — for non-traders / other entities (also EIK/Булстат numbers).
- **Регистър на действителните собственици** (beneficial ownership) — filed within the commercial register;
  access conditions apply.
- **НСИ** (National Statistical Institute) — statistical business register (aggregate).
- **НАП / VIES** — VAT validation.

## Conclusion

- **Registry**: open-ish — free public search + **CC-BY daily publications** (data.egov.bg) + official web
  service (registration); full bulk by **data-sharing agreement**.
- **Financials**: **public but document-based** (PDF) — parse the ГФО or use a commercial provider for
  structured figures. No open structured bulk.
- **Join**: single **ЕИК** (= VAT root). Bulgaria sits between the fully-open group (BE/PL/NO/FR) and the
  paid-register group (DE/AT/IT): registry openness via CC-BY, but financials are document-based.

## Risks / open questions

- **data.egov.bg automated access blocked here** — resolve the Registry Agency dataset's resource URLs via
  the portal UI / api_key; confirm the exact files (daily publications vs full snapshot) + columns.
- **Full bulk needs a data-sharing agreement**; the CC-BY daily publications are a change stream (build a
  master by accumulating them) rather than a single master file.
- **Financials are PDFs** — structured figures need OCR/parsing or a paid provider; no XBRL.
- **License**: Registry Agency open data = CC-BY (attribution); confirm before redistribution.
- **Identifiers**: ЕИК (9-digit; 13 for branches); VAT = `BG` + EIK; Булстат for non-traders.
- Could not pull a per-company open sample here (data.egov.bg WAF; web service registration) — documented;
  field structure from the public search + publications is well known.
