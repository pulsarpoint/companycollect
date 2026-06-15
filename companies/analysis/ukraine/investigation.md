# Ukraine Company Data — Investigation

## Conclusion

Ukraine has a **genuinely open company register** and **open financial data** for
larger/issuer/IFRS companies:

- **EDR** (Єдиний державний реєстр юридичних осіб, фізичних осіб-підприємців та
  громадських формувань) — the Unified State Register (Ministry of Justice),
  published as open bulk on **data.gov.ua** under **CC-BY 4.0**, refreshed weekly.
  The legal-entities file `UO.zip` (325 MB → 3.1 GB XML) holds **2,008,750**
  entities with EDRPOU, name, legal form, status, founders, **beneficial owners**,
  officers, authorized capital, and registration/termination history.
- **Financials**: securities-issuer statements via **NSSMC / SMIDA**
  (stockmarket.gov.ua / smida.gov.ua), and **IFRS reporters' financial statements
  in XBRL** via the Financial Reporting System (open, now integrated to XBRL
  International). Coverage skews to larger/IFRS/issuer companies, not every SME.

## Identifiers

- **EDRPOU** (ЄДРПОУ) — 8-digit code for legal entities; the company id and
  universal join key.
- **РНОКПП / ІПН** — individual tax number (for FOP / persons).
- VAT: Ukraine has **no separate VAT number** — VAT payers are identified by
  EDRPOU in the tax registers (see `TAX_PAYER_TYPE` in EDR).
- **KVED** — activity classifier (≈ NACE) — **NOT in the current open EDR export**
  (wartime reduction).

## Sources found

### 1. EDR — legal entities (UO.zip) on data.gov.ua — RECOMMENDED
- Dataset `https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7`
  (resources under package `03cc1239-…`). **CC-BY 4.0**, weekly.
- `UO.zip` → `UO.xml` (windows-1251). Record shape `<SUBJECT>`:
  `RECORD, NAME, SHORT_NAME, OPF (legal form), EDRPOU, STAN (status),
  FOUNDERS/FOUNDER, BENEFICIARIES/BENEFICIARY (UBO), SUPERIOR_MANAGEMENT,
  SIGNERS/SIGNER (officers), AUTHORIZED_CAPITAL, REGISTRATION, BRANCHES,
  TERMINATION_STARTED_INFO, BANKRUPTCY_READJUSTMENT_INFO, PREDECESSORS, ASSIGNEES,
  TERMINATED_INFO, EXCHANGE_DATA/EXCHANGE_ANSWER (TAX_PAYER_TYPE/START_DATE/…)`.
  **2,008,750** records (verified). XSD: `UO_schema.zip`.
- Companion: `FOP.zip` (individual entrepreneurs), `FSU.zip` (civic
  formations/divisions), each with a schema zip.
- **Wartime reduction**: the current open export has **no registered address** and
  **no KVED** element (removed for security since 2022). Founder/officer/UBO
  **person names** are present → **personal data**.

### 2. EDR — FOP (individual entrepreneurs) — useful_secondary
- `FOP.zip` — sole traders keyed on the individual tax number. Personal data.

### 3. NSSMC / SMIDA — securities-issuer financial statements — useful_secondary (open)
- `https://stockmarket.gov.ua` (NSSMC disclosure DB) and `https://smida.gov.ua`
  (SMIDA). Issuers' financial statements + disclosures. Open; covers listed/issuer
  companies.

### 4. Financial Reporting System (XBRL) — IFRS reporters — RECOMMENDED for financials
- Ukraine mandates **XBRL** for IFRS reporters (full rollout by Nov 2025) via a
  single Financial Reporting Collection Centre; the data is **open** and integrated
  to **XBRL International** (filings.xbrl.org). The structured route to balance
  sheets / income statements for IFRS reporters. (Exact open-bulk endpoint to be
  confirmed at implementation; cataloged from official NSSMC/XBRL sources.)

### 5. EDR full register (with address) — restricted
- The pre-2022 full export (with addresses, full founder details) is **access-
  restricted** during wartime. Not used; the reduced open export is authoritative
  for what is currently public.

## What was NOT bypassed

- Only the open data.gov.ua CC-BY resources were downloaded. No restricted full
  register, no paid aggregators (YouControl/Opendatabot) accessed. Person data
  redacted in committed samples.

## Recommended ingestion

Bulk-load `UO.zip`, stream `<SUBJECT>` records (windows-1251), key on **EDRPOU**,
redact PII as needed. Add financials from NSSMC/SMIDA + IFRS XBRL for the
companies that publish. Expect **no address / no KVED** in the open register.
