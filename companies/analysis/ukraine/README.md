# Company data sources for Ukraine (UA)

## Status

### Company registry data
- Official bulk data: **found (open)** — the **EDR** (Unified State Register) is published as open bulk on data.gov.ua.
- Official API: **partial** — bulk ZIP/XML is the primary route; per-company lookups via Diia/EDR services.
- Open data portal: **found** — data.gov.ua (Ministry of Justice publisher).
- License: **known** — **CC-BY 4.0** (Creative Commons Attribution).
- Recommended ingestion path: **bulk XML (UO.zip), keyed on EDRPOU**.

### Financial data
- Official open data: **found** — securities-issuer financial statements via **NSSMC / SMIDA**
  (stockmarket.gov.ua), and **IFRS reporters' financial statements in XBRL** via the Financial
  Reporting System (open, integrated to XBRL International). Not every SME publishes openly.

## Best source

**EDR — Єдиний державний реєстр юридичних осіб, ФОП та громадських формувань** (Ministry of Justice),
on **data.gov.ua**, **CC-BY 4.0**, refreshed weekly. The legal-entities file (`UO.zip`, 325 MB →
3.1 GB XML, **2,008,750** entities) is one of the most open registers in the world: **EDRPOU** code,
name, legal form (OPF), status, **founders**, **beneficial owners (UBO)**, **officers (signers)**,
**authorized capital**, registration/termination, and tax-payer registrations — all open.

**Important wartime caveat:** since 2022 the public EDR export is **reduced** — it **no longer
contains registered addresses or KVED activity codes** (and some personal details), removed for
security. The data carries **personal data** (founder/officer/beneficiary names) — handle per
data-protection law.

## Next action

Bulk-load `UO.zip`, stream the `<SUBJECT>` XML records (windows-1251), key on **EDRPOU**, and redact
person names where appropriate. For financials, pull issuer statements from NSSMC/SMIDA and IFRS
XBRL filings; expect coverage limited to larger/IFRS/issuer companies.
