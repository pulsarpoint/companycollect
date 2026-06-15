# Ukraine — License Notes

## EDR (data.gov.ua)

- **License: CC-BY 4.0** (Creative Commons Attribution). Free reuse incl.
  commercial, with **attribution** — credit "Ministry of Justice of Ukraine — EDR
  (data.gov.ua)" and retain source + retrieval date.
- Refreshed **weekly**. Encoding **windows-1251** (decode on ingest).

## PERSONAL DATA (important)

- EDR records contain **person names**: `FOUNDERS` (founders + share), `SIGNERS`
  (officers/directors), `BENEFICIARIES` (beneficial owners), and termination
  signatories. Even though published openly under CC-BY, this is **personal data**
  subject to Ukraine's data-protection law (and GDPR for EU processing).
  - Redact/minimise person-level fields in published profiles/samples.
  - Have a lawful basis and purpose limitation before persisting officer/owner
    identities.
- The **FOP** dataset concerns natural persons (sole traders) — treat the whole
  record as personal data.
- Company-level fields (EDRPOU, name, OPF, status, capital, registration) are not
  personal data for legal persons.

## Wartime data reduction

- Since 2022 the public EDR export is **reduced**: **registered addresses and KVED
  activity codes are removed** (and some personal details). The full register
  (usr.minjust.gov.ua) is access-restricted. Do not assume address/KVED are
  available openly.

## Financial data (NSSMC / SMIDA / XBRL FRS)

- Issuer disclosures (stockmarket.gov.ua / smida.gov.ua) and IFRS **XBRL** filings
  (Financial Reporting System, integrated to XBRL International) are **open**.
  Confirm the precise reuse terms and the open-bulk endpoint at implementation;
  attribute NSSMC / the Financial Reporting Collection Centre.

## Summary

- **Open & usable (attribute)**: EDR UO/FOP (CC-BY 4.0); NSSMC/SMIDA + XBRL FRS financials.
- **Personal-data caution**: founders/officers/beneficial owners; FOP records.
- **Restricted**: full EDR with address/KVED (wartime).
