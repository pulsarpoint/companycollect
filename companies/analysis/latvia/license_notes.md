# Latvia — License & Terms Notes

> Latvia's company open data — register, structured financial statements, beneficial owners, officers — is
> published under **CC0-1.0 (public domain)**. Reuse (incl. commercial) is unrestricted and **no attribution is
> required**. Personal data is still governed by GDPR regardless of the licence.

## UR Register of Enterprises open data (data.gov.lv) — CC0-1.0
- The Register of Enterprises publishes its datasets as **CC0-1.0 (public domain dedication)** on data.gov.lv:
  the **company register**, **annual-report financial data**, **beneficial owners**, officers (amatpersonas),
  members/shareholders (dalībnieki), equity capital, insolvency, liquidations, reorganizations, historical names
  (~35 datasets). The UR explicitly states open data may be **distributed without restrictions, for commercial
  and non-commercial purposes**, by natural and legal persons.
- Formats: CSV, XLSX, JSON, Parquet, SQLite, PostgreSQL dump (gzip).
- **No attribution required** under CC0 (attribution is courteous but not obligatory).

## Financial statements (gada pārskatu finanšu dati)
- Published as **structured open data** under the same CC0 — report metadata + balance sheets + income
  statements + cash flow statements + employee counts. No payment, no document paywall. Currency EUR (pre-2014
  reports may be LVL).

## Beneficial owners (patiesie labuma guvēji)
- Available as **open bulk CSV** under CC0. Unusual: after the 2022 CJEU ruling several EU states restricted
  public beneficial-ownership access, but Latvia continues to publish it openly.
- **GDPR still applies**: these are personal data (name, birth date, nationality, residence). A lawful basis and
  retention policy are required before persisting, and the data must not be reused for direct marketing. CC0
  governs IP reuse, not data protection.

## VID / VIES
- Latvian VAT (PVN reģistrācijas numurs) = `LV` + the 11-digit regcode. VID publishes VAT-payer information; VIES
  validates a given number. Validation/enrichment.

## data.gov.lv
- National open-data portal (CKAN); the UR datasets are CC0. Per-dataset licence otherwise.

## Personal data / GDPR
- Beneficial owners, officers and members (natural persons) are **personal data** — apply a GDPR lawful basis +
  retention; no direct marketing.

## Summary recommendation
- **Free to reuse (incl. commercial, no attribution) — CC0**: register, structured financials, beneficial
  owners, officers/members/equity/events.
- **GDPR care**: beneficial owners, officers, members.
- **Validation**: VIES/VID for VAT.
