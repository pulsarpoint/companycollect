# Bangladesh License Notes

## Dhaka Stock Exchange (DSE)

- Listed-company information (company list, per-company profile, capital, sector, listing year)
  is **public market disclosure**, browser-public on `dsebd.org`. No explicit open/reuse
  license was located; treat as **public disclosure** and attribute DSE / the issuer. Confirm
  bulk-reuse terms before redistribution (market data is often subject to exchange terms).
- No personal data in the listed-company profile fields used here.

## RJSC — Registrar of Joint Stock Companies and Firms

- The authoritative registrar. A free **company name search** exists, but document/schedule
  retrieval is **pay-per-use** under the RJSC fee schedule, and there is **no open bulk/API**.
  Treat access as **restricted/paid**; confirm any data-sharing terms with RJSC. The site had
  a TLS intermediate-certificate issue — handle carefully.
- Directors are natural persons under Bangladesh's data-protection norms — redact in any stored
  profile.

## Chittagong Stock Exchange (CSE)

- Listed-company information is **public market disclosure**, browser-public. Follow CSE terms;
  attribute CSE / the issuer.

## National Board of Revenue (NBR)

- BIN / e-TIN verification is public for verification purposes. Bulk reuse is **restricted**;
  individual taxpayer data is personal data — redact individuals.

## data.gov.bd

- National open-data portal (DKAN); statistical datasets under open terms. Not a company
  register.

## General

- Nothing was bypassed: RJSC pay-per-use documents were not purchased/circumvented; the RJSC
  TLS cert issue was only worked around for a read-only reachability check; DSE/CSE/NBR were
  not scraped aggressively.
- Redact natural-person data (directors; individual taxpayers).
- The RJSC registration number, DSE trading code, and BIN are public company identifiers.
