# Kazakhstan License Notes

## data.egov.kz — State Database of Legal Entities (gbd_ul)

- Published on the national **Open Data Portal** (`data.egov.kz`) under the portal's open-data
  terms — generally reusable, but access to the API requires a **free API key** obtained by
  **registering** on data.egov.kz. Confirm the dataset's specific license/terms on its page
  before redistribution and attribute data.egov.kz / the data owner.
- The dataset includes the **director's full name** (ФИО руководителя) — a natural person
  under Kazakhstan's **Law on Personal Data and its Protection** — redact in any stored
  profile.
- The **BIN**, company name, registration date, address, and activity are public company
  attributes.

## State Revenue Committee (KGD)

- KGD taxpayer search and published lists are public for verification/transparency. Some lists
  are downloadable (XLSX). Treat bulk reuse as **restricted** pending confirmation of terms;
  the lists may include individuals (IIN) — handle personal data per the personal-data law.

## Kazakhstan Stock Exchange (KASE)

- Listed-company information is **public market disclosure**, browser-public. Follow KASE
  terms for reuse and attribute KASE / the issuer.

## Bureau of National Statistics (stat.gov.kz)

- Statistical data under open terms; not a per-company register.

## General

- Nothing was bypassed: the data.egov.kz API key requirement was respected (no key obtained,
  no data captured); KGD/KASE were not scraped.
- Redact natural-person data (director names; individual taxpayers) per the personal-data law.
- The **BIN** is a public company identifier, not personal data.
