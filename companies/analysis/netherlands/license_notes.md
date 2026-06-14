# Netherlands — License & Terms Notes

> The KvK open datasets (basic company data + annual accounts) are CC-BY 4.0 but anonymised; identified data is
> paid (KvK API) or restricted (UBO). Personal data is governed by GDPR regardless of licence.

## KvK Open Data Sets (basis bedrijfsgegevens + jaarrekeningen) — CC-BY 4.0
- Both datasets are **EU High-Value DataSets (HVDS)** published under **Creative Commons Attribution 4.0
  (CC-BY 4.0)**. Reuse (incl. commercial) is allowed **with attribution** to the KvK.
- Available as **bulk downloads** (kvk.nl) and via the **HVDS open-data API** (opendata.kvk.nl). The API needs a
  **free API key** and is rate-limited (HTTP 429 observed without one).
- **Anonymised**: the bulk strips the KvK number, name, address and directors — only registration date, legal
  form, postcode region, SBI codes (basis) and balance-sheet figures + year (jaarrekeningen). The dataset notes
  it is "bedoeld voor hergebruik" (intended for reuse) and gives no legal guarantee.

## KvK Handelsregister API (Zoeken / Basisprofiel / Vestigingsprofiel / Naamgeving)
- The route to **identified** data (names, addresses, officers, KvK number). Requires an **API key + paid
  subscription** (free test environment). Use under the KvK API terms; KvK register data carries reuse
  restrictions (it is not CC-BY).

## Jaarrekeningen
- The open jaarrekeningen dataset is **CC-BY 4.0** structured financial data (XBRL-derived). Currency EUR.
  Anonymised in bulk; identified via the HVDS API by KvK number.

## UBO-register
- General public access was **suspended after the Nov 2022 CJEU ruling**. Access is for **AML-obliged entities**
  (expanded via the KvK API from April 2026). **Not** open. Personal data (GDPR).

## Belastingdienst / VIES
- Validates a supplied NL VAT number (`NL` + 9 digits + `B` + 2). Validation/enrichment only; not redistributable
  as a list.

## data.overheid.nl
- National open-data catalog (CKAN); the KvK datasets are CC-BY 4.0. Per-dataset licence otherwise.

## Personal data / GDPR
- The open datasets are anonymised, but identified data from the paid API / commercial providers (officers,
  shareholders) and the UBO register are **personal data** — apply a GDPR lawful basis + retention; no
  direct-marketing reuse.

## Summary recommendation
- **Free to reuse (incl. commercial) with attribution — CC-BY 4.0**: KvK basis + jaarrekeningen open datasets
  (anonymised; bulk + HVDS API).
- **Paid**: identified data via the KvK Handelsregister API or a commercial provider.
- **Restricted**: UBO (AML-obliged entities).
- **Validation**: VIES for VAT.
- Always **attribute the KvK** for the open datasets.
