# Kazakhstan Company Data Investigation

## Goal

Find official/open sources for Kazakhstani company data: registry, identifiers, status,
activity, director, and listing data, with reproducible access notes.

## What was found

Kazakhstan has a **genuine open company register** on its national open-data portal — the
best open register found in a while, though it is **gated by a free API key**.

1. **data.egov.kz — State Database of Legal Entities (ГБД ЮЛ, `gbd_ul`)** — the national
   open-data portal (Ministry of Digital Development). The dataset `gbd_ul` is the
   **authoritative open register of legal entities**. Its description (verbatim, RU):
   "регистрационные данные юридических лиц, филиалов, представительств Казахстана
   (наименование и дата регистрации юридического лица; идентификационный номер (БИН);
   юридический адрес (место нахождения при регистрации); вид деятельности; фамилия, имя,
   отчество руководителя (при его наличии))" — i.e. **name, registration date, BIN, legal
   address, activity (OKED), and director's full name**. Access is via the data.egov.kz API:
   `https://data.egov.kz/api/v4/gbd_ul/<version>?apiKey=…`. **Verified**: the API returns
   **HTTP 403 `{"error":"API key is required"}`** without a key — a **free API key
   (registration on data.egov.kz)** is required; no key-less bulk file was found. The
   identifier is the **BIN (12-digit Business Identification Number)**. The director's name is
   personal data. No per-company values were captured (no key).

2. **State Revenue Committee (KGD, `kgd.gov.kz`)** — the tax authority. The site hosts
   **taxpayer search** services and publishes lists (VAT payers, inactive / "pseudo-
   enterprise" taxpayers, tax debtors), browser-public and some as downloadable XLSX. Lookup
   by **BIN/IIN** returns taxpayer name and VAT/status. Not a single clean open API; per-
   search or per-list. Complements `gbd_ul` with **tax/VAT status**. Classified
   **useful_secondary_source**.

3. **Kazakhstan Stock Exchange (KASE, `kase.kz`)** — listed companies/securities. Browser-
   public, but the `/en/shares` and `/en/issuers` pages 301-redirect (SPA / trailing-variant)
   and no clean open JSON API was confirmed from static fetches. Listed companies only;
   Kazakhstani ISINs (`KZxxxxxxxxxx`). Classified **useful_secondary_source**.

4. **Bureau of National Statistics (`stat.gov.kz`)** — reachable; publishes statistical
   business-register aggregates and economic statistics, **not** a per-company register.
   Classified **not_company_data**.

## Identifiers

- **BIN (Business Identification Number)** — 12-digit; the company identifier across
  `gbd_ul`, KGD, and government services. The universal Kazakhstani company key. (Individuals
  use the **IIN**.)
- **ISIN** — for KASE-listed securities (`KZxxxxxxxxxx`).

## What was NOT found

- No **key-less** bulk download of the full register (the data.egov.kz API requires a free
  key; no open file mirror found here).
- No clean open JSON API for KASE listed companies (static fetches redirect).

## Conclusion

Kazakhstan is a **recommended / API** country: the **`gbd_ul`** dataset on data.egov.kz is a
real open legal-entities register (BIN, name, date, address, OKED, director), available via
the data.egov.kz API **after registering a free API key**. **KGD** adds tax/VAT status and
**KASE** the listed layer. This is the strongest open register since Taiwan/Pakistan, gated
only by free registration. Nothing was bypassed; no key was obtained, so no data was captured.

## Recommended ingestion approach

API: register a free data.egov.kz API key, then pull `gbd_ul` (JSON/XML/CSV) keyed on **BIN**;
page through versions. Use KGD for tax/VAT status (per-BIN / lists) and KASE for listed
companies. Convert dates; redact the director's name (personal data); confirm the current
dataset version string.
