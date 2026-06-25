# Armenia Company Data Investigation

## Goal

Find official/open sources for Armenian company data: registry, identifiers, status,
directors, and listing data, with reproducible access notes.

## What was found

Armenia has a reputation for open data, but in practice the **authoritative company register
is bot-protected** from this environment and the national open-data portal did not resolve.

1. **State Register of Legal Entities (`e-register.am` / `e-register.moj.am`)** — Ministry of
   Justice; the **authoritative** company/legal-entities register with a free public search.
   From this environment the register is protected by **Radware Bot Manager**:
   `www.e-register.am/en` and `.../companies` redirect to `validate.perfdrive.com` (bot
   validation), and `e-register.moj.am` redirects to `/hy/`. **No open bulk or free API.** The
   registry key is the **state registration number** plus the **TIN (ՀՎՀՀ / HVHH, 8-digit)**.
   Director/founders are personal data. Classified **blocked_by_authentication** (bot
   protection); not bypassed.

2. **State Revenue Committee (SRC, `src.am`) — taxpayer search** — the SRC site hosts a public
   **taxpayer search** (the `/en/search` page is a large search interface; internal endpoints
   include `/searchTaxpayerData`, `/singleSearchResult`). It looks up a taxpayer **name** and
   **status** (and VAT status) by **TIN (ՀՎՀՀ)** / name — **per-TIN AJAX**, browser-public,
   **not** a bulk download or documented open API. Identifier = **TIN (HVHH)**. Classified
   **useful_secondary_source**.

3. **Armenia Securities Exchange (AMX, `amx.am`)** — listed securities. The site is a
   **JavaScript SPA**: static fetches return an empty ~3 KB shell, and no clean public JSON
   API was found (guessed `api.amx.am` / `amx.am/api/instruments` returned 404 or the shell).
   Browser-public but not cleanly automatable from here. Listed securities only; small market
   (equities + bonds; Armenian ISINs `AMxxxxxxxxxx`). Classified **useful_secondary_source**.

4. **Open Data Armenia (`data.opendata.am`)** — a **civic CKAN portal**. `package_search`
   works, but a `q=company` search returns 10 **research/survey/sectoral** datasets (IFC
   projects, enterprise survey, drug-expertise centre, MIX Market) — **not a company
   register**; `legal entities` / `juridical` / `petakan` searches return **0**. **Not** an
   authoritative register. Classified **not_company_data**.

5. **data.gov.am** and **petakamutner.am** — **do not resolve** (NXDOMAIN / firewalled from
   this environment).

## Identifiers

- **TIN / ՀՎՀՀ (HVHH)** — 8-digit taxpayer identification number; shared by the SRC and the
  State Register; the universal Armenian company key.
- **State registration number** — State Register identifier.
- **ISIN** — for AMX-listed securities (`AMxxxxxxxxxx`).

## What was NOT found

- No open **bulk** company file or free **API** (State Register bot-protected; data.gov.am
  unresolved; AMX SPA; SRC per-TIN only).
- No open directors/beneficial-ownership dataset.
- No company-register dataset on the civic open-data portal.

## Conclusion

Armenia is, from this environment, a **browser-public / gated** country rather than the open
register its reputation suggests: the authoritative State Register is **Radware bot-
protected**, the national portal (`data.gov.am`) did not resolve, AMX is a SPA, and the civic
portal carries no register. The practical sources are the **SRC taxpayer search** (by TIN) and
**AMX** (listed), both browser-public; State Register search is bot-gated. Nothing was bypassed
or fabricated.

## Recommended ingestion approach

Manual/browser-public lookup. Company identity/status via the State Register search (bot-
protected — do not bypass); tax identity via the SRC taxpayer search by TIN; listed companies
via AMX. Re-check `data.gov.am` from another network. The universal key is the **TIN (ՀՎՀՀ)**.
Redact directors/founders (personal data).
