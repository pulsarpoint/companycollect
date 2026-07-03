# Czech companies — financials & domain-matching research (starting point)

Status: Czech register/industry/address is live (`czech_ares`, 3.51M companies).
`cz_company_contacts` now captures domain/email candidates embedded in company
names and validates domains against Common Crawl or DNS. Financials are still
deferred in favour of a broader **domain-first** approach (below).

---

## 0. The plan we actually want (domain-first financials)

Pulling financials for all **3,512,259** Czech companies is impractical (see §3 —
per-company scrape + scanned-PDF OCR + LLM). The high-value subset is companies
that **have a web presence**, so the intended pipeline is:

1. **Match existing `.cz` domains → Czech companies** → the (much smaller) set of
   companies that have a domain.
2. **Only for that subset**, pull financials (the per-company scrape + OCR + LLM).

So the next real task is the **domain ↔ company match**, then financials on the
matched subset.

### Matching `.cz` domains to companies — the key question
`cz_companies` has **no structured website/contact field** (ARES/RES expose none —
confirmed, §2). `cz_company_contacts` extracts the subset of domains/emails that
are embedded directly in company names, but broader coverage still needs an
external join key between a `.cz` domain and a company:

- **Best candidate — the CZ.NIC registry holder IČO.** `.cz` domains are managed by
  CZ.NIC; a domain held by a company has a **holder** with an **IČO** (the same
  8-digit Czech company/economic-subject identifier that is stored as `ico` and is
  the key of `cz_companies`). If we can get
  `domain → holder IČO`, the join is exact (`IČO`). **TO VERIFY:** does CZ.NIC
  whois / registry data still expose the holder IČO post-GDPR (org holders may
  still show it; natural-person holders are redacted)? Check the CZ.NIC whois
  (`whois <domain>.cz` / `https://rdap.nic.cz/domain/<domain>.cz`) and whether a
  bulk/zone option exists.
- **Fallback — name / address matching.** domain registrant org name (or the
  website's content) ↔ `cz_companies.name` (fuzzy) + city/postcode. Lower precision.
- **Where do "existing .cz domains" live?** The corpscout domain graph
  (`company_website_domains` / `domains` in ClickHouse) and/or the pulsarprotect
  data already hold domains — start from the `.cz` `root_domain`s there.

### Suggested next steps
1. Pull the distinct `.cz` `root_domain`s we already have (corpscout `domains` /
   `company_website_domains` where `root_domain LIKE '%.cz'`).
2. Resolve each to a holder **IČO** via CZ.NIC RDAP/whois (verify availability +
   rate limits); build a `domain → ico` table.
3. Join to `cz_companies` on `ico` → the companies-with-a-domain subset.
4. Run the targeted financials fetch (§3) only for that subset.

---

## 1. Czech register (DONE — `czech_ares`)
- **Bulk** source: ČSÚ RES open data `res_data.csv`
  (`https://opendata.csu.gov.cz/soubory/od/od_org03/res_data.csv`, ~540 MB,
  stable URL, twice-monthly, no key).
- Landed: **`cz_companies`** (3,512,259; 2,927,411 active; `ico` = Czech IČO
  8-digit company/economic-subject identifier; name, legal form,
  status, established/terminated dates, **address**, size category, sector) and
  **`cz_industries`** (CZ-NACE + NACE2025 → unified NACE, 2.83M mapped → 100% join
  `nace_categories`). Migrations 000038/000039.
- Landed: **`cz_company_contacts`** from company-name text only. It extracts
  embedded domains and email addresses, links rows to `cz_companies.ico`, stores
  `contact_type`, `contact_value`, normalized root `domain`, `domain_source`, and
  `confidence`. `domain_source='commoncrawl'` means the domain exists in
  `commoncrawl_domains` and receives higher confidence; `domain_source='dns'`
  means the domain was absent from Common Crawl but resolved successfully.

## 2. Structured contacts — ABSENT (confirmed)
- Neither the **ARES API** (full record scanned — zero contact keys, no `@`/`http`/
  `www`) nor the **RES bulk CSV** expose email/phone/website. Only the **registered
  address** (already in `cz_companies`) and `dic` (DIČ tax id). Czech company
  websites/emails exist only in commercial sources (Merk, Bisnode/D&B — paid), or
  as unstructured text occasionally embedded in the company name and captured by
  `cz_company_contacts`.

## 3. Financials — PDF-only, per-company (deferred)
- **No structured/XBRL financials, no API.** Statements (*účetní závěrka*) are filed
  as **PDF** in the **Sbírka listin** at `or.justice.cz`. `dataor.justice.cz`
  open-data dumps contain the **register only** (`<Subjekt>`/`<Udaj>`), **not** the
  document collection or financials (grepped a real dump — no Sbírka/účetní/financ
  elements).
- **No bulk for documents — strictly per-company.** Each company = 4 GETs + its own
  PDF. Spiked end-to-end on Asseco (IČO `27074358`, subjektId `157589`):
  1. `https://or.justice.cz/ias/ui/rejstrik-$firma?ico=<ICO>` → detail HTML →
     `subjektId` (regex `subjektId=(\d+)`) + Sbírka-listin link. *(Direct GET by IČO
     works — avoids the stateful Wicket search form.)*
  2. `https://or.justice.cz/ias/ui/vypis-sl-firma?subjektId=<id>` → document list;
     rows link `vypis-sl-detail?dokument=<docId>&subjektId=<id>&spis=<spis>` with
     labels like **"účetní závěrka [2024], výroční zpráva [2024]"** + filing date.
  3. `https://or.justice.cz/ias/ui/vypis-sl-detail?dokument=<docId>&...` → PDF link
     `href="/ias/content/download?id=<hash>"`.
  4. `https://or.justice.cz/ias/content/download?id=<hash>` → the PDF
     (`application/pdf`; Asseco's 2024 závěrka = **17.5 MB, 63 pages**).
- **The PDFs are scanned images** — `pdftotext` got 63 chars from 63 pages. So:
  **`pdftoppm` → `tesseract` OCR → LLM** (reuse `uk_companies_house/pdf_extract.py`).
  - Czech labels for the metric prompt: `tržby`/`výnosy`=revenue, `náklady`=costs,
    `provozní výsledek hospodaření`=operating result, `výsledek hospodaření`=net
    result, `aktiva celkem`=total assets, `vlastní kapitál`=equity, `cizí
    zdroje`/`závazky`=liabilities, `oběžná aktiva`=current assets, `stálá aktiva`=
    fixed assets, `peněžní prostředky`=cash. Currency **CZK**; statements often in
    **thousands** ("údaje v tis. Kč") → capture unit scale.
  - OCR note: this host's `tesseract` has **`eng` only** (no `ces`); install the
    `ces` language pack for cleaner Czech-label OCR (numbers are language-agnostic).
- **Scope:** targeted/on-demand for a provided list of IČOs (→ the domain-matched
  subset from §0). Module `czech_financials` is scaffolded (this doc only); the
  fetch+OCR+LLM build was paused per the domain-first decision.

## 4. Useful endpoints / facts
- ARES per-company API: `https://ares.gov.cz/ekonomicke-subjekty-v-be/rest/ekonomicke-subjekty/<ICO>`
  (rich JSON: name, address, `czNace`/`czNace2008` list, legal form, dates, DIČ).
- RES open data dir base: `https://opendata.csu.gov.cz/soubory/od/od_org03/`.
- Justice open data (register only): `https://dataor.justice.cz/` — CKAN API
  (`/api/3/action/package_list`) + gzipped XML dumps
  (`/api/file/<legalform>-<full|actual>-<court>-<year>.xml(.gz)`).
