# Search attempts — Indonesia

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: GET `ahu.go.id`, `oss.go.id`, `idx.co.id`, `data.go.id`, `pajak.go.id`
- Language: Indonesian, English
- Why: locate the legal-entity registry, NIB issuer, exchange, open-data portal, tax
- Result: ahu 000 (timeout); oss 307→200; idx 403; data.go.id 200; pajak/ereg 000
- Decision: pursue AHU, OSS, IDX, Satu Data

## Attempt 2
- Date/time: 2026-06-24
- Source: AHU + DNS
- Query: GET ahu.go.id (browser UA); `host ahu.go.id`
- Result: **DNS resolves** (`103.200.129.129`) but HTTPS **times out** → network block
  from this environment; profiles paid (PNBP)
- Decision: document AHU from public knowledge; mark blocked (geo + paid)

## Attempt 3
- Date/time: 2026-06-24
- Source: `data.go.id` (Satu Data Indonesia)
- Query: home + CKAN-style `/api/3/action/package_search?q=perusahaan`
- Result: portal works but lists **regional/sectoral statistics**, no company
  register; CKAN API not at standard path (404)
- Decision: Satu Data = useful_secondary (statistics), not a register

## Attempt 4
- Date/time: 2026-06-24
- Source: `idx.co.id` listed-company API
- Query: `GET /primary/ListedCompany/GetCompanyProfiles`
- Result: **HTTP 403 Cloudflare "Attention Required"** — public via browser but
  Cloudflare-gated for automation; not bypassed
- Decision: IDX = listed financials, browser-only (Cloudflare)

## Attempt 5
- Date/time: 2026-06-24
- Source: `oss.go.id` (OSS / BKPM)
- Query: home; `informasi/statistik`; `/id/pencarian`; NIB search/API guesses
- Result: SPA loads; offers "Cari NIB" + `/id/pencarian`; direct data endpoints 404
  (SPA routing); no open bulk register; per-company JS search
- Decision: OSS = NIB issuer, per-company (useful)

## Attempt 6
- Date/time: 2026-06-24
- Source: financial data (private)
- Query: open private-company financial statements
- Result: none open — LKTP filed with Ministry of Trade, not public; only IDX
  (listed)
- Decision: record private financials as not available openly
