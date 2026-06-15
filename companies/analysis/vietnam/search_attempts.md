# Vietnam — Search Attempts

## Attempt 1
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Vietnam national business registration portal dangkykinhdoanh.gov.vn company data API bulk download open data mã số doanh nghiệp`
- Result: NBRP (MPI/Business Registration Authority) offers free per-company search (name, enterprise code = tax code 10–13 digits, address, business lines, legal rep, status). **No documented open API / bulk.** Vietnamese-only.
- Decision: confirm no open bulk; check financials + open-data portal.

## Attempt 2
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `Vietnam company financial statements listed companies disclosure HOSE HNX SSC báo cáo tài chính open data API`
- Result: only **listed companies** disclose audited statements via HOSE/HNX/SSC (CISM/ECM). No comprehensive open financial API; non-listed not published.
- Decision: financials = listed-only.

## Attempt 3
- Date/time: 2026-06-15
- Source: WebSearch
- Query: `data.gov.vn doanh nghiệp dataset open data Vietnam enterprise GSO statistical business register`
- Result: data.gov.vn / open.data.gov.vn publish **no enterprise-registration dataset** (construction prices, cultural heritage, …). GSO VES = statistical survey, access-controlled.
- Decision: no open company bulk on the national portal.

## Attempt 4
- Date/time: 2026-06-15
- Source: curl (reachability)
- Query: HEAD/GET dangkykinhdoanh.gov.vn, tracuunnt.gdt.gov.vn, masothue.com
- Result: NBRP 302→200; GDT 200; masothue (aggregator) 200. NBRP/GDT searches are **CAPTCHA-gated**; not bypassed.
- Decision: NBRP per-company search documented as gated; no automated query run.

## Attempt 5
- Date/time: 2026-06-15
- Source: curl (open.data.gov.vn package_search) + NBRP landing page
- Result: open.data.gov.vn API not reachable for a company query (HTTP 000); NBRP landing page captured (no open-data/bulk/download links). Confirms no open enterprise dataset.
- Decision: classify Vietnam as portal-gated, no open bulk; financials listed-only.
