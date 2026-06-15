# Company data sources for Canada (CA)

## Status

- Official bulk data: **found (open, federal only)** — Corporations Canada "Federal Corporations" CSVs on open.canada.ca.
- Official API: **found** — Corporations Canada real-time API (status, address, directors).
- Open data portal: **found** — open.canada.ca; plus provincial portals (Québec REQ, BC OrgBook).
- License: **known** — **Open Government Licence – Canada (OGL)** for the federal dataset.
- Recommended ingestion path: **bulk CSV (federal) + per-province sources; SEDAR+ for financials**.

## Key fact

Canada has **no single national company register**. Incorporation is split between
the **federal** level (Corporations Canada, under the CBCA) and **13 provinces/
territories**, each with its own corporate registry. The federal open dataset is
excellent but covers **only federally-incorporated** corporations — **many
companies incorporate provincially** and are **not** in it. Comprehensive coverage
requires the provincial registries too (Québec REQ and BC OrgBook are open; others
vary).

## Best source

**Corporations Canada — Federal Corporations** (Innovation, Science and Economic
Development Canada / ISED), on **open.canada.ca**, **OGL**. CSVs split by
active/inactive × CBCA/non-CBCA. The active CBCA business-corporations file alone
is **642,720** corporations. Per record: **corporation number** (federal id),
**Business Number (BN)** (CRA tax id), corporate name (EN + FR), governing
legislation, status, anniversary date, **full registered address**, last annual
filing/meeting, and director counts. Plus a real-time **API** (adds director
names).

## Financial data

**Open only for reporting issuers** (public companies + investment funds) via
**SEDAR+** (sedarplus.ca, the Canadian Securities Administrators' system) — free
access to annual reports, financial statements, and continuous disclosure.
**Private-company financials are not public.** So open structured financials are
essentially **reporting-issuer-only**.

## Next action

Bulk-load the federal CSVs (keyed on **corporation number**, with **BN** as the
tax/join key); add **provincial** registries (Québec REQ, BC OrgBook, …) for
provincially-incorporated companies; pull **SEDAR+** for reporting-issuer
financials. No VAT id — GST/HST registration uses the BN + RT program account.
Director names are personal data — redact.
