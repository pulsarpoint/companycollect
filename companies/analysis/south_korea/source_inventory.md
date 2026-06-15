# South Korea — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| OpenDART API | Financial Supervisory Service / DART | financial disclosure | free API key | JSON, XML, ZIP | public disclosure (reusable) | blocked_by_authentication (free key) |
| NTS business-status API | National Tax Service via data.go.kr | tax registry | free service key | JSON | KOGL | blocked_by_authentication (free key) |
| IROS court registry | Supreme Court | official registry | paid per-document | HTML, PDF | restricted | blocked_by_payment |
| data.go.kr | MOIS / NIA | open data portal | free service key | JSON, XML, CSV | KOGL | useful_secondary_source |

## Roles

- **opendart_api** — the authoritative open **identity + financials** source for
  DART-registered companies (listed + external-audit). corpCode.xml (entity list),
  company.json (identity incl. 법인등록번호 + 사업자등록번호), fnlttSinglAcnt(All)
  (XBRL financials, KRW). Free key (verified status 900 / 302 without one).
- **nts_business_status** — business-registration **status** (active/suspended/
  closed, tax type) by 사업자등록번호. Free key.
- **iros_court_register** — the full register incl. **unlisted** companies and
  directors/capital/purpose; fee-based per extract.
- **data_go_kr** — the portal hub for non-DART company/tax APIs (free key, KOGL).

## Join keys

`corp_code` within OpenDART; **business registration number (10-digit)** links
OpenDART ↔ NTS; **corporate registration number (13-digit)** links to the court
register. Tax id = business registration number = VAT number (no separate VAT id).
