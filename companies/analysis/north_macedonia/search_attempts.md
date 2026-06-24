# Search attempts — North Macedonia

## Attempt 1
- Date/time: 2026-06-24
- Source: direct probe of candidate official hosts
- Query: GET `crm.com.mk`, `e-submit.crm.com.mk`, `data.gov.mk`, `ujp.gov.mk`
- Language: Macedonian, English
- Why: locate the company register, distribution service, open-data portal, tax authority
- Result: crm.com.mk / data.gov.mk → HTTP 000 (timeout); ujp.gov.mk → 502
- Decision: retry with browser UA / longer timeout

## Attempt 2
- Date/time: 2026-06-24
- Source: same hosts, browser UA, 30s timeout
- Query: GET crm.com.mk (mk/http/apex), crm.com.mk root, data.gov.mk, e-submit login
- Result: all .mk hosts still HTTP 000 (curl exit 28); ujp.gov.mk 502
- Decision: check DNS vs TCP

## Attempt 3
- Date/time: 2026-06-24
- Source: DNS + CKAN API + EU e-Justice
- Query: `host www.crm.com.mk`; `data.gov.mk/api/3/action/package_list`; e-Justice BRIS page
- Result: **DNS resolves** (`crm.com.mk` → `92.55.95.145`) but TCP/HTTP **times out**
  → network-level block from this environment; CKAN 000; e-Justice page 404
- Decision: document the CRM model from established public documentation; mark
  environment block; do not fabricate values

## Attempt 4
- Date/time: 2026-06-24
- Source: financial data / identifiers (public documentation)
- Query: CRM Registry of Annual Accounts; ЕМБС/ЕДБ/ДДВ identifiers
- Result: financials filed with CRM (баланс/успех, MKD) via paid distribution;
  identifiers confirmed from public docs
- Decision: classify CRM as official register (free basic search + paid bulk/financials)
