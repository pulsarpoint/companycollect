# License & terms — Thailand

## Summary

The **DBD OpenAPI** is an official open service (no key) — the cleanest reuse story.
DBD DataWarehouse financials are login-gated; data.go.th is openly licensed but
WAF-blocked here. Confirm DBD's published reuse terms before large-scale
redistribution; treat any personal data under Thailand's **PDPA**.

## Per source

### DBD OpenAPI (`openapi.dbd.go.th`)
- Official **open** API (Ministry of Commerce / DBD), no token observed. Returns
  company identity + capital + activity + address. Intended for public consumption;
  confirm DBD's terms of use for bulk/commercial reuse. No personal data
  (directors/shareholders) in this endpoint.

### DBD DataWarehouse (`datawarehouse.dbd.go.th`)
- Company profiles + **financial statements**; **login/session required** (302/403
  for automation). Reuse governed by DBD terms; do not bypass the login.

### data.go.th (DGA, CKAN)
- National open-data portal — datasets generally under an open-government license
  with attribution. Portal/API were **WAF-blocked** for automation from this
  environment; usable from an allowed network.

### SET (`set.or.th`)
- Listed-company disclosures are public (mandatory disclosure). Attribution to SET /
  issuer applies; listed only.

## Personal data

Company **directors and shareholders** are personal data when natural persons under
the **PDPA** (Personal Data Protection Act B.E. 2562). They are **not exposed by the
DBD OpenAPI**; if obtained from DataWarehouse or filings, **redact** in committed
outputs. The open-API sample contains only company-level fields (id, name, type,
status, capital, TSIC, address) for real public companies — safe to keep.

## Practical guidance

- Prefer the **DBD OpenAPI** (open, per 13-digit ID) for identity + capital +
  activity + address.
- Use **DataWarehouse** (login) / **SET** (listed) for full financials.
- Currency **THB**; dates YYYYMMDD in the API; Thai + English names.
