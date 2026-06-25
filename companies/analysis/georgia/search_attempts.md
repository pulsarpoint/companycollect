# Georgia Search Attempts

## Attempt 1

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes (official hosts)
- Query: NAPR, enapr, reportal, data.gov.ge, Revenue Service, GSE
- Language: English / Georgian
- Why this query was tried: locate the registry, open-data portal, tax service, exchange.
- Top relevant URLs:
  - https://napr.gov.ge/ → HTTP 200
  - https://enapr.napr.gov.ge/ , reportal.napr.gov.ge , data.gov.ge → connection reset / timeout
  - https://www.rs.ge/ → HTTP 200 ; https://gse.ge/ → HTTP 200
- Result: NAPR main site, RS, GSE up; several subdomains and data.gov.ge reset/firewalled.
- Decision: chase NAPR public-registry search endpoints and reportal.ge.

## Attempt 2

- Date/time: 2026-06-25
- Search engine or source: direct HTTP probes
- Query: enreg.reestri.gov.ge, reestri.gov.ge, api.napr.gov.ge, reportal.ge, RS taxpayer search
- Language: Georgian/English
- Why this query was tried: find the working registry search and any API.
- Top relevant URLs:
  - https://enreg.reestri.gov.ge/main.php → HTTP 200 (search forms)
  - https://reportal.ge/ → HTTP 200 (title "ანგარიშგების პორტალი" = Reporting Portal)
  - https://api.napr.gov.ge/ → HTTP 200 "Access Denied"
  - https://reestri.gov.ge/ → 301 → napr.gov.ge
- Result: enreg is the registry search; reportal.ge is the SARAS reporting portal; NAPR API
  is access-denied.
- Decision: inspect the enreg search form and reportal.ge search.

## Attempt 3

- Date/time: 2026-06-25
- Search engine or source: enreg + reportal forms
- Query: enreg form fields; reportal /en/Reports, /en/Base/Search, /en/Reports/List
- Language: Georgian/English
- Why this query was tried: determine whether search is automatable.
- Top relevant URLs:
  - enreg form: fields include `captcha_validator_field`, `auth_username`, `auth_password`
  - reportal: `POST /en/Base/Search` needs `__RequestVerificationToken`; `GET /en/Reports/List`
    (orgName, year, legalFormId, naceCodes) → 404 for guessed URLs
- Result: NAPR search is CAPTCHA-gated; reportal search is anti-forgery-token-gated.
- Decision: NAPR = blocked_by_authentication (CAPTCHA); reportal = useful_secondary_source
  (browser-public financial statements).

## Attempt 4

- Date/time: 2026-06-25
- Search engine or source: GSE + data.gov.ge
- Query: gse.ge/en/securities; data.gov.ge (http/https)
- Language: English
- Why this query was tried: cover listed companies and re-check the open-data portal.
- Top relevant URLs:
  - https://gse.ge/en/securities → HTTP 200 (32 distinct Georgian ISINs observed)
  - http://data.gov.ge/ → 302 → https://data.gov.ge/ (then SSL hostname mismatch / timeout)
- Result: GSE securities list is browser-public (ISINs); data.gov.ge firewalled/cert-broken
  from this environment.
- Decision: GSE = useful_secondary_source; data.gov.ge = unavailable (from here).
