# License & terms — Montenegro

## Summary

The official register (**CRPS**) publishes no open-data license and was inaccessible
at investigation time. The **data.gov.me** datasets are openly published (intended
for reuse) but cover only public enterprises and statistics. Treat CRPS reuse terms
as **uncertain**.

## Per source

### CRPS (`eprijava.tax.gov.me/TaxisPortal`, Uprava prihoda i carina)
- The official company register. No open bulk/API; no stated reuse license. The
  current portal returned **HTTP 503** (down) and the legacy `crps.me` is a parked
  domain. Per-company lookup only (when available). Do not assume redistribution
  rights.

### data.gov.me — Javna preduzeća (Ministry of Public Administration)
- Openly published CKAN dataset (public-enterprises list). Reuse with attribution
  to the publisher / data.gov.me is appropriate. Covers public/state enterprises
  only; no PIB/registration number.

### data.gov.me portal / MONSTAT
- Openly published statistics; reuse with attribution. Aggregate, non-personal.

## Personal data

CRPS exposes **founders/owners (osnivači)** and **authorised representatives
(ovlašćeno lice)** — personal data when natural persons (Montenegro Law on Personal
Data Protection, Zakon o zaštiti podataka o ličnosti). These must be **redacted** in
committed outputs. Because CRPS was unavailable, **no per-company register values
were captured**; the sample uses only public-enterprise (legal-entity) names from
the open data.gov.me dataset.

## Practical guidance

- Realistic access is **per-company CRPS lookup once the portal returns**.
- Do not scrape; do not assume bulk-reuse rights for CRPS.
- data.gov.me public-enterprises + statistics may be reused with attribution.
- Currency **EUR**; dates dd.mm.yyyy.
