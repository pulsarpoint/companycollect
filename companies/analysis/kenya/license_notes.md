# License & terms — Kenya

## Summary

NSE listed disclosures are **public**; BRS company search/documents are **paid** (via
eCitizen); opendata.go.ke has no accessible company dataset. Treat BRS
reuse/redistribution as **restricted**.

## Per source

### NSE (`nse.co.ke`)
- Listed-company directory, announcements, and financial results are **public**
  (mandatory disclosure). Attribution to NSE / issuer. Note: **real-time market data**
  is a **paid** product per a published pricelist — use the public directory /
  announcements, not the paid feed. Listed companies only.

### BRS via eCitizen (`brs.ecitizen.go.ke`)
- Official registrar. Search + documents (**CR12**, status report, certified extract,
  annual returns) are **paid per transaction** behind an **eCitizen login**. No open
  bulk/API; no stated bulk-reuse rights. Do not bypass the login/paywall. Field model
  from public knowledge — **no real values copied**.

### Kenya Open Data (`opendata.go.ke`)
- National open-data portal under an open-government license where datasets exist
  (attribution to the publisher), but no accessible company-register dataset was
  found.

## Personal data

BRS **CR12** documents expose **directors and shareholders** — personal data under
the **Kenya Data Protection Act, 2019**. These must be **redacted** in committed
outputs. Because BRS data is paid/gated, **no per-company BRS values were captured**;
the sample uses **NSE-verified + public-knowledge listed companies** with **null BRS
identifiers** (nothing fabricated).

## Practical guidance

- Use **NSE** for listed companies (open); buy **BRS** documents (search/CR12/status
  report) per company via eCitizen for the rest.
- Do not bypass the eCitizen login/paywall; do not use the paid NSE market-data feed
  without a licence.
- Currency **KES**; English; dates dd/mm/yyyy.
