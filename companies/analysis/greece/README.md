# Company data sources for Greece

## Status

- Official bulk data: **not found** (no open GEMI bulk export)
- Official API: **found but blocked for automation** (GEMI `/api` is undocumented, rate-limited, reCAPTCHA-protected; AADE web service needs registered credentials)
- Open data portal: **found** (data.gov.gr — but statistical, not the company register)
- License: **unclear** (public register, reuse/redistribution terms not stated)
- Recommended ingestion path: **manual review / per-entity lookup**; a commercial provider for structured financials at scale

## Best source

The authoritative register is **GEMI** (Γενικό Εμπορικό Μητρώο, General Commercial Registry), searchable free
at **businessportal.gr** (EN) / **publicity.businessportal.gr** (EL), keyed on the **GEMI number** and the
**ΑΦΜ** (AFM, 9-digit tax/VAT id). It holds company identity, legal form, status, address, ΚΑΔ activity,
directors, and filed **financial statements**. But:

- The underlying `/api` is **undocumented**, **rate-limited** (verified HTTP 429) and **reCAPTCHA-protected**
  (verified token in page HTML) → **automated/bulk access is blocked; do not bypass**.
- There is **no official open bulk export**.
- **Financial statements** (ισολογισμοί / οικονομικές καταστάσεις) are published per company as **PDF
  documents** — not structured open data.
- **AADE**'s company-data web service (RgWsPublic) needs **registered TaxisNet credentials**.

So Greece is a **partial-open** country: free manual lookups, but blocked for lawful automated bulk ingestion.

## Next action

For lawful automation, pursue either (a) registered AADE RgWsPublic credentials (per-ΑΦΜ tax-side basic data),
or (b) a commercial provider (ICAP/CRIF, Kyckr) for structured financials at scale. Confirm GEMI reuse terms
before any redistribution. Use Diavgeia / procurement as open ΑΦΜ↔name cross-references.
