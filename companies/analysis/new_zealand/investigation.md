# New Zealand Company Data Investigation

## Conclusion

New Zealand has an **authoritative machine-readable entity API (free key)** but
**no free bulk dump**, and **public financials only for FMC reporting entities**:

- **Identity (API, free key):** the **NZBN API**
  (`api.business.govt.nz/gateway/nzbn/v5/entities`), run by the Companies Office
  (MBIE). The **NZBN (New Zealand Business Number, 13-digit GLN)** is the universal
  identifier for every NZ business entity. The API returns the publicly available
  NZBN data (name, type, status, registration date, source register + identifier,
  addresses, trading names, contacts, ANZSIC industry). Requires a **free
  subscription key** — verified HTTP **401** without one.
- **Companies Register:** public per-company search; holds the **company number**,
  directors/shareholders, and **filed documents** (incl. financial statements for
  entities required to file). No free bulk/API found.
- **Financials (subset only):** NZ does **not** require ordinary companies to file
  public financial statements. Only **FMC reporting entities** (issuers, large
  companies, large overseas-owned companies, managed investment schemes) file
  audited statements — public on the **Companies Register** and the **Disclose
  Register** (FMA) as downloadable documents.

## What was verified live

- **NZBN API**: `…/v5/entities?search-term=air new zealand` → HTTP **401**
  `{"message":"Access denied due to missing subscription key…"}`. The NZBN public
  website search also routes through OAuth-gated `api.business.govt.nz`.
- **Companies Register** (200) and **Disclose Register** (200) homepages reachable;
  Companies Register help mentions **no** API/bulk/extract; Disclose is the FMA
  register of FMC offers / managed investment schemes under the FMC Act 2013.
- **data.govt.nz**: bot-protected (Imperva "Pardon Our Interruption") — and does
  not openly host the company register anyway.

## Identifiers

- **NZBN** — 13-digit New Zealand Business Number (a GS1 GLN, starts `9429…`). The
  universal join key across all NZ business registers.
- **Company number** — the Companies Register's own identifier (exposed via the
  NZBN API as `sourceRegisterUniqueIdentifier` when `sourceRegister=COMPANIES`).
- **IRD number** — tax id (Inland Revenue). **Not public.**
- **GST number** = the IRD number for GST-registered entities. **Not public.** NZ
  has **GST, not VAT** — there is no VAT number.

## NZBN entity data (publicly available fields)

`nzbn`, `entityName`, `entityTypeCode`/`Description` (LTD / sole trader /
partnership / trust / government / etc.), `entityStatusCode`/`Description`
(Registered / Removed / In liquidation / …), `registrationDate`, `sourceRegister`
+ `sourceRegisterUniqueIdentifier`, `addresses[]` (registered / service / postal),
`tradingNames[]`, `emailAddresses[]`, `phoneNumbers[]`, `websites[]`,
`industryClassifications[]` (ANZSIC). Some elements (GST numbers, roles/directors)
are restricted and not in the public tier.

## What is NOT openly available

- **Free bulk** of the full register (API per-entity / search only).
- **Financial statements** for ordinary (non-FMC) companies — they don't file.
- **IRD/GST numbers** — not public.
- **Directors/shareholders** in the open API tier — on the Companies Register
  search UI (personal data).

## Recommended ingestion

1. **NZBN API** (free subscription key) — primary entity layer, keyed on NZBN;
   capture the Companies Register company number from `sourceRegisterUniqueIdentifier`.
2. **Companies / Disclose registers** — pull filed financial statements for the
   FMC-reporting subset, joined on NZBN / company number.
3. Treat directors/shareholders as personal data (NZ Privacy Act 2020); redact in
   shared samples.
