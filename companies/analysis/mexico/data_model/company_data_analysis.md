# Company Data Analysis For Mexico

## Summary

Mexico is a **fragmented** case: there is **no single open legal-entity register**
and **no shared open join key**, and **private-company financials are not public**.
The best open layer is **INEGI DENUE**, the national statistical directory of
**economic units (establishments)** — openly downloadable per-state CSV (no token),
giving trade name, **legal name (`raz_social`)**, SCIAN activity, employee band,
full address, and geolocation. But DENUE is establishment-level and carries **no
RFC and no folio mercantil**, so it cannot be joined to the tax or commercial data
by a shared key (only by name). The example uses real DENUE data.

## Sources Analyzed

| Source slug | Source name | Status | Access | License | Role |
|---|---|---|---|---|---|
| inegi_denue | INEGI DENUE | recommended | public, no token | INEGI libre uso | Open identity/activity/location (establishments) |
| sat_69b | SAT Listado 69-B | useful_secondary | public | public (CFF 69-B) | RFC risk overlay |
| rpc_psm_registry | RPC/SIGER & PSM | blocked_payment | search/per-doc (paid) | restricted | Legal identity (folio, form, capital) |
| bmv_cnbv_listed | BMV / CNBV | planning_only | public (listed) | exchange terms | Listed-company financials |

## What Each Source Contributes

- **inegi_denue** — the open layer: DENUE id + clee, trade name, legal name
  (`raz_social` when present), SCIAN activity, employee band, address + geolocation.
  Verified live (Aguascalientes, 71,871 units, 42 cols). No RFC/folio; no financials.
- **sat_69b** — open RFC risk list (presumed non-existent operations / EFOS), with
  RFC + legal name (incl. corporate-form suffix) + situation. Verified (14,247 rows).
- **rpc_psm_registry** — the authoritative legal registry (folio mercantil
  electrónico, tipo societario, fecha de constitución, capital, objeto social);
  search/per-document, fee-based. Planning-only.
- **bmv_cnbv_listed** — financial statements for **listed** issuers only (IFRS,
  MXN). The only open financial route; private companies have none.

## Proposed Country Company Profile

A single object keyed on `identity.denue_id` (+ clee), with placeholders for the
non-open ids (folio mercantil, RFC):

- `identity` — DENUE id/clee (open) + folio mercantil (paid) + RFC (SAT/name-join).
- `tax_identifiers` — tax_id = vat_id = RFC.
- `legal_identity` — legal name, trade name, legal form (paid/inferred).
- `status` — in_directory (DENUE) + risk_69b (SAT).
- `activity` — SCIAN; `size` — employee band.
- `locations[]` — establishment addresses, geolocated (a company may span several).
- `registry_details` — incorporation/capital/objeto social (paid, planning-only).
- `financial_statements[]` — listed-only (BMV/CNBV, MXN), planning-only.
- `source_provenance[]`.

## Join And Precedence Rules

- **Keys**: DENUE `id`/`clee` (establishment); RPC `folio mercantil`; SAT `RFC`.
  **No shared open key** — DENUE↔SAT↔RPC joins are **name-based** (fuzzy).
- **Precedence**: DENUE (open identity/activity/location) > SAT 69-B (risk + RFC) >
  RPC registry (authoritative legal fields, paid) > BMV/CNBV (listed financials).
- **RFC = tax id = VAT id**; no separate VAT number.

## Missing Or Restricted Data

- **Single open legal-entity register / shared key** — none.
- **Private-company financials** — not public.
- **RFC / folio mercantil in the open layer** — absent.
- **Officers, incorporation date, capital, objeto social** — fee-based registry.
- **Beneficial owners** — not openly published.

## Common Mapper Notes

- Map `company_id` to the DENUE id/clee; treat `registration_number` (folio) and
  `tax_id` (RFC) as not-in-DENUE (paid/name-join).
- Map `financials` only for listed issuers (BMV/CNBV, MXN).
- Treat cross-source links as approximate (name-based).
- Redact DENUE contact fields and individual RFCs/names (LFPDPPP).
