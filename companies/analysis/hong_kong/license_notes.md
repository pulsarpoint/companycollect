# Hong Kong License Notes

## Companies Registry open data (RNC063, via data.gov.hk)

- Published by the Companies Registry on data.gov.hk. The CKAN record did not expose an
  explicit license id (`license_id` null), so reuse is governed by the **data.gov.hk Terms
  and Conditions of Use** (the standard PSI terms for data.gov.hk), which generally permit
  reuse including commercial use with conditions/attribution. **Confirm the current
  data.gov.hk T&C** before redistribution.
- The feed is **company-level only** — it contains **no personal data** (no directors,
  shareholders, or officers), which simplifies handling.

## Companies Registry ICRIS e-Search

- The full register is provided via ICRIS e-Search; document and full-particulars searches
  are **pay-per-use** under the CR's fee schedule. Treat as **restricted/paid**.
- Full particulars include **directors and company secretary** — natural persons under the
  **Personal Data (Privacy) Ordinance (PDPO, Cap. 486)**; redact in any stored profile.
- Note: the CR has historically restricted access to some director personal particulars
  (e.g. residential addresses / full ID numbers) — handle director data conservatively.

## HKEX List of Securities

- Provided under **HKEX website terms**. The static `.xlsx` URL returns a template; the
  populated list is browser-public. Listed-securities data is regulatory/market information —
  follow HKEX terms for reuse and attribute HKEX.

## data.gov.hk

- Public-sector-information portal under the **data.gov.hk Terms and Conditions of Use**.

## General

- Nothing was bypassed: CR CSVs and the CKAN API are openly accessible; ICRIS pay-per-use
  and the HKEX dynamic list were **not** circumvented.
- Redact any natural-person data (directors, secretary) per the PDPO. The open RNC063 feed
  contains none.
