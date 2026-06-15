# EDR Full Register (with Address/KVED) — Restricted (Wartime) Field Catalog

> **PLANNING-ONLY / RESTRICTED.** The pre-2022 full EDR export contained
> registered **addresses** and **KVED** activity codes; these were **removed from
> the open export for security since 2022**, and the full register
> (usr.minjust.gov.ua) is access-restricted. Cataloged to document the gap; no
> records retrieved.

## Source Summary

- Country: Ukraine
- Source type: official_registry
- Organization: Ministry of Justice of Ukraine
- URL: https://usr.minjust.gov.ua/
- License: restricted
- Access: restricted (wartime)
- Freshness: real-time
- Record shape: planning-only
- Primary keys: `EDRPOU`
- Join keys: `EDRPOU`

## Fields (the open-export gaps)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| registered_address | ADDRESS | Registered address | string | address | planning-only; removed from open export |
| activity_kved | KVED | Activity code (КВЕД ≈ NACE) | array | activity | planning-only; removed from open export |

## Interpretation Notes

- Documents the **two fields the open EDR export lacks** (address, KVED) due to the
  wartime data reduction. Both are **restricted** — do not assume open
  availability. If access is later restored/granted, they join the open data on
  **EDRPOU**.
