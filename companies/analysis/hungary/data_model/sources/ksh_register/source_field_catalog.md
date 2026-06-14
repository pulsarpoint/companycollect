# KSH — Hungarian Central Statistical Office business register Field Catalog

## Source Summary

- Country: Hungary
- Source type: statistical_business_register
- Organization: Központi Statisztikai Hivatal (KSH)
- URL: https://www.ksh.hu/
- License: open (KSH terms)
- Access: public
- Freshness: periodic
- Record shape: statistical register + classifications
- Primary keys: `statisztikai_szamjel`
- Join keys: `adoszam` (via the embedded 8-digit base)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| statisztikai_szamjel | statisztikai számjel | 17-digit statistical code | string | identifier | (not copied) | base+TEÁOR+form+county |
| teaor | TEÁOR | Activity classification | string | activity | (not copied) | NACE-aligned |
| enterprise_demographics | vállalkozás-demográfia | Aggregate stats | object | metadata | (not copied) | not per-company |

## Interpretation Notes

- **Classification authority.** KSH owns the **statisztikai számjel** (17-digit, embedding the 8-digit tax base
  + TEÁOR + legal-form + county codes) and the **TEÁOR** activity classification. Useful to obtain the canonical
  TEÁOR and decode the statistical code; join via the embedded **8-digit base**. KSH also publishes aggregate
  enterprise demographics (not a per-company open master).
