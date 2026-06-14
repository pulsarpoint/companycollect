# RŽP — Živnostenský rejstřík (trade licensing register) Field Catalog

## Source Summary

- Country: Czech Republic
- Source type: official_registry
- Organization: Ministerstvo průmyslu a obchodu ČR (MPO)
- URL: https://www.rzp.cz/
- License: open/public (confirm terms)
- Access: public
- Freshness: regular
- Record shape: trade-licence records keyed by IČO
- Primary keys: `ico`
- Join keys: `ico`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ico | IČO | Trader id | string | identifier | — | covers OSVČ too |
| zivnosti | živnosti | Trade licences / scope | array | license_or_terms | — | activity detail |
| odpovedny_zastupce | odpovědný zástupce | Responsible representative | object | person | — | PII |

## Interpretation Notes

- The trade licensing register covers **sole traders (OSVČ)** and licensed activities, including entities that
  are not in the Veřejný rejstřík. Its value is **licence/activity detail** beyond NACE, and coverage of
  OSVČ. Reachable via ARES (`stavZdrojeRzp`). The responsible representative is personal data (GDPR).
