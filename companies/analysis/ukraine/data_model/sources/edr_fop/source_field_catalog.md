# EDR — Individual Entrepreneurs (FOP) Field Catalog

> **PERSONAL DATA.** The whole FOP record concerns a natural person (sole trader).
> Open (CC-BY 4.0) but treat as personal data; cataloged from the dataset, not
> fully parsed here (no `sample_record.json`).

## Source Summary

- Country: Ukraine
- Source type: official_registry
- Organization: Ministry of Justice of Ukraine via data.gov.ua
- URL: https://data.gov.ua/dataset/a1799820-195b-4982-8141-6e84f58103e7 (FOP.zip)
- License: CC-BY 4.0
- Access: public
- Freshness: weekly
- Record shape: windows-1251 XML; one record per sole trader
- Primary keys: `fop_id`
- Join keys: (none to legal entities)

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| NAME | NAME (П.І.Б.) | Person name | string | person | PII — redact |
| STAN | STAN | Status | string | status | |
| KVED | activity | Declared activities | array | activity | may be reduced |

## Interpretation Notes

- **Sole traders (ФОП)** — a separate entity stream from legal entities; **not
  joined into the company (legal-entity) profile** by default. The entire record
  is **personal data** (a natural person) — redact/minimise and have a lawful
  basis before persisting. Same wartime reductions apply.
