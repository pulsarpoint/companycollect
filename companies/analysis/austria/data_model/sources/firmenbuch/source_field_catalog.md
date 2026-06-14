# Firmenbuch (Companies Register) — Field Catalog

> **PLANNING-ONLY.** The authoritative Austrian company register. Access is **mixed**: a free brief
> extract + an **ID-Austria-gated** JustizOnline API, but full extracts/documents/financials are **paid**
> via clearing houses (Verrechnungsstellen). Fields from public docs + `schema_notes.md`; **no values copied**.

## Source Summary

- Country: Austria
- Source type: official_registry (authoritative spine)
- Organization: BMJ / courts; clearing houses (Compass, KSV1870, HF data, Lexunited, Manz)
- URL: https://justizonline.gv.at/jop/web/firmenbuchabfrage
- License: free brief extract; full paid/contractual; API free but **ID-Austria-gated** → planning-only
- Access: mixed (free brief / paid full / ID-gated API)
- Freshness: authoritative / continuous
- Record shape: per-company Firmenbuchauszug (HTML/PDF; JSON via clearing houses / firmafind)
- Primary keys: `firmenbuchnummer`
- Join keys: `firmenbuchnummer`, `uid`

## Fields

| Path | Source field (DE) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| firmenbuchnummer | Firmenbuchnummer | FN###### + check letter | string | identifier | **PK** |
| firmenwortlaut | Firmenwortlaut | Legal name | string | legal_name | |
| rechtsform | Rechtsform | Legal form | string | legal_form | GmbH/AG/OG/KG/eU |
| uid | UID | VAT id | string | identifier | `ATU########` |
| sitz | Sitz | Registered seat | string | geography | → municipality |
| geschaeftsanschrift | Geschäftsanschrift | Business address | string | address | |
| stammkapital | Stammkapital | Share capital | decimal | financial | register capital, EUR |
| geschaeftszweig | Geschäftszweig | Line of business | string | activity | **free text, not ÖNACE** |
| organe | vertretungsbefugte Organe | Officers | array | person | **PII** |
| status | Status | aufrecht/gelöscht | string | status | + Ediktsdatei insolvency |
| eintragungsdatum | Eintragungsdatum | Registration date | date | date | incorporation |

## Interpretation Notes

- **The authoritative spine, but paid** — modeled planning-only. The free brief extract gives some of these
  fields per company; the **ID-Austria-gated API** gives more (only usable with an Austrian identity); full
  data + documents + financials are **paid** via clearing houses.
- **Identifiers**: **Firmenbuchnummer** (with check letter) is the key; **UID** (`ATU########`) bridges to VAT.
- **No clean activity code**: the register's `Geschäftszweig` is **free text** (no ÖNACE in the public
  extract) — derive/classify, or use the **GISA Gewerbeschlüssel** (open) as a proxy.
- **PII**: officers (Organe) are natural persons — GDPR (paid source).
- No `sample_record.json` (paid/ID-gated; values not retrievable under planning-only terms).
