# Ediktsdatei / Insolvenzdatei — Field Catalog

> Official insolvency gazette. **Free web queries**; the **structured JSON feed** (iwg.justiz.gv.at) needs
> an **IWG re-use licence** (login wall confirmed). Useful as an **insolvency/status signal**, not a master.
> Fields documented; no values copied (structured feed licensed).

## Source Summary

- Country: Austria
- Source type: official_gazette
- Organization: BMJ
- URL: https://edikte.justiz.gv.at/ (free web); https://iwg.justiz.gv.at/edikte/ (licensed JSON feed)
- License: free web queries; structured feed = IWG re-use licence
- Access: public (web) / licensed (feed)
- Freshness: daily
- Record shape: per-edict (insolvency / auction / register announcement)
- Primary keys: `aktenzeichen + gericht`
- Join keys: `firmenbuchnummer` (when present), `schuldner name`

## Fields

| Path | Source field (DE) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| art | Art des Edikts | Konkurs/Sanierung/Versteigerung | string | filing | filter to insolvency |
| schuldner.name | Schuldner | Debtor name | string | legal_name | may be PII |
| schuldner.firmenbuchnummer | Firmenbuchnummer | Debtor FN | string | identifier | clean join when present |
| gericht | Gericht | Court | string | metadata | |
| aktenzeichen | Aktenzeichen | Case number | string | identifier | case key |
| datum | Datum | Date | date | date | event date |
| verfahrensstatus | Verfahrensstatus | Proceeding status | string | status | distressed flag |

## Interpretation Notes

- **Status/lifecycle signal.** Use insolvency edicts (Konkurs/Sanierungsverfahren) to flag a company as
  **distressed/insolvent** — complements the Firmenbuch `Status` (aufrecht/gelöscht).
- **Join**: clean when the **Firmenbuchnummer** is present in the edict; otherwise match by debtor name.
- **Access**: free per-case **web queries** are open; the **structured JSON feed requires an IWG licence**
  (formal application) — treat the feed as licensed, not open.
- **PII**: debtors can be natural persons — GDPR.
