# Company data sources for Austria

## Status

### Company registry data
- Official bulk data: **not found** (no open bulk of the Firmenbuch)
- Official API: **found but ID-gated** (JustizOnline Firmenbuch API — free, but requires Austrian ID /
  ID Austria to register) + **paid** clearing-house (Verrechnungsstelle) access for bulk/documents
- Open data portal: **found** (data.gv.at) — open company-adjacent data is GISA trade authorizations
- License: **mixed** (GISA/data.gv.at open; Firmenbuch + Jahresabschluss paid/contractual)
- Recommended ingestion path: **paid clearing house / commercial aggregator** for the full company master;
  **open GISA** + free brief Firmenbuch extract for a partial open layer

### Financial data (Jahresabschluss) — PAID
- Official bulk data: **not found** (no open bulk of annual accounts)
- Official API: **paid** — Jahresabschluss documents from the Firmenbuch Urkundensammlung via clearing
  houses (publicly accessible *for a fee*); since 2026 filing is via JustizOnline
- Format: filed documents (PDF; structured electronic filing) — no open machine-readable bulk
- Recommended ingestion path: **commercial aggregator** (Compass, KSV1870, firmafind) or paid per-document
  retrieval via a Verrechnungsstelle

## Best source

There is **no free open per-company master** for Austria. The authoritative source is the **Firmenbuch**
(courts / BMJ), accessed via **paid clearing houses** (Verrechnungsstellen) or, free-but-limited, the
**JustizOnline** brief extract and an **Austrian-ID-gated API**. Annual accounts (**Jahresabschluss**) are
publicly accessible **for a fee**. For free, the practical open layer is the **GISA "Gewerbe in Österreich"**
dataset on data.gv.at (active trade authorizations, no personal data) plus the **free insolvency-gazette**
queries (Ediktsdatei). Financials at scale realistically require a **commercial aggregator** (Compass /
KSV1870 / firmafind) or paid clearing-house access.

## Next action

1. Decide access model: **paid clearing house / aggregator** (full master + financials) vs **open partial
   layer** (GISA trade authorizations + free brief Firmenbuch extract + insolvency gazette).
2. For an open seed: ingest the **GISA "Gewerbe in Österreich"** open dataset (data.gv.at; CSV/JSON, no
   personal data) and the **trade-code list**; enrich via paid Firmenbuch lookups keyed on Firmenbuchnummer/UID.
3. **Financials:** plan paid Jahresabschluss retrieval (clearing house) or an aggregator's financial API.
4. Note: the JustizOnline Firmenbuch API is free but needs **ID Austria**; the structured insolvency feed
   needs an **IWG re-use licence** (the free queries are web-only).

See `investigation.md` for detail and `source_inventory.md` for the table.
