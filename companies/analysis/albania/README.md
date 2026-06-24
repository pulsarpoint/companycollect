# Company data sources for Albania

## Status

- Official bulk data: **partial** — QKB commercial register republished as open data (Open Data Albania); per-company on QKB
- Official API: per-company extract search (QKB); Open Data Albania has lists/pages
- Open data portal: **found** (opendata.gov.al; opencorporates.al)
- License: open data (Open Data Albania) / official register (QKB)
- Recommended ingestion path: **Open Data Albania** (opencorporates.al) lists + QKB per-company extract

## Best source

Albania's commercial register is the **QKB (Qendra Kombëtare e Biznesit / National
Business Center)**. Every entity is keyed by its **NIPT/NUIS** (Numri i
Identifikimit për Personin e Tatueshëm — letter + 8 digits + letter, e.g.
`K12345678L`). The register is republished as **open data** on **opencorporates.al**
(Open Data Albania / AIS) with company lists and per-company pages (name, NIPT,
administrator, owners, capital, activity, status, former names); QKB
(`qkb.gov.al`) provides the official per-company extract (ekstrakt).

Verified live: extracted **4,459 NIPTs** from one Open Data Albania company list —
e.g. `L67508702G` NEXUS GROUP, `L61307015S` MALESIA TRAVEL, `L61306025U` MEDIAL.

## Financial data

Albanian companies file annual financial statements (**bilanci/pasqyrat
financiare**) with the QKB. Open Data Albania publishes some financial indicators;
the authoritative statements are with QKB. No clean open bulk financial dataset.

## Identifiers & tax

- **NIPT/NUIS** — the unique business identifier; it is the **company id, the tax
  id, and the VAT id** (Albania has VAT; the NIPT serves as the VAT number).
  Format: letter + 8 digits + letter.

## Next action

Ingest the Open Data Albania (opencorporates.al) company lists + per-company pages
keyed on NIPT; use the QKB official extract for authoritative detail; redact
administrator/owner personal data. Sample uses real QKB/Open Data Albania records.
