# OpenMercantil (BORME-derived) — Field Catalog

## Source Summary

- Country: Spain
- Source type: open_data_reconstructed (community/NGO)
- Organization: OpenMercantil
- URL: https://openmercantil.es/ (bulk `/descargas`, API `/api`, per-company `/export`)
- License: **CC BY 4.0** (commercial use permitted with attribution)
- Access: public, no auth
- Freshness: daily (D+1 from BORME); coverage 2009→present
- Record shape: **sample/master = one row per company** (`slug,name,cif,province,first_seen,last_seen,acts_count`); **full bulk CSV = 12 columns, act-level** (`Date, Section, Province, Company Name, CIF, Website, Capital, Address, Workers, Act Type, Details, ID`)
- Primary keys: `slug` (always), `cif` (when present)
- Join keys: `cif`, `slug`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| slug | slug | OpenMercantil slug / fallback id | string | identifier | `mirador-de-bellavista-sl` | PK when cif empty |
| name | name | Legal name (uppercase, form suffix) | string | legal_name | `MIRADOR DE BELLAVISTA SL` | May carry `EN LIQUIDACION` / `(R.M. …)` |
| cif | cif | Spanish tax id (CIF/NIF) | string | identifier | `B12345678` | **~10–18% populated** |
| province | province | Province (registry/seat) | string | geography | `Málaga`, `Balears (Illes)` | Province granularity |
| first_seen | first_seen | First BORME appearance | date | date | `2015-06-03` | Incorporation proxy only |
| last_seen | last_seen | Last BORME appearance | date | date | `2017-07-06` | Recency signal |
| acts_count | acts_count | # linked BORME acts | integer | metadata | `5` | Enrichment depth |
| Date | Date | [full CSV] act date | date | date | — | act-level file |
| Capital | Capital | [full CSV] share capital from act | decimal | financial | — | **register capital, NOT accounts** |
| Workers | Workers | [full CSV] worker count from act | integer | employment | — | sparse |
| Website | Website | [full CSV] website | string | metadata | — | sparse, unvalidated |
| Address | Address | [full CSV] registered address | string | address | — | free text |

## Interpretation Notes

- **Two shapes.** The downloadable **samples and company master** are one row per company
  (7 fields). The **full historical CSV** (210 MB, ~5.8M rows, 12 cols, "Próximamente") is **act-level**
  (one row per BORME act). Model them distinctly: company master vs. act stream.
- **CIF is the cross-source key but sparse.** Only ~10% of the 100-row sample carried a CIF (~18.2%
  overall per OpenMercantil). Without CIF, joins to financials must fall back to name+province or the
  BORME Hoja registral.
- **`Capital` is register share capital, not a financial statement.** OpenMercantil explicitly **excludes
  full financial statements / revenue**. Do not treat `Capital`/`Workers` as accounts data.
- **Name hygiene.** Names are uppercase with embedded legal-form suffix (`SL`, `SA`, `SLU`, `SOCIEDAD
  LIMITADA`) and occasionally a status (`EN LIQUIDACION`) or registry tag (`(R.M. PALMA DE MALLORCA)`).
  Strip these for `normalized_name`; derive `company_type` from the suffix / CIF leading letter.
- **Provenance.** This is a community reconstruction of an official gazette (BORME). For authoritative
  values verify against BORME / the Registro Mercantil. CC-BY requires attributing OpenMercantil + BORME.
- See `sample_record.json` for one real CC-BY company row.
