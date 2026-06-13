# OffeneRegister.de German Company Bulk (JSONL) — Field Catalog

## Source Summary

- Country: Germany
- Source type: open_data_bulk
- Organization: Open Knowledge Foundation Deutschland e.V. / OpenCorporates
- URL: https://daten.offeneregister.de/de_companies_ocdata.jsonl.bz2
- License: CC-BY 4.0 (per OffeneRegister) — OpenSanctions mirror tags it CC-BY-**NC** 4.0; **confirm before commercial use**
- Access: public, no auth
- Freshness: **stale** — mainly 2017-06 → 2019-01 (JSONL dated 2019-02-05); separate 2022 SQLite snapshot exists
- Record shape: NDJSON, one company per line (OpenCorporates company schema)
- Primary keys: `company_number` (synthetic OpenCorporates id)
- Join keys: `company_number`; natural key = `registrar` + `_registerArt` + `_registerNummer`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company_number | company_number | Synthetic OC id (court id + register no) | string | identifier | `K1101R_HRB150148` | Primary key; not the official quotable number |
| name | name | Legal name incl. legal-form suffix | string | legal_name | `Beispiel GmbH` | Parse suffix → company_type |
| current_status | current_status | Textual registration status | string | status | `currently registered` | Free text, no dissolution date |
| jurisdiction_code | jurisdiction_code | Always `de` | string | metadata | `de` | Constant |
| registered_address | registered_address | Registered address (free text) | string | address | `Beispielstraße 1, 40210 Düsseldorf` | **Unparsed** |
| retrieved_at | retrieved_at | Scrape timestamp | datetime | metadata | `2018-11-14T00:00:00Z` | Shows staleness |
| previous_names | previous_names | Prior names | array | legal_name | `[{company_name:…}]` | No per-name dates |
| subsequent_registrations | subsequent_registrations | Links to later registrations | array | relationship | — | Sparse |
| officers | officers | Representatives (see below) | array | person | — | **PII** |
| officers[].name | name | Officer full name | string | person | `Max Mustermann` | **PII** |
| officers[].position | position | Role | string | person | `Geschäftsführer`,`Prokurist`,`Vorstand` | Free text |
| officers[].type | type | person/company | string | person | `person` | — |
| officers[].start_date | start_date | Role start | date | date | `2015-03-01` | Often null |
| officers[].end_date | end_date | Role end | date | date | `2018-06-30` | Implies left/dismissed |
| officers[].other_attributes.firstname | firstname | First name | string | person | `Max` | **PII** |
| officers[].other_attributes.lastname | lastname | Last name | string | person | `Mustermann` | **PII** |
| officers[].other_attributes.city | city | Officer city | string | person | `Düsseldorf` | **PII** |
| officers[].other_attributes.dismissed | dismissed | Removed flag | boolean | person | `false` | Pair with end_date |
| officers[].other_attributes.flag | flag | Representation rule | string | person | — | Free text |
| all_attributes._registerArt | _registerArt | Register type | string | identifier | `HRB`,`HRA`,`GnR`,`VR`,`PR` | Part of natural key |
| all_attributes._registerNummer | _registerNummer | Court-scoped register number | string | identifier | `150148` | **Not globally unique** |
| all_attributes | all_attributes | Register metadata block | object | metadata | see JSON | native_company_number / federal_state / registered_office / registrar / additional_data |

## Interpretation Notes

- **Identifiers.** `company_number` is synthetic to OpenCorporates and is **not** what a German user
  quotes. The human/official reference is `native_company_number` (e.g. `Hamburg HRB 150148`), and the
  stable natural key is `registrar` + `_registerArt` + `_registerNummer`. `_registerNummer` is **scoped
  per court** — never unique on its own.
- **Register types (`_registerArt`).** HRB = Kapitalgesellschaften (GmbH, AG, UG); HRA =
  Personengesellschaften / sole traders (e.K., OHG, KG); GnR = cooperatives (eG); VR = associations
  (e.V.); PR = partnerships (PartG).
- **`additional_data` flags** (AD, CD, HD, DK, SI, UT, VÖ) describe **which documents are available at
  handelsregister.de** for that company — they are availability booleans, not the documents/data
  themselves. `SI` = structured XML ("Strukturierte Inhalte") is retrievable per document.
- **Language is mixed**: `federal_state` is given in English (e.g. "North Rhine-Westphalia"),
  `registered_office`/`registrar` in German.
- **No activity/NACE (WZ) code, no tax_id, no VAT-ID, no incorporation/dissolution date** as clean
  fields. These are genuine gaps of this dataset (see `schema_notes.md`).
- **PII**: the `officers[]` block carries personal data (names, city, role dates). Apply a GDPR lawful
  basis and retention policy before persisting beyond the raw zone.
- **Sample availability**: the June-2026 bulk download was not retained (data folder is gitignored), so
  example values here come from `schema_notes.md` (derived from the original OffeneRegister sample) and
  are illustrative, CC-BY-sourced placeholders — not copied personal records.
