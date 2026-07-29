# France Annuaire enrichments design

## 1. Source overview

- **Country/source:** France — Annuaire des Entreprises enriched legal units.
- **Module:** `defs/france_annuaire/`
- **DuckDB:** `data/france_annuaire_source.duckdb`
- **Pool:** `france_annuaire_duckdb`
- **ClickHouse:** `corpscout.fr_company_enrichments`, migration `000209`.
- **Dataset:** daily legal-unit Parquet, Open Licence 2.0, approximately
  1.14 GB observed 2026-07-28, no authentication.
- **Entity key:** SIREN; one row per legal unit.

## 2. Ingest mode

Non-partitioned full refresh. The official dataset publishes one cumulative
legal-unit Parquet each day. The dated resource URL is resolved at runtime from
the stable data.gouv.fr dataset API.

## 3. Loading

The dlt retry-capable HTTP session streams the selected Parquet to a temporary
path with whole-download retries and Content-Length validation. DuckDB
`read_parquet` selects the enrichment fields with their native booleans and
arrays. Empty snapshots fail.

## 4. Transform

One set-based DuckDB statement maps source names to explicit English schema
names. Nullable booleans remain nullable, so unknown is not changed to false.
Training, collective-agreement and FINESS identifiers stay as arrays. Raw JSON
and SHA-256 are retained in DuckDB and excluded from ClickHouse.

## 5. ClickHouse schema

`fr_company_enrichments` is one row per SIREN. It contains gender-equality,
responsible-purchasing, Alim'Confiance, association, individual entrepreneur,
entertainment, living-heritage, ESS, training, Qualiopi, administration,
mission-company, inclusion, ADEME-aid, lawyer, IDCC and FINESS evidence.

## 6. Translation

None. Exported strings are short source codes/statuses; proper names and French
free text are not part of this table.

## 6b. Contacts

The legal-unit Parquet contains no website, email, phone, mobile, fax or social
fields. The separate establishment Parquet also documents identifiers and
address/status data, not contacts. No contact table is produced by this source.

## 6c. Industry and Wikidata

The Annuaire file repeats Sirene identity fields, but the existing
`france_sirene` source remains authoritative for NAF→NACE and Wikidata P1616.
Those duplicate columns are deliberately not copied into this enrichment table.

## 7. Currency

Not applicable; the source contains no monetary amounts.

## 8. Scheduling

`france_annuaire_job`, daily at `25 4 * * *`, Europe/Belgrade. The schedule is
stopped until the first live materialization.

## 9. Issues found

- Resource URLs rotate daily, so hardcoded static URLs are unsafe.
- `est_societe_mission` is a source status code (`O`/`N`), not a boolean.
- Several evidence fields are lists; flattening them to comma-delimited strings
  would make exact matching unreliable.

## 10. Verification

- `tests/test_france_annuaire.py`
- `uv run dg check defs`
- Apply migrations, materialize the explicit three-asset chain and compare a
  known SIREN with the Recherche Entreprises complements.
