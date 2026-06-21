# france_sirene design doc

Ingest the French SIRENE register (INSEE) — legal units + their NACE-mapped
principal activity — into DuckDB → ClickHouse, mirroring `estonia_ar`/`latvia_ur`.

## 1. Source overview
- **SIRENE** = INSEE's national company register. Two levels: **unités légales**
  (legal units, `siren` = 9 digits) and **établissements** (`siret` = siren + 5-digit nic).
- Free, open, **no credentials** — monthly "Stock" snapshots on data.gouv.fr.
  Resource URLs carry a **rotating monthly datestamp** (e.g. `20260601-091648`) →
  resolve the current URL at runtime from the data.gouv.fr dataset API (like Estonia's
  datestamp resolver), do **not** hardcode.
- Dataset: `base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret`.
  Phase-1 file: **`stock-stockunitelegale-csv.zip`** (~4M legal units, ~1.5 GB zip,
  comma-delimited UTF-8, quoted).

## 2. Ingest mode — bulk file, non-partitioned full-refresh
- Single cumulative monthly snapshot re-downloaded in full → **non-partitioned full
  refresh**, like Latvia/Estonia. One DuckDB file per source (`france_sirene_source.duckdb`,
  stem ≠ dlt dataset `france_sirene`). Pool `france_sirene_duckdb` on every writer.

## 3. Loading
- **DuckDB-native `read_csv`** (multithreaded C++), NOT row-by-row Python — the file is
  wide/large (guidelines). Stream-download the zip (retry loop), unzip, `create or replace
  table france_sirene.unite_legale_raw as select * from read_csv(<path>, all_varchar=true,
  header=true)`, then set-based normalization SQL.

## 4. Transform (plain DuckDB SQL, no dbt)
- **Name**: companies → `denominationUniteLegale`; sole traders (personne physique) →
  `prenomUsuelUniteLegale` + `nomUniteLegale` (`coalesce(denomination, trim(prenom||' '||nom))`).
  Respect `statutDiffusionUniteLegale='P'` (partially-diffusible/protected — personal data
  masked by INSEE; keep whatever is present).
- **Legal form**: `categorieJuridiqueUniteLegale` (INSEE 4-digit code) → static EN map
  (`FR_LEGAL_FORM_EN_BY_CODE`, top ~40 codes cover the vast majority; unknown → "").
- **Status**: `etatAdministratifUniteLegale` (A=active, C=ceased) → `status_en` + `is_active`.
- Creation date `dateCreationUniteLegale` (ISO). Plus `sigle`, `categorieEntreprise`
  (PME/ETI/GE), `economieSocialeSolidaireUniteLegale` flag.

## 5. ClickHouse schema (migration-owned, ReplacingMergeTree)
- **`fr_companies`** `ORDER BY (siren)` — provenance + `siren`, `name`,
  `denomination_original`, `legal_form_code`, `legal_form_en`, `status_code`, `status_en`,
  `is_active`, `creation_date Nullable(Date)`, `acronym`, `enterprise_category`,
  `is_social_solidarity_economy`, `naf_code`, `naf_nomenclature`. No `raw_*`/hash in DDL.
- Non-nullable String cols coalesced to `''` (native-driver `.encode()` rule).

## 6. Translation
- **No LLM** — legal form + status via static EN maps; NAF/industry descriptions come from
  the English `nace_categories` reference. Company names = proper nouns (not translated).

## 6b. Contacts (§8b) — ABSENT
- **SIRENE has no email/phone/website** for legal units (only physical addresses, which live
  in the établissement file). Documented absent per the standard; no `fr_company_contacts`.
  (A future website signal could only come from a non-INSEE source.)

## 6c. Industry / NACE — the strong point
- SIRENE carries **two** NAF codes per unit:
  `activitePrincipaleUniteLegale` (NAF Rev2) and `activitePrincipaleNAF25UniteLegale`
  (NAF 2025). NAF = NACE + a 5th French sub-letter, so **strip the trailing letter** to get
  the NACE code (`32.12Z`→`32.12`, `32.12Y`→`32.12`).
- **`fr_industries`** mirrors `ee_industries`/`no_industries`: one row per unit's principal
  activity (`is_primary=1`). Prefer NAF 2025 → `NACE_REV_2_1`; fall back to NAF Rev2 →
  `NACE_REV_2`. `source_industry_code`=NAF, `source_industry_code_set`='NAF2025'/'NAFRev2',
  `nace_code`=stripped, `nace_normalized_code`=digits, `nace_mapping_method`='national_truncation',
  `nace_mapping_status`='mapped'/'unmapped'. Joins `corpscout.nace_categories`.
- **Nomenclature-aware**: only NAF 2025 and `nomenclatureActivitePrincipaleUniteLegale='NAFRev2'`
  truncate to NACE. Old/ceased units carry pre-2008 codes (NAFRev1/NAP, no trailing letter, e.g.
  `52.4`) that are *not* NACE — keep the source code but leave `nace_*` empty + `unmapped`. This is
  why ~12% (mostly old ceased units) don't resolve; the live `mapped` subset joins ~100%.

## 7. Currency — N/A (Phase 1)
- No monetary values in SIRENE legal units; currency cross-cutting applies only once
  financials land (Phase 3).

## 8. Scheduling
- `france_sirene_register_job` (legal units → fr_companies) + `france_sirene_industries`
  ride one monthly snapshot → **monthly**, staggered cron; default STOPPED. (NAF + register
  refresh together from the same file → single download builds both, like EE general data.)

## 9. Phase 2 — siège (HQ) address (DONE)
- `france_sirene_etablissement_siege_raw_duckdb` downloads `stock-stocketablissement-csv.zip`
  (~30M établissements, ~5 GB), `read_csv` filtered to `etablissementSiege='true'` → one siège
  address per `siren` (`france_sirene.etablissement_siege`). Foreign-address fields fall back when
  the French ones are empty.
- The companies build LEFT JOINs it on `siren`, adding `address`, `address_supplement`,
  `postal_code`, `city`, `city_code`, `country_label` to `fr_companies` (migration `000034` ALTER).
- Both files ride the one monthly register job (legal units + establishments downloaded once each).

## 10. Deferred
- **Phase 3 — financials** from INPI (Registre National des Entreprises) — a separate source/module
  needing a free INPI API token; then the currency + USD cross-cutting applies (EUR). (A free but
  partial revenue/result summary is also available via the recherche-entreprises API.)
- **Contacts** — not available in French open data (SIRENE + recherche-entreprises both lack
  email/phone/website); would need a commercial source.
- VAT id derivation (`FR` + key + siren) if needed.

## 11. Verification
- `uv run pytest tests/test_france_sirene_*.py tests/test_clickhouse_migrations.py -q` +
  `uv run dg check defs`. Migrations apply (ledger advances). Materialize live; check
  `fr_companies` count + `legal_form_en`/`status_en` populated; `fr_industries` rows join
  `nace_categories` (sample a NAF→NACE). TDD throughout; commit by explicit path.
