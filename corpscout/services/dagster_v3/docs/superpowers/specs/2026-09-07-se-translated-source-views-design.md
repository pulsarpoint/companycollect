# SE translated source views: translations keyed on the register tables

Date: 2026-09-07. Owner decision in chat the same day. Amends the basic-info
design of 2026-09-03 (section on the Bolagsverket and Ratsit extractors) and
brings Sweden onto the pattern every other country already follows.

## Why

The Swedish activity-description translations are keyed in
`corpscout.text_translations` on the spine table `corpscout.se_companies`
(`source_table = 'corpscout.se_companies'`, `source_column =
'activity_description'`). The basic-info Bolagsverket extractor reads
`se_bolagsverket_companies` and joins those rows itself by `cityHash64` of the
text. Two things are wrong with that:

1. The spine is retired in basic-info slice 5. A key named after a table that
   is about to disappear has to be re-keyed anyway.
2. The join lives inside an extractor. Every other country exposes a plain
   `<table>_translated` view (`no_companies_translated`,
   `lv_companies_translated`, the legal-form views) that carries the base
   table's columns plus one `<column>_en` per translated column, and readers
   select columns. Sweden had `se_companies_translated` until migration 000339
   folded it into the serving view; the extractors then grew their own joins.

Observed on prod 2026-09-07: 226,104 companies whose newest Bolagsverket
suggestion still shows Swedish in `description` although their translation
landed on 2026-09-05/06, after the last extractor run on 2026-09-04. The
extractor's change scan already handles this (its `observed_at` is the later
of the register stamp and the translation stamp); nothing has run since. The
redesign keeps that property.

## Facts the design rests on (prod, 2026-09-07)

| Source table | Translatable column | Distinct texts | Translated by identical hash |
|---|---|---|---|
| `se_bolagsverket_companies` | `activity_description` (sv) | 2,168,175 | all, under the spine key |
| `se_ratsit_company` | `business_description` (sv) | 66,910 | 65,080, under the spine key |
| `se_scb_companies` | none | 0 | not applicable |

- Register text and spine text are byte-identical for all 2,855,016 companies
  with a text, so the `cityHash64` keys carry over unchanged.
- Stored texts are already trimmed in both tables (0 rows differ from
  `trim()`), so the loader's `cityHash64(column)` and the extractors'
  `cityHash64(trim(column))` agree.
- No Ratsit text exceeds the loader's 8,000-character bound (max 3,423).
- SCB carries names and codes only. Legal-form and status codes get curated
  labels from `se_code_labels`, never the LLM. Nothing to translate: no SCB
  view.
- Ratsit's `legal_form` is a 28-value vocabulary (`Aktiebolag`,
  `Handelsbolag`, ...). It wants a code mapping, not a translation. Out of
  scope here; the extractor keeps not supplying `legal_form_code`.
- `FINAL` passes through a plain view onto its MergeTree base (verified on
  `lv_companies_translated`), so the extractors keep their `FINAL`.
- `text_translations.version` is the loader's `int(time.time())` at insert,
  so `max(version)` per hash is when the text was last translated.
- Prod `schema_migrations` head is 389; 000388 and 000389 exist in the
  working tree but are not yet committed on main. The new migration is 000390.

## Design

### Translation keys

One key per source table and column, both `sv -> en`:

- `('corpscout.se_bolagsverket_companies', 'activity_description')`
- `('corpscout.se_ratsit_company', 'business_description')`

The spine key `('corpscout.se_companies', 'activity_description')` is kept
untouched until slice 5. Its readers that survive until then (the serving view
`se_companies_serving` via migration 000347, the old `se_company_info_scb`
artifact in `se_company/scb.py`) are not changed by this work.

### Migration 000390 (`000390_corpscout_se_source_translated_views`)

Additive. In order:

1. Copy every spine-key row to the Bolagsverket key, preserving `version`
   (so translation stamps do not move and the extractor does not treat every
   text as freshly translated):

   ```sql
   INSERT INTO corpscout.text_translations
   SELECT 'corpscout.se_bolagsverket_companies', source_column, source_text_hash,
          source_lang, target_lang, translated_text, provider, model, version
   FROM corpscout.text_translations
   WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';
   ```

2. Seed the Ratsit key from the same rows for every hash that appears in
   `se_ratsit_company.business_description`, so only the roughly 1,830 texts
   Ratsit alone has ever reach the LLM:

   ```sql
   INSERT INTO corpscout.text_translations
   SELECT 'corpscout.se_ratsit_company', 'business_description', source_text_hash,
          source_lang, target_lang, translated_text, provider, model, version
   FROM corpscout.text_translations
   WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description'
     AND source_text_hash IN (
       SELECT cityHash64(business_description) FROM corpscout.se_ratsit_company
       WHERE business_description IS NOT NULL AND business_description != '');
   ```

3. Two plain views, the Latvia shape plus a stamp column:

   ```sql
   CREATE OR REPLACE VIEW corpscout.se_bolagsverket_companies_translated AS
   SELECT
       c.*,
       ifNull(act.translated_text, '') AS activity_description_en,
       act.translated_at AS activity_description_translated_at
   FROM corpscout.se_bolagsverket_companies AS c
   LEFT JOIN (
       SELECT source_text_hash,
              argMax(translated_text, version) AS translated_text,
              toDateTime64(max(version), 3, 'UTC') AS translated_at
       FROM corpscout.text_translations
       WHERE source_table = 'corpscout.se_bolagsverket_companies'
         AND source_column = 'activity_description'
         AND source_lang = 'sv' AND target_lang = 'en'
       GROUP BY source_text_hash
   ) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_description, ''));
   ```

   `se_ratsit_company_translated` is the same over `se_ratsit_company`,
   `business_description`, columns `business_description_en` and
   `business_description_translated_at`. Under `join_use_nulls = 0` the stamp
   for an untranslated row is the DateTime64 zero, so readers test
   `activity_description_en != ''`, never the stamp, to know whether a
   translation exists.

   Plain views, not refreshable materialized views: the extractors full-scan
   the base table on every change scan anyway, `FINAL` passes through, and a
   refresh cadence would put lag on the stamp and need a memory budget like
   the serving view's.

Down migration: drop the two views. The copied rows stay, matching 000252's
convention that a rollback never deletes translations. Re-running the up
migration re-inserts identical rows that collapse on merge.

### Scan assets (the translator side)

- `sweden_company_translation_load` (`sweden_company/translation.py`):
  `ACTIVITY_DESCRIPTION_FIELD` becomes
  `TranslationField("corpscout.se_bolagsverket_companies", "activity_description", "sv", "en", extra_where="has_company = 1")`;
  the asset's dep moves from `sweden_company_companies_clickhouse` to
  `sweden_company_bolagsverket_companies_clickhouse`. Coverage and the
  `translations_present` check follow the field.
- New `sweden_ratsit_translation_load` next to the Ratsit normalizer, same
  shape, field
  `TranslationField("corpscout.se_ratsit_company", "business_description", "sv", "en", extra_where=...)`
  where `extra_where` scopes to the current normalizer version, interpolated
  from the `RATSIT_NORMALIZER_VERSION` constant (94 rows of v1 remain and must
  not be enqueued); dep `se_ratsit_normalized`, with its coverage check. Added to
  `TRANSLATION_LOAD_ASSETS` in `czech_legal_forms/assets.py` (the
  `translation_coverage_job` test enforces the list) and to the sweden_ratsit
  definitions.

### Extractors (the basic-info side)

- `basic_info/bolagsverket.py`, version `bolagsverket-v3`: reads
  `FROM corpscout.se_bolagsverket_companies_translated FINAL WHERE has_company = 1`,
  no `text_translations` CTE. `observed_at = greatest(observed_at,
  if(activity_description_en != '', activity_description_translated_at, observed_at))`
  in both `current_sql` and `select_sql`, so a translation that lands later
  still re-selects the company. The `if` guards the stamp: under
  `join_use_nulls = 1` (the local test runs both settings) an untranslated
  row's stamp is NULL and a bare `greatest` would return NULL.
  `description` is the English column when
  non-empty else the Swedish text; `description_language` is `en` or `sv`
  accordingly; `description_sv` is the Swedish text. Everything else unchanged.
- `basic_info/ratsit.py`, version `ratsit-v2`: reads
  `corpscout.se_ratsit_company_translated FINAL`, same three-column rule over
  `business_description_en`. Its `observed_at` becomes
  `greatest(normalized_at, translated stamp)` in both SQL texts, so the
  `LIMIT 1 BY company_id` newest-report choice is unchanged but a new
  translation re-selects the company.
- Fold, precedence, the suggestion table and the backoffice are untouched.
  The Info tab already renders `description_language`, so translated Ratsit
  rows show as English without UI work.

### Rollout order

1. Merge and apply 000390 on prod (a few minutes: 2.18M plus about 65k row
   inserts, two views).
2. Deploy dagster_v3. Deploying before step 1 would make both scans anti-join
   an empty key and enqueue every text to the LLM again. The plan carries a
   pre-deploy check: count of rows under the Bolagsverket key equals the count
   under the spine key.
3. Materialize `sweden_company_translation_load` (expected 0 to enqueue),
   `sweden_ratsit_translation_load` (about 1,830 texts), then
   `se_basic_info_suggestions_bolagsverket` (re-selects the 226,104 stale
   companies) and `se_basic_info_suggestions_ratsit`, then the fold.

### Slice 5, unchanged

When `se_companies` goes: delete the spine-key rows, re-base
`se_companies_serving` on `se_company_basic_info` (the folded row already
carries both languages with their sources, so that view's translation join can
disappear), and delete `se_company/scb.py` with the old publisher in slice 4.

## Testing

- `test_se_company_basic_info_extractors_sql.py`: the Bolagsverket and Ratsit
  SQL name the `_translated` views and never `text_translations`; the
  `observed_at` expression references the stamp column.
- `test_se_company_basic_info_extractors_clickhouse_local.py`: the fixture
  replays `CREATE OR REPLACE VIEW` statements from the migration list as well
  as `CREATE TABLE` (today it keeps only `CREATE DATABASE`/`CREATE TABLE`),
  adds 000390 and the Ratsit table migration, seeds `text_translations` under
  the new keys, and asserts: translated Bolagsverket row is `en` with the
  Swedish in `description_sv`; untranslated row is `sv`; a translation newer
  than the register stamp moves `observed_at`; the same three for Ratsit.
- `test_clickhouse_migrations.py`: 000390 registered; view columns present;
  the down drops both views.
- A test over `build_scan_sql` for the Ratsit field (extra_where scoping).
- `test_translation_coverage_job_covers_every_loader` passes with the new
  loader listed.
- Migration-number collision check against main and prod before merge, per
  the 000368 incident.

## Out of scope

- SCB view (nothing to translate).
- Ratsit `legal_form` code mapping.
- Deleting the spine-key rows or touching `se_companies_serving`,
  `se_company/scb.py`, `se-company-shell.server.ts` (slices 4 and 5).
- Any change to fold, precedence, the suggestion table or the Info tab.
