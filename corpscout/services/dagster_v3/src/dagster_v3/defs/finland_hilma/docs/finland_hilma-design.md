# finland_hilma design doc

> Records *decisions*, not code. Follows `docs/data-source-guidelines.md`; deviations called out.

## 1. Source overview

- **Country / registry**: Finland — Hilma (hankintailmoitukset.fi), the statutory public
  procurement portal (Hansel Oy / Ministry of Finance).
- **Module**: `defs/finland_hilma/` · DuckDB file `data/finland_hilma_source.duckdb` ·
  pool `finland_hilma_duckdb`
- **ClickHouse tables**: `corpscout.fi_hilma_notices`, `corpscout.fi_hilma_notice_winners`
  (migration `000147`)
- **Dataset**: the authenticated-user **"Hilma search results" CSV export** downloaded
  manually from hankintailmoitukset.fi. The AVP read API exists but needs a per-account
  subscription key (see `companies/analysis/finland/search_attempts.md` attempt 9); until a
  key is provisioned, the manual export is the ingestion path.
  | dataset | acquisition | format | size | cadence | auth? |
  |---|---|---|---|---|---|
  | Hilma search results export | manual download → `scripts/upload_hilma_export.py` → S3 | CSV (cp1252, `;`, quoted multiline) | ~17 MB / 12.5k notices (2018→now) | manual, ad hoc | portal login (manual step) |
- **Entity keys**: buyer `Registration number` (Y-tunnus) and winner business ids embedded
  in the `Lot winner` column (`Onninen Oy (1071207-9)//Ahlsell Oy (1819153-8)`; 10,339 of
  11,265 winner entries carry an id).
- **Value**: company↔public-contract graph — awards with values (5,427 rows), buyer
  relationships, incl. national below-EU-threshold notices that TED never carries.

## 2. Ingest mode (§2) — and why

- Chosen: **manual bulk file** (deviation from the automated golden path — there is no
  keyless machine interface; the AVP API needs an account-bound subscription key).
- The S3 export object is modeled as an **external asset**
  (`finland_hilma_export_s3`, a `dg.AssetSpec` with no materialization function): Dagster
  cannot materialize it, and its description states that files are uploaded manually with
  `scripts/upload_hilma_export.py`. Everything downstream is a normal materializable chain.
- Multiple export files accumulate under `s3://source-finland-hilma/exports/`; every run
  reads them all and dedups (see §4), so a newer export supersedes older rows without any
  bookkeeping.
- **No schedule** — runs are launched manually after an upload (`finland_hilma_job`).

## 3. Loading (§3)

- Upload: `scripts/upload_hilma_export.py <csv...>` puts each file at
  `exports/<utc-timestamp>_<slug>.csv` plus a `.metadata.json` sidecar (sha256, source
  filename, size) in bucket `source-finland-hilma`.
- Parse: the file is **cp1252** with `;` delimiter and quoted embedded newlines (even in
  the header). Python transcodes cp1252→UTF-8 to a temp file, validates the header against
  the exact expected 58 normalized column titles (**a different column selection in the
  portal export is refused loudly** — the operator must export with the full column set),
  then DuckDB `read_csv(header=true, names=[...], all_varchar)` loads it.
- All files land in one raw staging table with a `source_key` provenance column.
- Refuse-empty: zero data rows raises.

## 4. Transform (§5)

- Mechanism: set-based DuckDB SQL.
- **Dedup**: one row per `(notice_number, lot_id)` — keep the row with the greatest
  `published_at`, tie-broken by greatest `source_key` (newest upload wins). Within one
  export the key is already unique (verified).
- Typing: ISO timestamps → timestamp; values → `decimal(38,2)`; `is_award` derived from
  `lower(notice_type) LIKE '%award%'`.
- Mojibake: the portal export emits JS-style `%u2013` escapes in notice types → replaced
  with "–".
- **Winners normalization**: `Lot winner` splits on `//`; each entry's trailing
  `(NNNNNNN-N)` becomes `winner_business_id` (empty when absent), the rest is
  `winner_name`. One row per winner in `fi_hilma_notice_winners` — this is the
  company-join surface.

## 5. ClickHouse schema — and DDL deviations

- `fi_hilma_notices`: 1 row per `(notice_number, lot_id)`; ORDER BY those two
  (non-nullable, lot_id may be `''`). ReplacingMergeTree.
- `fi_hilma_notice_winners`: 1 row per `(notice_number, lot_id, winner_ordinal)`.
- **Deviations**:
  - Four money amounts (notice estimate, lot estimate, procurement value, lots value)
    each carry their own currency column and `_usd` companion, but the row-level
    `fx_rate_to_usd/fx_rate_date/fx_source` trio reflects the **procurement-value
    currency** only (99.9% of rows are EUR; 14 rows carry other currencies — each `_usd`
    is still computed with its own currency's rate).
  - The 15 sustainability/innovation survey columns, `*_other` language variants and
    free-text additional-info columns stay in DuckDB staging only (sparse, no consumer).
  - `published_at` doubles as the FX key (there is no fiscal period).

## 6. Translation (§8)

- None. Names/titles ship in fi/en/sv variants from the source itself; buyer and winner
  names are proper nouns.

## 6b. Contacts (§8b) — assessed

- **No contact data** (no email/phone/website columns). Canonical contacts/domains pair
  intentionally omitted — same reasoning as `finland_verotax`: procurement is a
  supplement keyed to the existing register spine, `finland_ytj` owns the canonical pair.

## 6c. Industry / NACE (§8c) — assessed

- No company industry data; notices carry **CPV codes** (procurement vocabulary), stored
  verbatim. CPV→NACE mapping is a possible later enrichment, out of scope here.

## 7. Currency (§7)

- Amounts overwhelmingly EUR; conversion via shared `ExchangeRateClient` keyed on
  `toDate(published_at)`, batched ≤50; missing rates (e.g. exotic TZS/USN rows) keep
  native-only `_usd = NULL`.

## 8. Scheduling (§9)

- **None.** Manual `finland_hilma_job` launch after each upload. If the AVP API key is
  ever provisioned, replace the external asset with an automated download asset and add a
  daily schedule — the rest of the chain is unchanged.

## 9. Issues found during processing

- Export encoding is **cp1252**, not Latin-1 (en-dashes/euro signs live in the 0x80–0x9F
  range Latin-1 maps to control chars) — transcode in Python before DuckDB.
- The **header row contains quoted embedded newlines** — `skip=1` would cut mid-record;
  use `header=true` + `names=[...]` so the CSV reader consumes the header as one record.
- Notice types contain literal `%u2013` escape sequences (portal bug) — cleaned in SQL.
- The portal lets users export arbitrary column subsets (a 30-column trial export
  existed alongside the 58-column one) — strict header validation prevents silently
  ingesting a partial shape.

## 10. Verification

- Tests: `tests/test_finland_hilma_parsing.py` (cp1252 fixture with quoted newlines,
  `//` winners, `%u2013`, dedup across uploads, per-amount USD, column contracts),
  migration coverage in `tests/test_clickhouse_migrations.py`.
- Live: upload real export → materialize chain → spot-check counts (12,544 notices,
  ~11k winner rows), business-id join rate against `fi_companies`, `_usd` fill.
