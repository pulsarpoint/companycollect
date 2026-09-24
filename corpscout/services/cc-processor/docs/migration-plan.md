# Migration plan — profile split + identifier rename (in-place / clean history)

This is a **fundamentally-broken-schema fix**, so we do NOT keep the iterations in the migration
ledger. The migration *files* must read as the clean final-state schema in [`schema.md`](./schema.md);
the one-time data moves run **manually on the live DB**, and the historical files are **edited in
place** so a fresh replay builds the target directly. Same approach as the 063/064/065 rewrite.

## What's on disk / live (confirmed)

- `000051_corpscout_commoncrawl_company_identifiers.{up,down}.sql` — creates **only**
  `commoncrawl_company_identifiers`. → rename in place to `domain_identifiers`.
- `000053_corpscout_commoncrawl_company_profile.{up,down}.sql` — creates **only**
  `commoncrawl_company_profile`. → remove (the table is being dropped).
- `000067_corpscout_commoncrawl_company_contacts.{up,down}.sql` — uncommitted dead end. → delete,
  reuse the number for `domain_metadata`.
- Live ledger at **066**. Live tables: `commoncrawl_company_identifiers` (1440 rows),
  `commoncrawl_company_profile` (row count TBD — check before backfill).

## Migrations vs manual

- **Migrations = final-state DDL only.** After this, the only commoncrawl migration files that mention
  these areas are: `000051 …domain_identifiers` (CREATE), `000067 …domain_metadata` (CREATE),
  `000068 …domain_contact_info` (CREATE). No `company_profile`, no `company_identifiers`, no backfill,
  no rename/drop steps anywhere.
- **Manual one-time, live DB only:** backfill profile→new tables, drop `company_profile`, rename
  `company_identifiers`→`domain_identifiers`. These leave **no** migration files.

> Why the rename/drop are manual, not migrations: the live DB already has these tables (created when
> 051/053 first applied). Editing 051/053's *files* does not re-run them (golang-migrate tracks
> version, not content — confirmed with 063/064). So the file is cosmetic/replay-only; the live table
> is changed by hand. golang-migrate tolerates a version gap (deleting 053) — `up`/fresh-replay apply
> the next existing version; only a `down` past 53 would miss it (acceptable).

## End state of the migration files

| File | Action | Result |
|---|---|---|
| `000051_…_company_identifiers.*` | rename file + edit content | `000051_…_domain_identifiers.*` creates `commoncrawl_domain_identifiers` |
| `000053_…_company_profile.*` | delete | gone |
| `000067_…_company_contacts.*` | delete | gone |
| `000067_…_domain_metadata.*` | new | creates `commoncrawl_domain_metadata` |
| `000068_…_domain_contact_info.*` | new | creates `commoncrawl_domain_contact_info` |

`tests/test_clickhouse_migrations.py::EXPECTED_MIGRATIONS`: drop the `…company_profile` entry, rename
the `…company_identifiers` entry to `…domain_identifiers`, add the two new ones. Update/rename any
per-table contract tests for 051/053 (the wikidata_company_identifiers tests are a different table —
leave them).

## Steps

### Phase A — files + new tables (no live coordination; old binary keeps running)

1. `git rm` the dead `000067_…_company_contacts.{up,down}.sql`.
2. Write `000067_…_domain_metadata.{up,down}.sql` (CREATE per schema.md:
   `ReplacingMergeTree(resolved_at) ORDER BY (root_domain, crawl_id)`).
3. Write `000068_…_domain_contact_info.{up,down}.sql` (CREATE:
   `ORDER BY (root_domain, contact_type, value)` — the ex-`company_contacts` body).
4. `git mv` `000051_…_company_identifiers.{up,down}.sql` → `…_domain_identifiers.*`; edit content
   (`commoncrawl_company_identifiers` → `commoncrawl_domain_identifiers` in up + down).
5. `git rm` `000053_…_company_profile.{up,down}.sql`.
6. Update `EXPECTED_MIGRATIONS` + contract tests; `uv run pytest tests/test_clickhouse_migrations.py`.
7. Apply the two new tables: `make clickhouse-migrate-up` (applies 067, 068; 051/053 already past,
   not re-run — live `company_identifiers`/`company_profile` untouched for now).

### Phase B — backfill (manual, live, while `company_profile` still exists)

8. `SELECT count() FROM corpscout.commoncrawl_company_profile FINAL;` — if 0, skip to C.
9. Validate the selects on a few domains (mirror the 065 sample check), then run:
```sql
INSERT INTO corpscout.commoncrawl_domain_metadata
  (crawl_id,root_domain,subdomain,name,description,logo,country,founding_year,employee_count,source,source_url,source_run_id,resolved_at)
SELECT crawl_id,root_domain,subdomain,name,description,logo,country,founding_year,employee_count,'jsonld',source_url,source_run_id,resolved_at
FROM corpscout.commoncrawl_company_profile FINAL
WHERE name!='' OR description!='' OR logo!='' OR country!='' OR founding_year!=0 OR employee_count!=0;

INSERT INTO corpscout.commoncrawl_domain_contact_info (crawl_id,root_domain,contact_type,value,source,source_url,source_run_id,resolved_at)
SELECT crawl_id,root_domain,'email',email,'jsonld',source_url,source_run_id,resolved_at
FROM corpscout.commoncrawl_company_profile FINAL WHERE email!='';

INSERT INTO corpscout.commoncrawl_domain_contact_info (crawl_id,root_domain,contact_type,value,source,source_url,source_run_id,resolved_at)
SELECT crawl_id,root_domain,'phone',phone,'jsonld',source_url,source_run_id,resolved_at
FROM corpscout.commoncrawl_company_profile FINAL WHERE phone!='';

INSERT INTO corpscout.commoncrawl_domain_contact_info (crawl_id,root_domain,contact_type,value,source,source_url,source_run_id,resolved_at)
SELECT crawl_id,root_domain,'social',sa,'jsonld',source_url,source_run_id,resolved_at
FROM corpscout.commoncrawl_company_profile FINAL ARRAY JOIN same_as AS sa WHERE sa!='';
```
(Idempotent — ReplacingMergeTree collapses re-runs.)

### Phase C — worker cutover + live rename/drop (one window, between shard runs)

10. Worker changes (most contact plumbing already exists locally):
    - `internal/output`: add `MetadataRow`; **drop `ProfileRow`/`WriteProfiles`**; keep
      `ContactRow`/`WriteContacts`; extend the `ch==parquet` tag test.
    - `internal/worker`: tech path builds `MetadataRow` (from `a.profile`) + `ContactRow`s
      (email regex+jsonld, phone jsonld, **`same_as`→`social`**); remove the `ProfileRow` build;
      `ShardResult` swaps `Profiles`→`Metadata`, keeps `Contacts`.
    - `internal/load`: `Tables`/`Kinds` — add `metadata`→`commoncrawl_domain_metadata`,
      `contacts`→`commoncrawl_domain_contact_info`, **rename** `identifiers`→
      `commoncrawl_domain_identifiers`, **remove** `profiles`; `FromFile` cases to match.
    - `internal/load/legacy.go`: also emit `ContactRow`s from the fat parquet's `Emails` →
      `domain_contact_info` (one-time recovery of the industry-pass emails 066 dropped).
    - `cmd/cc-enrich-worker`: write/load `metadata.parquet` + `contacts.parquet`; stop `profiles.parquet`.
    - `make vet build test` green; commit by explicit path; **push**.
11. On the box, between shards: `git pull && make -C cc-enrich-worker build`, then **manually** on live:
```sql
RENAME TABLE corpscout.commoncrawl_company_identifiers TO corpscout.commoncrawl_domain_identifiers;
DROP TABLE IF EXISTS corpscout.commoncrawl_company_profile;
```
12. Recover old industry emails: `cc-enrich-worker load --dir data/crawl/out_industry_<N>` per old
    shard dir → `domain_contact_info` (extended legacy loader). ⚠️ those parquets are the only copy.
13. Resume processing — new chunks write the target schema directly.

## Coordination rule (the recurring skew)

Do step 11 (live rename/drop) **only after** the new binary is built on the box. Renaming/dropping a
table the deployed binary still writes re-breaks `load.FromDir` (the `No such column emails` class of
failure). Old binary keeps working through Phases A+B because `company_identifiers`/`company_profile`
still exist live; the file edits don't touch the live DB.

## Verification

```sql
SELECT count() FROM corpscout.commoncrawl_domain_metadata FINAL;
SELECT contact_type, count() FROM corpscout.commoncrawl_domain_contact_info FINAL GROUP BY contact_type;
SELECT name FROM system.tables WHERE database='corpscout'
  AND name IN ('commoncrawl_domain_identifiers','commoncrawl_company_identifiers','commoncrawl_company_profile');
-- expect: domain_identifiers present; the two old names absent
```

## Out of scope

The GLEIF/VAT company-master resolver (general source, identifier-keyed) — see schema.md.
