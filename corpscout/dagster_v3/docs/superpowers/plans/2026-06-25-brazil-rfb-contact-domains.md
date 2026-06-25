# Brazil RFB Contact Domains Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build Brazil contact info and email-derived domain feeder tables, export them to ClickHouse, and connect `br_websites` to `company_website_domains`.

**Architecture:** Extend the existing `brazil_rfb` Dagster package. DuckDB SQL derives `br_company_contact_info` from normalized establishments, derives deduped `br_websites` from accepted email domains, exports both tables to ClickHouse, and updates the existing domain graph SQL with a Brazil branch.

**Tech Stack:** Python 3.10-compatible typing, Dagster assets, DuckDB SQL, ClickHouse migrations, `dagster_clickhouse`, pytest.

---

### Task 1: Add Brazil Contact And Website Transform Tests

**Files:**
- Modify: `corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py`

- [ ] **Step 1: Write failing tests**

Add tests that prove `build_brazil_rfb_contact_info_and_websites` creates `br_company_contact_info` and `br_websites`, filters shared/public email domains, and keeps table columns aligned with ClickHouse export constants.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_brazil_rfb_transforms.py::test_build_contact_info_and_websites_extracts_unique_email_domains -q
```

Expected: fail because `build_brazil_rfb_contact_info_and_websites` does not exist.

- [ ] **Step 3: Implement minimal transform code**

Create `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/contacts.py` with DuckDB SQL that:

- unpivots email, phone, and fax contact values from `brazil_rfb.establishments`,
- extracts lowercase email suffixes,
- counts distinct `cnpj_basico` per suffix,
- applies the same unique-domain/provider-denylist rule as Estonia,
- creates `brazil_rfb.company_contact_info`, and
- creates `brazil_rfb.websites` as one row per `(cnpj_basico, root_domain)`.

- [ ] **Step 4: Run tests to verify green**

Run:

```bash
uv run pytest tests/test_brazil_rfb_transforms.py -q
```

Expected: all Brazil transform tests pass.

### Task 2: Add Table Contracts And ClickHouse Migration

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/tables.py`
- Create: `corpscout/clickhouse/migrations/000055_corpscout_br_rfb_contact_domains.up.sql`
- Create: `corpscout/clickhouse/migrations/000055_corpscout_br_rfb_contact_domains.down.sql`
- Modify: `corpscout/dagster_v3/tests/test_clickhouse_migrations.py`

- [ ] **Step 1: Write failing migration tests**

Add `000055_corpscout_br_rfb_contact_domains` to `EXPECTED_MIGRATIONS`, and add assertions that the migration creates/drops `corpscout.br_company_contact_info` and `corpscout.br_websites` with every exported column from the Brazil table constants.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_clickhouse_migrations.py::test_clickhouse_migration_files_are_explicit tests/test_clickhouse_migrations.py::test_brazil_rfb_contact_domains_migration_covers_exported_columns -q
```

Expected: fail because migration files and table constants are missing.

- [ ] **Step 3: Implement table constants and migration DDL**

Add constants for `COMPANY_CONTACT_INFO_TABLE`, `WEBSITES_TABLE`, `BR_COMPANY_CONTACT_INFO_TABLE_CH`, `BR_WEBSITES_TABLE_CH`, qualified table names, and export columns. Create ClickHouse `ReplacingMergeTree(resolved_at)` tables ordered by `(cnpj_basico, cnpj, contact_type, contact_value)` and `(cnpj_basico, root_domain)`.

- [ ] **Step 4: Run migration tests to verify green**

Run:

```bash
uv run pytest tests/test_clickhouse_migrations.py::test_clickhouse_migration_files_are_explicit tests/test_clickhouse_migrations.py::test_brazil_rfb_contact_domains_migration_covers_exported_columns -q
```

Expected: selected migration tests pass.

### Task 3: Add ClickHouse Export Assets

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/clickhouse.py`
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_brazil_rfb_clickhouse.py`
- Modify: `corpscout/dagster_v3/tests/test_brazil_rfb_assets.py`

- [ ] **Step 1: Write failing asset/export tests**

Assert that `brazil_rfb_contact_info_duckdb`, `brazil_rfb_websites_duckdb`, `brazil_rfb_clickhouse_contact_info`, and `brazil_rfb_clickhouse_websites` are registered, use the Brazil DuckDB pool, and have the expected dependencies.

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
uv run pytest tests/test_brazil_rfb_assets.py tests/test_brazil_rfb_clickhouse.py -q
```

Expected: fail because assets/export functions do not exist.

- [ ] **Step 3: Implement assets and exports**

Add DuckDB assets for contact info and websites, ClickHouse export functions for both tables, and ClickHouse assets that depend on the corresponding DuckDB assets.

- [ ] **Step 4: Run tests to verify green**

Run:

```bash
uv run pytest tests/test_brazil_rfb_assets.py tests/test_brazil_rfb_clickhouse.py -q
```

Expected: selected asset/export tests pass.

### Task 4: Feed Brazil Into Cross-Source Domain Graph

**Files:**
- Modify: `corpscout/dagster_v3/src/dagster_v3/defs/domains/assets.py`
- Modify: `corpscout/dagster_v3/tests/test_domains_assets.py`

- [ ] **Step 1: Write failing graph SQL test**

Assert the domain insert SQL contains `br_websites`, `brazil_rfb`, `cnpj_basico`, and `domain_source`.

- [ ] **Step 2: Run test to verify failure**

Run:

```bash
uv run pytest tests/test_domains_assets.py -q
```

Expected: fail because the `br_websites` branch is missing.

- [ ] **Step 3: Add graph dependency and UNION branch**

Add `brazil_rfb_clickhouse_websites` as a dependency of `domains_clickhouse`, and add a `br_websites` `UNION ALL` branch mapping `root_domain`, `domain_source`, source identity, website fields, and current/primary flags into `company_website_domains`.

- [ ] **Step 4: Run test to verify green**

Run:

```bash
uv run pytest tests/test_domains_assets.py -q
```

Expected: domain asset tests pass.

### Task 5: Verify Dagster Definitions And Focused Suite

**Files:**
- No additional files.

- [ ] **Step 1: Run focused tests**

Run:

```bash
uv run pytest tests/test_brazil_rfb_transforms.py tests/test_brazil_rfb_assets.py tests/test_brazil_rfb_clickhouse.py tests/test_domains_assets.py tests/test_clickhouse_migrations.py::test_clickhouse_migration_files_are_explicit tests/test_clickhouse_migrations.py::test_brazil_rfb_contact_domains_migration_covers_exported_columns -q
```

Expected: all selected tests pass.

- [ ] **Step 2: Run Dagster definition check**

Run:

```bash
set -a; source .env; set +a; uv run dg check defs
```

Expected: Dagster loads definitions successfully.

- [ ] **Step 3: Commit only this feature's files**

Run:

```bash
git status --short
git add corpscout/dagster_v3/docs/superpowers/specs/2026-06-25-brazil-rfb-contact-domains-design.md \
  corpscout/dagster_v3/docs/superpowers/plans/2026-06-25-brazil-rfb-contact-domains.md \
  corpscout/dagster_v3/src/dagster_v3/defs/brazil_rfb \
  corpscout/dagster_v3/src/dagster_v3/defs/domains/assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_transforms.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_assets.py \
  corpscout/dagster_v3/tests/test_brazil_rfb_clickhouse.py \
  corpscout/dagster_v3/tests/test_domains_assets.py \
  corpscout/dagster_v3/tests/test_clickhouse_migrations.py \
  corpscout/clickhouse/migrations/000055_corpscout_br_rfb_contact_domains.up.sql \
  corpscout/clickhouse/migrations/000055_corpscout_br_rfb_contact_domains.down.sql
git commit -m "Add Brazil RFB contact domain assets"
```

Do not stage unrelated existing changes in `brazil_cnae` files.
