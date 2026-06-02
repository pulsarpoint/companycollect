# NACE Taxonomy Tables Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add canonical NACE taxonomy tables to Companycollect/Corpscout without storing Norwegian SN-specific codes in the global company collection model.

**Architecture:** Global Corpscout tables store only standard NACE classifications and codes. BRREG keeps Norwegian source/native industry codes in BRREG-specific tables and maps those codes to canonical NACE rows later during BRREG publish/merge. This first task is deliberately limited to schema, sqlc read/write queries, migration tests, and seed/import-ready table design; it does not change BRREG workflow processing or `company_industries`.

**Tech Stack:** PostgreSQL migrations, sqlc, Go tests, `github.com/stretchr/testify/require`, existing Corpscout database/migration conventions.

---

## Design Decision

Do **not** store Norwegian SN subclass codes like `68.200` as canonical global industry taxonomy rows. Those are source/native or national-extension codes. Store canonical NACE rows globally, for example:

- Section: `L`
- Division: `68`
- Group: `68.2`
- Class: `68.20`

BRREG-specific rows can later map:

```text
BRREG/SN code 68.200 -> canonical NACE code 68.20
```

This keeps Companycollect universal and avoids polluting global industry tables with source-specific codes. Source-specific evidence and native code values remain in `brreg_workflow` / BRREG source tables.

## Official References

Use these as source references in table metadata and later importer code:

- Eurostat NACE overview: `https://ec.europa.eu/eurostat/web/nace`
- Eurostat NACE Rev. 2.1 page and files: `https://ec.europa.eu/eurostat/web/nace/nace-rev.-2.1`
- Eurostat correspondence tables: `https://ec.europa.eu/eurostat/web/nace/correspondence-tables`
- EU vocabulary / linked-data assets may be used later if we want RDF/SKOS import rather than CSV/XLSX.

This plan does not implement network import yet. It creates tables shaped so an importer can safely upsert NACE Rev. 2.1 and future versions.

## Current Repo Context

Existing relevant state:

- Latest migration: `corpscout/database/migrations/000069_brreg_domain_search_evidence.*.sql`
- Current company industry table: `company_industries(industry TEXT, source TEXT, evidence JSONB)`
- Current suggestion industry table: `suggestion_company_industries(industry TEXT, source TEXT, evidence JSONB)`
- Current BRREG source-specific industry table: `brreg_source_industries(code TEXT, description TEXT, description_en TEXT, classification_type TEXT, raw_section JSONB)`
- sqlc config reads all files under:
  - `corpscout/database/migrations/`
  - `corpscout/database/schema_stubs/`
  - `corpscout/database/queries/`
- Generated Go package: `corpscout/scheduler/internal/db/gen`

## Scope

In scope:

- Canonical NACE taxonomy schema.
- Version-aware code hierarchy.
- Aliases for machine lookup/search where aliases are still canonical NACE-shaped, not SN-specific.
- Mapping-ready constraints and views.
- sqlc queries for upsert/list/lookup.
- Migration tests and sqlc generation.

Out of scope for this first plan:

- Import workflow that downloads NACE files.
- BRREG SN-to-NACE mapping table.
- Updating `company_industries`.
- Updating suggestions.
- UI.
- Any source-specific taxonomy data.

## File Structure

Create:

- `corpscout/database/migrations/000070_nace_taxonomy.up.sql`  
  Creates canonical NACE taxonomy tables, indexes, and read views.
- `corpscout/database/migrations/000070_nace_taxonomy.down.sql`  
  Drops NACE taxonomy views and tables in FK-safe order.
- `corpscout/database/queries/nace_taxonomy.sql`  
  sqlc queries for upsert, lookup, and listing.
- `corpscout/scheduler/internal/db/nace_taxonomy_migration_test.go`  
  Migration shape tests.

Generated:

- `corpscout/scheduler/internal/db/gen/nace_taxonomy.sql.go`
- `corpscout/scheduler/internal/db/gen/models.go` updates

No app, worker, or HTTP files should change in this first task.

## Table Model

Use two core tables and one alias table.

### `nace_classifications`

One row per NACE version, e.g. `NACE Rev. 2.1`.

Columns:

- `id UUID`
- `code_system TEXT` fixed to `NACE`
- `revision TEXT`, e.g. `2.1`
- `name TEXT`, e.g. `NACE Rev. 2.1`
- `valid_from DATE`
- `valid_to DATE`
- `source_url TEXT`
- `source_metadata JSONB`
- timestamps

### `nace_codes`

One row per canonical NACE code in a version.

Columns:

- `id UUID`
- `classification_id UUID`
- `code TEXT`
- `normalized_code TEXT`
- `level SMALLINT`
- `level_name TEXT`
- `parent_code TEXT`
- `parent_id UUID`
- `title TEXT`
- `description TEXT`
- `includes TEXT`
- `excludes TEXT`
- `notes JSONB`
- `source_payload JSONB`
- `source_hash TEXT`
- `active BOOLEAN`
- timestamps

Allowed levels:

- `1 / section`
- `2 / division`
- `3 / group`
- `4 / class`

No level `5` belongs in canonical NACE tables. Norwegian SN `68.200` belongs in BRREG-specific mapping later.

### `nace_code_aliases`

Aliases for lookup convenience only. They must not represent non-NACE taxonomy rows.

Examples:

- exact: `68.20`
- normalized: `6820`
- dotted variant: `68.2` for group-level code

Do not store `68.200` as an alias in this table in this first step. That belongs to the later BRREG mapping layer where the alias source is explicitly BRREG/SN.

## Task 1: Migration Shape Tests

**Files:**

- Create: `corpscout/scheduler/internal/db/nace_taxonomy_migration_test.go`
- Create later: `corpscout/database/migrations/000070_nace_taxonomy.up.sql`
- Create later: `corpscout/database/migrations/000070_nace_taxonomy.down.sql`

- [ ] **Step 1: Write failing migration test**

Create `corpscout/scheduler/internal/db/nace_taxonomy_migration_test.go`:

```go
package db

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNACETaxonomyMigrationDefinesCanonicalTables(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000070_nace_taxonomy.up.sql")
	require.NoError(t, err)
	sql := string(body)

	required := []string{
		"CREATE TABLE nace_classifications",
		"CREATE TABLE nace_codes",
		"CREATE TABLE nace_code_aliases",
		"CREATE OR REPLACE VIEW v_nace_taxonomy_state",
		"CREATE OR REPLACE VIEW v_nace_code_tree",
	}
	for _, item := range required {
		require.Contains(t, sql, item)
	}

	require.Contains(t, sql, "code_system TEXT NOT NULL DEFAULT 'NACE'")
	require.Contains(t, sql, "revision TEXT NOT NULL")
	require.Contains(t, sql, "UNIQUE (code_system, revision)")
	require.Contains(t, sql, "UNIQUE (classification_id, code)")
	require.Contains(t, sql, "level_name IN ('section', 'division', 'group', 'class')")
	require.Contains(t, sql, "level BETWEEN 1 AND 4")
	require.NotContains(t, sql, "brreg_workflow.")
	require.NotContains(t, sql, "68.200")
	require.NotContains(t, sql, "SN 2007")
	require.NotContains(t, sql, "SN 2025")
}

func TestNACETaxonomyDownMigrationDropsCanonicalObjects(t *testing.T) {
	body, err := os.ReadFile("../../../database/migrations/000070_nace_taxonomy.down.sql")
	require.NoError(t, err)
	sql := string(body)

	require.Contains(t, sql, "DROP VIEW IF EXISTS v_nace_code_tree")
	require.Contains(t, sql, "DROP VIEW IF EXISTS v_nace_taxonomy_state")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_code_aliases")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_codes")
	require.Contains(t, sql, "DROP TABLE IF EXISTS nace_classifications")
}
```

- [ ] **Step 2: Run test and verify it fails**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db -run TestNACETaxonomy -count=1
```

Expected: fail because `000070_nace_taxonomy.up.sql` does not exist.

- [ ] **Step 3: Commit failing test only if using strict TDD commits**

Usually skip this commit in this repo unless the team wants red commits.

## Task 2: NACE Taxonomy Migration

**Files:**

- Create: `corpscout/database/migrations/000070_nace_taxonomy.up.sql`
- Create: `corpscout/database/migrations/000070_nace_taxonomy.down.sql`
- Test: `corpscout/scheduler/internal/db/nace_taxonomy_migration_test.go`

- [ ] **Step 1: Create up migration**

Create `corpscout/database/migrations/000070_nace_taxonomy.up.sql`:

```sql
CREATE TABLE nace_classifications (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  code_system TEXT NOT NULL DEFAULT 'NACE',
  revision TEXT NOT NULL,
  name TEXT NOT NULL,
  valid_from DATE,
  valid_to DATE,
  source_url TEXT,
  source_metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_classifications_code_system CHECK (code_system = 'NACE'),
  CONSTRAINT chk_nace_classifications_revision CHECK (btrim(revision) <> ''),
  CONSTRAINT chk_nace_classifications_name CHECK (btrim(name) <> ''),
  CONSTRAINT chk_nace_classifications_source_metadata_object CHECK (jsonb_typeof(source_metadata) = 'object'),
  UNIQUE (code_system, revision)
);

CREATE TABLE nace_codes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  classification_id UUID NOT NULL REFERENCES nace_classifications(id) ON DELETE CASCADE,
  code TEXT NOT NULL,
  normalized_code TEXT NOT NULL,
  level SMALLINT NOT NULL,
  level_name TEXT NOT NULL,
  parent_code TEXT,
  parent_id UUID REFERENCES nace_codes(id) ON DELETE SET NULL,
  title TEXT NOT NULL,
  description TEXT,
  includes TEXT,
  excludes TEXT,
  notes JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_hash TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_codes_code CHECK (btrim(code) <> ''),
  CONSTRAINT chk_nace_codes_normalized_code CHECK (btrim(normalized_code) <> ''),
  CONSTRAINT chk_nace_codes_level CHECK (level BETWEEN 1 AND 4),
  CONSTRAINT chk_nace_codes_level_name CHECK (level_name IN ('section', 'division', 'group', 'class')),
  CONSTRAINT chk_nace_codes_title CHECK (btrim(title) <> ''),
  CONSTRAINT chk_nace_codes_notes_object CHECK (jsonb_typeof(notes) = 'object'),
  CONSTRAINT chk_nace_codes_source_payload_object CHECK (jsonb_typeof(source_payload) = 'object'),
  UNIQUE (classification_id, code)
);

CREATE TABLE nace_code_aliases (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  nace_code_id UUID NOT NULL REFERENCES nace_codes(id) ON DELETE CASCADE,
  alias_type TEXT NOT NULL,
  alias_code TEXT NOT NULL,
  normalized_alias_code TEXT NOT NULL,
  source TEXT NOT NULL DEFAULT 'nace',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT chk_nace_code_aliases_type CHECK (alias_type IN ('exact', 'normalized', 'search')),
  CONSTRAINT chk_nace_code_aliases_code CHECK (btrim(alias_code) <> ''),
  CONSTRAINT chk_nace_code_aliases_normalized CHECK (btrim(normalized_alias_code) <> ''),
  CONSTRAINT chk_nace_code_aliases_source CHECK (source = 'nace'),
  CONSTRAINT chk_nace_code_aliases_metadata_object CHECK (jsonb_typeof(metadata) = 'object'),
  UNIQUE (nace_code_id, alias_type, alias_code),
  UNIQUE (source, alias_type, normalized_alias_code, nace_code_id)
);

CREATE INDEX idx_nace_codes_classification_level_code
  ON nace_codes(classification_id, level, code);

CREATE INDEX idx_nace_codes_parent
  ON nace_codes(parent_id)
  WHERE parent_id IS NOT NULL;

CREATE INDEX idx_nace_codes_parent_code
  ON nace_codes(classification_id, parent_code)
  WHERE parent_code IS NOT NULL;

CREATE INDEX idx_nace_codes_normalized
  ON nace_codes(classification_id, normalized_code);

CREATE INDEX idx_nace_codes_active
  ON nace_codes(classification_id, active, level, code);

CREATE INDEX idx_nace_code_aliases_lookup
  ON nace_code_aliases(source, alias_type, normalized_alias_code);

CREATE OR REPLACE VIEW v_nace_taxonomy_state AS
SELECT
  nc.id AS classification_id,
  nc.code_system,
  nc.revision,
  nc.name,
  nc.valid_from,
  nc.valid_to,
  count(ncode.id) FILTER (WHERE ncode.active) AS active_codes,
  count(ncode.id) FILTER (WHERE NOT ncode.active) AS inactive_codes,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'section') AS sections,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'division') AS divisions,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'group') AS groups,
  count(ncode.id) FILTER (WHERE ncode.active AND ncode.level_name = 'class') AS classes,
  max(ncode.updated_at) AS codes_updated_at,
  nc.updated_at AS classification_updated_at
FROM nace_classifications nc
LEFT JOIN nace_codes ncode ON ncode.classification_id = nc.id
GROUP BY nc.id;

CREATE OR REPLACE VIEW v_nace_code_tree AS
SELECT
  nc.code_system,
  nc.revision,
  ncode.id,
  ncode.classification_id,
  ncode.code,
  ncode.normalized_code,
  ncode.level,
  ncode.level_name,
  ncode.parent_code,
  ncode.parent_id,
  parent.code AS parent_nace_code,
  ncode.title,
  ncode.description,
  ncode.includes,
  ncode.excludes,
  ncode.active,
  ncode.created_at,
  ncode.updated_at
FROM nace_codes ncode
JOIN nace_classifications nc ON nc.id = ncode.classification_id
LEFT JOIN nace_codes parent ON parent.id = ncode.parent_id;

GRANT SELECT ON nace_classifications TO corpscout_anon;
GRANT SELECT ON nace_codes TO corpscout_anon;
GRANT SELECT ON nace_code_aliases TO corpscout_anon;
GRANT SELECT ON v_nace_taxonomy_state TO corpscout_anon;
GRANT SELECT ON v_nace_code_tree TO corpscout_anon;
```

- [ ] **Step 2: Create down migration**

Create `corpscout/database/migrations/000070_nace_taxonomy.down.sql`:

```sql
DROP VIEW IF EXISTS v_nace_code_tree;
DROP VIEW IF EXISTS v_nace_taxonomy_state;

DROP TABLE IF EXISTS nace_code_aliases;
DROP TABLE IF EXISTS nace_codes;
DROP TABLE IF EXISTS nace_classifications;
```

- [ ] **Step 3: Run migration tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db -run TestNACETaxonomy -count=1
```

Expected: pass.

- [ ] **Step 4: Commit migration and tests**

```bash
git add corpscout/database/migrations/000070_nace_taxonomy.* corpscout/scheduler/internal/db/nace_taxonomy_migration_test.go
git commit -m "feat: add nace taxonomy schema"
```

## Task 3: sqlc Queries

**Files:**

- Create: `corpscout/database/queries/nace_taxonomy.sql`
- Generated: `corpscout/scheduler/internal/db/gen/*.go`

- [ ] **Step 1: Create sqlc query file**

Create `corpscout/database/queries/nace_taxonomy.sql`:

```sql
-- name: UpsertNACEClassification :one
INSERT INTO nace_classifications (
  code_system,
  revision,
  name,
  valid_from,
  valid_to,
  source_url,
  source_metadata
) VALUES ('NACE', $1, $2, $3, $4, $5, $6)
ON CONFLICT (code_system, revision)
DO UPDATE SET
  name = EXCLUDED.name,
  valid_from = EXCLUDED.valid_from,
  valid_to = EXCLUDED.valid_to,
  source_url = EXCLUDED.source_url,
  source_metadata = EXCLUDED.source_metadata,
  updated_at = now()
RETURNING *;

-- name: UpsertNACECode :one
INSERT INTO nace_codes (
  classification_id,
  code,
  normalized_code,
  level,
  level_name,
  parent_code,
  title,
  description,
  includes,
  excludes,
  notes,
  source_payload,
  source_hash,
  active
) VALUES (
  $1, $2, $3, $4, $5, $6, $7,
  $8, $9, $10, $11, $12, $13, true
)
ON CONFLICT (classification_id, code)
DO UPDATE SET
  normalized_code = EXCLUDED.normalized_code,
  level = EXCLUDED.level,
  level_name = EXCLUDED.level_name,
  parent_code = EXCLUDED.parent_code,
  title = EXCLUDED.title,
  description = EXCLUDED.description,
  includes = EXCLUDED.includes,
  excludes = EXCLUDED.excludes,
  notes = EXCLUDED.notes,
  source_payload = EXCLUDED.source_payload,
  source_hash = EXCLUDED.source_hash,
  active = true,
  updated_at = CASE
    WHEN nace_codes.source_hash IS DISTINCT FROM EXCLUDED.source_hash THEN now()
    ELSE nace_codes.updated_at
  END
RETURNING *;

-- name: LinkNACECodeParents :exec
UPDATE nace_codes child
SET parent_id = parent.id,
    updated_at = CASE
      WHEN child.parent_id IS DISTINCT FROM parent.id THEN now()
      ELSE child.updated_at
    END
FROM nace_codes parent
WHERE child.classification_id = $1
  AND parent.classification_id = child.classification_id
  AND child.parent_code IS NOT NULL
  AND child.parent_code = parent.code;

-- name: ClearRootNACECodeParents :exec
UPDATE nace_codes
SET parent_id = NULL,
    updated_at = now()
WHERE classification_id = $1
  AND parent_code IS NULL
  AND parent_id IS NOT NULL;

-- name: DeactivateMissingNACECodes :one
WITH active_input_codes AS (
  SELECT unnest(sqlc.arg('active_codes')::text[]) AS code
),
updated AS (
  UPDATE nace_codes nc
  SET active = false,
      updated_at = now()
  WHERE nc.classification_id = sqlc.arg('classification_id')::uuid
    AND nc.active
    AND NOT EXISTS (
      SELECT 1 FROM active_input_codes input_codes WHERE input_codes.code = nc.code
    )
  RETURNING 1
)
SELECT count(*)::integer AS deactivated_count FROM updated;

-- name: UpsertNACECodeAlias :exec
INSERT INTO nace_code_aliases (
  nace_code_id,
  alias_type,
  alias_code,
  normalized_alias_code,
  source,
  metadata
) VALUES ($1, $2, $3, $4, 'nace', $5)
ON CONFLICT (nace_code_id, alias_type, alias_code)
DO UPDATE SET
  normalized_alias_code = EXCLUDED.normalized_alias_code,
  metadata = EXCLUDED.metadata;

-- name: GetNACEClassificationByRevision :one
SELECT * FROM nace_classifications
WHERE code_system = 'NACE'
  AND revision = $1;

-- name: GetNACECodeByRevisionAndCode :one
SELECT ncodes.*
FROM nace_codes ncodes
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
  AND nclass.revision = $1
  AND ncodes.code = $2
  AND ncodes.active;

-- name: ResolveNACECodeAlias :one
SELECT ncodes.*
FROM nace_code_aliases aliases
JOIN nace_codes ncodes ON ncodes.id = aliases.nace_code_id
JOIN nace_classifications nclass ON nclass.id = ncodes.classification_id
WHERE nclass.code_system = 'NACE'
  AND nclass.revision = $1
  AND aliases.source = 'nace'
  AND aliases.normalized_alias_code = $2
  AND ncodes.active
ORDER BY ncodes.level DESC
LIMIT 1;

-- name: ListNACECodeTree :many
SELECT * FROM v_nace_code_tree
WHERE code_system = 'NACE'
  AND revision = $1
  AND active = true
ORDER BY level, code;

-- name: ListNACETaxonomyState :many
SELECT * FROM v_nace_taxonomy_state
ORDER BY revision;
```

- [ ] **Step 2: Regenerate sqlc**

Run:

```bash
cd corpscout/database
sqlc generate
```

Expected generated methods include:

- `UpsertNACEClassification`
- `UpsertNACECode`
- `LinkNACECodeParents`
- `DeactivateMissingNACECodes`
- `ResolveNACECodeAlias`
- `ListNACECodeTree`

- [ ] **Step 3: Run generated package tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db/gen -count=1
```

Expected: pass.

- [ ] **Step 4: Commit query and generated code**

```bash
git add corpscout/database/queries/nace_taxonomy.sql corpscout/scheduler/internal/db/gen
git commit -m "feat: add nace taxonomy queries"
```

## Task 4: Minimal Go Helper Tests For Code Normalization

**Files:**

- Create: `corpscout/scheduler/internal/nacetaxonomy/code.go`
- Create: `corpscout/scheduler/internal/nacetaxonomy/code_test.go`

Why this package exists:

- The database stores `normalized_code`.
- Later importers and BRREG mapping must normalize codes the same way.
- This package is not a service abstraction and does not hide SQL/Temporal.

- [ ] **Step 1: Write tests**

Create `corpscout/scheduler/internal/nacetaxonomy/code_test.go`:

```go
package nacetaxonomy

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeCode(t *testing.T) {
	require.Equal(t, "6820", NormalizeCode("68.20"))
	require.Equal(t, "682", NormalizeCode("68.2"))
	require.Equal(t, "L", NormalizeCode(" l "))
}

func TestLevelNameForCode(t *testing.T) {
	require.Equal(t, "section", LevelNameForCode("L"))
	require.Equal(t, "division", LevelNameForCode("68"))
	require.Equal(t, "group", LevelNameForCode("68.2"))
	require.Equal(t, "class", LevelNameForCode("68.20"))
}

func TestLevelForCode(t *testing.T) {
	require.Equal(t, int16(1), LevelForCode("L"))
	require.Equal(t, int16(2), LevelForCode("68"))
	require.Equal(t, int16(3), LevelForCode("68.2"))
	require.Equal(t, int16(4), LevelForCode("68.20"))
}

func TestNACEClassFromNorwegianSNCode(t *testing.T) {
	require.Equal(t, "68.20", ClassFromNorwegianSNCode("68.200"))
	require.Equal(t, "01.11", ClassFromNorwegianSNCode("01.110"))
	require.Empty(t, ClassFromNorwegianSNCode("L"))
	require.Empty(t, ClassFromNorwegianSNCode("68.20"))
}
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/nacetaxonomy -count=1
```

Expected: fail because the package does not exist.

- [ ] **Step 3: Implement helper package**

Create `corpscout/scheduler/internal/nacetaxonomy/code.go`:

```go
package nacetaxonomy

import (
	"strings"
	"unicode"
)

func NormalizeCode(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		if r == '.' || r == '-' || unicode.IsSpace(r) {
			continue
		}
		b.WriteRune(unicode.ToUpper(r))
	}
	return b.String()
}

func LevelForCode(value string) int16 {
	normalized := NormalizeCode(value)
	switch {
	case len(normalized) == 1 && normalized[0] >= 'A' && normalized[0] <= 'Z':
		return 1
	case len(normalized) == 2:
		return 2
	case len(normalized) == 3:
		return 3
	case len(normalized) == 4:
		return 4
	default:
		return 0
	}
}

func LevelNameForCode(value string) string {
	switch LevelForCode(value) {
	case 1:
		return "section"
	case 2:
		return "division"
	case 3:
		return "group"
	case 4:
		return "class"
	default:
		return ""
	}
}

func ClassFromNorwegianSNCode(value string) string {
	normalized := NormalizeCode(value)
	if len(normalized) != 5 {
		return ""
	}
	return normalized[:2] + "." + normalized[2:4]
}
```

- [ ] **Step 4: Run helper tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/nacetaxonomy -count=1
```

Expected: pass.

- [ ] **Step 5: Commit helper package**

```bash
git add corpscout/scheduler/internal/nacetaxonomy
git commit -m "feat: add nace code helpers"
```

## Task 5: Integration With Existing Schema Without Changing Behavior

**Files:**

- No schema changes unless tests reveal grants or query type issues.

- [ ] **Step 1: Run focused DB tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./internal/db ./internal/db/gen ./internal/nacetaxonomy -count=1
```

Expected: pass.

- [ ] **Step 2: Run all scheduler tests**

Run:

```bash
cd corpscout/scheduler
GOWORK=off go test ./...
```

Expected: pass.

- [ ] **Step 3: Validate migration stack with sqlc**

Run:

```bash
cd corpscout/database
sqlc generate
```

Expected: no errors.

- [ ] **Step 4: Validate no BRREG workflow code changed**

Run:

```bash
git diff --name-only | grep 'corpscout/scheduler/internal/brreg' || true
```

Expected: no output. This task creates canonical taxonomy only.

- [ ] **Step 5: Commit any fixes from verification**

If verification required changes:

```bash
git add <changed-files>
git commit -m "fix: stabilize nace taxonomy schema"
```

If no changes were required, do not create a commit.

## Task 6: Manual Database Verification

**Files:**

- No source files.

- [ ] **Step 1: Apply migrations locally**

Use the repo's existing migration path:

```bash
cd corpscout
docker compose up migrate
```

Expected: migration `000070_nace_taxonomy` applies successfully.

- [ ] **Step 2: Verify tables exist**

Run:

```bash
psql "$CORPSCOUT_DATABASE_URL" -c "\\dt nace_*"
psql "$CORPSCOUT_DATABASE_URL" -c "\\dv v_nace_*"
```

Expected:

```text
nace_classifications
nace_codes
nace_code_aliases
v_nace_taxonomy_state
v_nace_code_tree
```

- [ ] **Step 3: Insert one tiny sample by SQL for validation**

Run:

```bash
psql "$CORPSCOUT_DATABASE_URL" <<'SQL'
INSERT INTO nace_classifications (revision, name, valid_from, source_url, source_metadata)
VALUES ('2.1', 'NACE Rev. 2.1', '2025-01-01', 'https://ec.europa.eu/eurostat/web/nace/nace-rev.-2.1', '{"verification":"manual"}')
ON CONFLICT (code_system, revision) DO NOTHING;

WITH class AS (
  SELECT id FROM nace_classifications WHERE code_system = 'NACE' AND revision = '2.1'
)
INSERT INTO nace_codes (classification_id, code, normalized_code, level, level_name, parent_code, title, source_hash)
SELECT id, 'L', 'L', 1, 'section', NULL, 'Real estate activities', 'manual-section-l'
FROM class
ON CONFLICT (classification_id, code) DO NOTHING;

WITH class AS (
  SELECT id FROM nace_classifications WHERE code_system = 'NACE' AND revision = '2.1'
)
INSERT INTO nace_codes (classification_id, code, normalized_code, level, level_name, parent_code, title, source_hash)
SELECT id, '68.20', '6820', 4, 'class', '68.2', 'Renting and operating of own or leased real estate', 'manual-6820'
FROM class
ON CONFLICT (classification_id, code) DO NOTHING;

SELECT revision, active_codes, sections, classes FROM v_nace_taxonomy_state;
SQL
```

Expected: at least one state row for revision `2.1`; sample counts reflect inserted rows.

- [ ] **Step 4: Roll back sample if needed**

If the local database should remain clean:

```bash
psql "$CORPSCOUT_DATABASE_URL" -c "DELETE FROM nace_classifications WHERE revision = '2.1' AND source_metadata->>'verification' = 'manual';"
```

Expected: sample rows cascade-delete from `nace_codes` and `nace_code_aliases`.

## Follow-Up Plan After This One

Create a separate plan for BRREG mapping:

- Add `brreg_workflow.nace_mappings` or a BRREG-specific read model.
- Map `brreg_source_industries.code` / raw `naeringskode1..3` to canonical `nace_codes.id`.
- Use `nacetaxonomy.ClassFromNorwegianSNCode("68.200") == "68.20"`.
- Store mapping evidence with `raw_record_id`, source field name, original code, original description, mapped NACE code id, mapping method, and confidence.
- Only when publishing to global company/suggestion tables should the BRREG publisher use canonical NACE ids/codes.

Create another separate plan for updating company industry tables:

- Add `nace_code_id` to `company_industries` or create a new normalized `company_industry_codes`.
- Decide how to migrate existing text `industry` rows.
- Update suggestion tables and review/apply code.

## Self-Review

- Spec coverage: The plan creates proper canonical NACE taxonomy tables in Companycollect and avoids Norwegian-specific global rows.
- Scope control: It does not implement BRREG mapping, import workflow, UI, or company industry migration.
- Placeholder scan: No deferred placeholders are required to implement the listed tasks.
- Type consistency: Table names, query names, and helper names are consistent across tasks.
- Architecture consistency: No unnecessary interfaces or generic source abstractions are introduced.
