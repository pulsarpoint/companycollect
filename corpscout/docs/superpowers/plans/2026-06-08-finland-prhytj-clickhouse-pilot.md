# Finland PRH YTJ ClickHouse Pilot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the retired all-Postgres company source-record plan with a Finland PRH YTJ ClickHouse pilot that preserves full API payloads, generates source-specific DDL from Parquet schemas, applies ClickHouse migrations, and imports the existing Finland export.

**Architecture:** PostgreSQL remains the control plane for source registry, Temporal runs, export manifests, central companies, brands, and curated relationships. ClickHouse stores source-specific Finland PRH YTJ facts and later read projections. The first implementation creates deterministic tooling and migrations for one source, not a universal company schema.

**Tech Stack:** Go 1.26, `golang-migrate`, ClickHouse, `clickhouse-local`, Docker Compose, Parquet exports, `log/slog`, `github.com/cockroachdb/errors`.

---

## Source Documents

- Design spec: `companycollect/corpscout/docs/superpowers/specs/2026-06-08-corpscout-clickhouse-company-store-design.md`
- Retired plan to annotate: `companycollect/corpscout/docs/superpowers/plans/2026-06-07-corpscout-clean-schema-replacement.md`
- Finland source package: `companycollect/companies/finland`
- Finland PRH YTJ source export: `companycollect/companies/data/finland/countrydata/sources/prhytj/exports/20260607T205519Z-prhytj`
- Corpscout Makefile: `companycollect/corpscout/Makefile`
- Corpscout Docker Compose: `companycollect/corpscout/docker-compose.yml`

## Scope

This plan builds the first working ClickHouse source ingestion slice:

1. Retire the older all-Postgres clean replacement plan.
2. Add ClickHouse local infrastructure and migration commands.
3. Add a deterministic Parquet-to-ClickHouse DDL generator.
4. Add Finland PRH YTJ source table config.
5. Generate and commit Finland PRH YTJ ClickHouse migrations.
6. Add raw payload export support so no PRH YTJ API fields are lost.
7. Add an import command that loads Finland PRH YTJ Parquet files into ClickHouse.

This plan does not implement the `/companies/search` UI, cross-source entity resolution, brand UI, or 20-country consolidation.

## File Structure

Create:

- `companycollect/corpscout/clickhouse/go.mod`
- `companycollect/corpscout/clickhouse/go.sum`
- `companycollect/corpscout/clickhouse/migrations/000001_create_databases.up.sql`
- `companycollect/corpscout/clickhouse/migrations/000001_create_databases.down.sql`
- `companycollect/corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql`
- `companycollect/corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql`
- `companycollect/corpscout/clickhouse/sources/finland_prhytj.yaml`
- `companycollect/corpscout/clickhouse/tools/parquetddl/main.go`
- `companycollect/corpscout/clickhouse/tools/parquetddl/config.go`
- `companycollect/corpscout/clickhouse/tools/parquetddl/config_test.go`
- `companycollect/corpscout/clickhouse/tools/parquetddl/describe.go`
- `companycollect/corpscout/clickhouse/tools/parquetddl/generate.go`
- `companycollect/corpscout/clickhouse/tools/parquetddl/generate_test.go`
- `companycollect/corpscout/clickhouse/tools/chimport/main.go`
- `companycollect/corpscout/clickhouse/tools/chimport/importer.go`
- `companycollect/corpscout/clickhouse/tools/chimport/importer_test.go`

Modify:

- `companycollect/corpscout/Makefile`
- `companycollect/corpscout/docker-compose.yml`
- `companycollect/corpscout/.env.example`
- `companycollect/companies/finland/prhytj/export_rows.go`
- `companycollect/companies/finland/prhytj/export.go`
- `companycollect/companies/finland/prhytj/parquet_writer.go`
- `companycollect/companies/finland/prhytj/export_rows_test.go`
- `companycollect/companies/finland/prhytj/export_test.go`
- `companycollect/corpscout/docs/superpowers/plans/2026-06-07-corpscout-clean-schema-replacement.md`

---

### Task 1: Retire The All-Postgres Replacement Plan

**Files:**
- Modify: `companycollect/corpscout/docs/superpowers/plans/2026-06-07-corpscout-clean-schema-replacement.md`

- [ ] **Step 1: Add a retirement banner**

Insert this block at the top of `companycollect/corpscout/docs/superpowers/plans/2026-06-07-corpscout-clean-schema-replacement.md`, before the existing title:

```markdown
> **Status: Retired**
>
> Do not execute this all-Postgres company/source-record replacement plan.
> Corpscout now uses PostgreSQL for source registry, workflow metadata, central
> identity, brands, and curated relationships, while ClickHouse stores
> high-volume source-specific company facts and projections.
>
> Replacement plan:
> `companycollect/corpscout/docs/superpowers/plans/2026-06-08-finland-prhytj-clickhouse-pilot.md`
```

- [ ] **Step 2: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/docs/superpowers/plans/2026-06-07-corpscout-clean-schema-replacement.md
git commit -m "docs: retire all postgres corpscout schema plan"
```

---

### Task 2: Add ClickHouse Compose And Migration Commands

**Files:**
- Modify: `companycollect/corpscout/docker-compose.yml`
- Modify: `companycollect/corpscout/Makefile`
- Modify: `companycollect/corpscout/.env.example`
- Create: `companycollect/corpscout/clickhouse/migrations/000001_create_databases.up.sql`
- Create: `companycollect/corpscout/clickhouse/migrations/000001_create_databases.down.sql`

- [ ] **Step 1: Create the first ClickHouse migration**

Create `companycollect/corpscout/clickhouse/migrations/000001_create_databases.up.sql`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout_sources;
CREATE DATABASE IF NOT EXISTS corpscout_projection;
```

Create `companycollect/corpscout/clickhouse/migrations/000001_create_databases.down.sql`:

```sql
DROP DATABASE IF EXISTS corpscout_projection;
DROP DATABASE IF EXISTS corpscout_sources;
```

- [ ] **Step 2: Add ClickHouse services to Docker Compose**

In `companycollect/corpscout/docker-compose.yml`, add these services after the existing `migrate` service:

```yaml
  clickhouse:
    image: clickhouse/clickhouse-server:25.5
    environment:
      CLICKHOUSE_DB: ${CLICKHOUSE_DB:-corpscout_sources}
      CLICKHOUSE_USER: ${CLICKHOUSE_USER:-default}
      CLICKHOUSE_PASSWORD: ${CLICKHOUSE_PASSWORD:-change-me}
    ports:
      - "8123:8123"
      - "9002:9000"
    volumes:
      - ./data/clickhouse:/var/lib/clickhouse

  clickhouse-migrate:
    image: migrate/migrate:v4.17.0
    depends_on:
      clickhouse:
        condition: service_started
    volumes:
      - ./clickhouse/migrations:/clickhouse-migrations
    command:
      - "-path=/clickhouse-migrations"
      - "-database=${CLICKHOUSE_MIGRATE_URL:-clickhouse://default:change-me@clickhouse:9000/corpscout_sources?x-multi-statement=true}"
      - "up"
```

- [ ] **Step 3: Add Makefile targets**

Modify the `.PHONY` line in `companycollect/corpscout/Makefile` to include:

```make
clickhouse-migrate-up clickhouse-migrate-down clickhouse-generate-finland-prhytj clickhouse-import-finland-prhytj
```

Add these variables near the existing `DATABASE_URL` variables:

```make
CLICKHOUSE_MIGRATE_URL := $(or $(CLICKHOUSE_MIGRATE_URL),$(shell sed -n 's/^CLICKHOUSE_MIGRATE_URL=//p' .env 2>/dev/null | tail -n 1),clickhouse://default:change-me@localhost:9002/corpscout_sources?x-multi-statement=true)
CLICKHOUSE_HTTP_URL := $(or $(CLICKHOUSE_HTTP_URL),$(shell sed -n 's/^CLICKHOUSE_HTTP_URL=//p' .env 2>/dev/null | tail -n 1),http://default:change-me@localhost:8123)
FINLAND_PRHYTJ_EXPORT_DIR ?= ../companies/data/finland/countrydata/sources/prhytj/exports/20260607T205519Z-prhytj
FINLAND_PRHYTJ_SOURCE_EXPORT_ID ?= 00000000-0000-0000-0000-000000000000
```

Add these targets after `migrate-test-down`:

```make
clickhouse-migrate-up:
	@docker compose run --rm clickhouse-migrate -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" up

clickhouse-migrate-down:
	@docker compose run --rm clickhouse-migrate -path=/clickhouse-migrations -database="$(CLICKHOUSE_MIGRATE_URL)" down 1

clickhouse-generate-finland-prhytj:
	cd clickhouse && GOWORK=off go run ./tools/parquetddl \
		--source finland_prhytj \
		--database corpscout_sources \
		--export-dir "$(FINLAND_PRHYTJ_EXPORT_DIR)" \
		--config sources/finland_prhytj.yaml \
		--out migrations/000002_create_finland_prhytj_tables.up.sql \
		--down-out migrations/000002_create_finland_prhytj_tables.down.sql

clickhouse-import-finland-prhytj:
	cd clickhouse && GOWORK=off go run ./tools/chimport \
		--clickhouse-url "$(CLICKHOUSE_HTTP_URL)" \
		--database corpscout_sources \
		--source-export-id "$(FINLAND_PRHYTJ_SOURCE_EXPORT_ID)" \
		--export-dir "$(FINLAND_PRHYTJ_EXPORT_DIR)" \
		--config sources/finland_prhytj.yaml
```

- [ ] **Step 4: Add `.env.example` settings**

Add this block to `companycollect/corpscout/.env.example` after the Postgres URLs:

```dotenv
# ClickHouse
CLICKHOUSE_DB=corpscout_sources
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=change-me
CLICKHOUSE_MIGRATE_URL=clickhouse://default:change-me@localhost:9002/corpscout_sources?x-multi-statement=true
CLICKHOUSE_HTTP_URL=http://default:change-me@localhost:8123
FINLAND_PRHYTJ_EXPORT_DIR=../companies/data/finland/countrydata/sources/prhytj/exports/20260607T205519Z-prhytj
FINLAND_PRHYTJ_SOURCE_EXPORT_ID=00000000-0000-0000-0000-000000000000
```

- [ ] **Step 5: Run the first migration**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose up -d clickhouse
make clickhouse-migrate-up
```

Expected: `1/u create_databases` is applied successfully.

- [ ] **Step 6: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/docker-compose.yml corpscout/Makefile corpscout/.env.example corpscout/clickhouse/migrations/000001_create_databases.up.sql corpscout/clickhouse/migrations/000001_create_databases.down.sql
git commit -m "feat: add clickhouse migration support"
```

---

### Task 3: Add The Parquet DDL Generator

**Files:**
- Create: `companycollect/corpscout/clickhouse/go.mod`
- Create: `companycollect/corpscout/clickhouse/tools/parquetddl/config.go`
- Create: `companycollect/corpscout/clickhouse/tools/parquetddl/config_test.go`
- Create: `companycollect/corpscout/clickhouse/tools/parquetddl/describe.go`
- Create: `companycollect/corpscout/clickhouse/tools/parquetddl/generate.go`
- Create: `companycollect/corpscout/clickhouse/tools/parquetddl/generate_test.go`
- Create: `companycollect/corpscout/clickhouse/tools/parquetddl/main.go`

- [ ] **Step 1: Create ClickHouse tooling module**

Create `companycollect/corpscout/clickhouse/go.mod`:

```go
module github.com/pulsarpoint/corpscout/clickhouse

go 1.26.1

require (
	github.com/cockroachdb/errors v1.13.0
	gopkg.in/yaml.v3 v3.0.1
)
```

- [ ] **Step 2: Write config tests**

Create `companycollect/corpscout/clickhouse/tools/parquetddl/config_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	cfg, err := parseConfig([]byte(`
database: corpscout_sources
source_prefix: fi_prhytj
tables:
  companies:
    parquet: companies.parquet
    table: fi_prhytj_companies
    engine: ReplacingMergeTree
    order_by: [business_id, source_run_id]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
`))
	require.NoError(t, err)
	require.Equal(t, "corpscout_sources", cfg.Database)
	require.Equal(t, "fi_prhytj", cfg.SourcePrefix)
	require.Equal(t, "companies.parquet", cfg.Tables["companies"].Parquet)
	require.Equal(t, []string{"business_id", "source_run_id"}, cfg.Tables["companies"].OrderBy)
	require.Equal(t, "UUID", cfg.Tables["companies"].InjectColumns["source_export_id"])
}
```

- [ ] **Step 3: Implement config parsing**

Create `companycollect/corpscout/clickhouse/tools/parquetddl/config.go`:

```go
package main

import (
	"sort"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Database     string                 `yaml:"database"`
	SourcePrefix string                 `yaml:"source_prefix"`
	Tables       map[string]TableConfig `yaml:"tables"`
}

type TableConfig struct {
	Parquet       string            `yaml:"parquet"`
	Table         string            `yaml:"table"`
	Engine        string            `yaml:"engine"`
	OrderBy       []string          `yaml:"order_by"`
	PartitionBy   string            `yaml:"partition_by"`
	InjectColumns map[string]string `yaml:"inject_columns"`
}

func parseConfig(body []byte) (Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return Config{}, errors.Wrap(err, "parse parquet ddl config")
	}
	if cfg.Database == "" {
		return Config{}, errors.New("database is required")
	}
	if len(cfg.Tables) == 0 {
		return Config{}, errors.New("at least one table is required")
	}
	for name, table := range cfg.Tables {
		if table.Parquet == "" {
			return Config{}, errors.Errorf("table %s parquet is required", name)
		}
		if table.Table == "" {
			return Config{}, errors.Errorf("table %s table name is required", name)
		}
		if table.Engine == "" {
			return Config{}, errors.Errorf("table %s engine is required", name)
		}
		if len(table.OrderBy) == 0 {
			return Config{}, errors.Errorf("table %s order_by is required", name)
		}
	}
	return cfg, nil
}

func sortedTableNames(tables map[string]TableConfig) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
```

- [ ] **Step 4: Write generator tests**

Create `companycollect/corpscout/clickhouse/tools/parquetddl/generate_test.go`:

```go
package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type fakeDescriber map[string][]Column

func (f fakeDescriber) Describe(path string) ([]Column, error) {
	return f[path], nil
}

func TestGenerateMigrationIsDeterministic(t *testing.T) {
	cfg := Config{
		Database: "corpscout_sources",
		Tables: map[string]TableConfig{
			"companies": {
				Parquet: "companies.parquet",
				Table:   "fi_prhytj_companies",
				Engine:  "ReplacingMergeTree",
				OrderBy: []string{"business_id", "source_run_id"},
				InjectColumns: map[string]string{
					"source_export_id": "UUID",
					"ingested_at":      "DateTime64(3, 'UTC')",
				},
			},
		},
	}
	describer := fakeDescriber{
		"/exports/companies.parquet": {
			{Name: "country_iso2", Type: "String"},
			{Name: "source_slug", Type: "String"},
			{Name: "business_id", Type: "String"},
			{Name: "source_run_id", Type: "String"},
		},
	}

	up, down, err := generateMigrations(cfg, "/exports", describer)
	require.NoError(t, err)
	require.Contains(t, up, "CREATE TABLE IF NOT EXISTS corpscout_sources.fi_prhytj_companies")
	require.Contains(t, up, "`source_export_id` UUID")
	require.Contains(t, up, "`ingested_at` DateTime64(3, 'UTC')")
	require.Contains(t, up, "ENGINE = ReplacingMergeTree")
	require.Contains(t, up, "ORDER BY (`business_id`, `source_run_id`)")
	require.Contains(t, down, "DROP TABLE IF EXISTS corpscout_sources.fi_prhytj_companies;")
	require.Equal(t, up, strings.TrimSuffix(up, "\n")+"\n")
}
```

- [ ] **Step 5: Implement describer**

Create `companycollect/corpscout/clickhouse/tools/parquetddl/describe.go`:

```go
package main

import (
	"bytes"
	"encoding/json"
	"os/exec"
	"strings"

	"github.com/cockroachdb/errors"
)

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type Describer interface {
	Describe(path string) ([]Column, error)
}

type ClickHouseLocalDescriber struct {
	Binary string
}

func (d ClickHouseLocalDescriber) Describe(path string) ([]Column, error) {
	binary := strings.TrimSpace(d.Binary)
	if binary == "" {
		binary = "clickhouse-local"
	}
	query := "DESCRIBE TABLE file('" + strings.ReplaceAll(path, "'", "\\'") + "', Parquet) FORMAT JSONEachRow"
	cmd := exec.Command(binary, "--query", query)
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, errors.Wrapf(err, "describe parquet schema stderr=%s", stderr.String())
	}

	var columns []Column
	for _, line := range strings.Split(strings.TrimSpace(stdout.String()), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var row struct {
			Name string `json:"name"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			return nil, errors.Wrap(err, "decode clickhouse describe row")
		}
		columns = append(columns, Column{Name: row.Name, Type: row.Type})
	}
	return columns, nil
}
```

- [ ] **Step 6: Implement SQL generation**

Create `companycollect/corpscout/clickhouse/tools/parquetddl/generate.go`:

```go
package main

import (
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

func generateMigrations(cfg Config, exportDir string, describer Describer) (string, string, error) {
	var up strings.Builder
	var down strings.Builder

	for _, name := range sortedTableNames(cfg.Tables) {
		table := cfg.Tables[name]
		columns, err := describer.Describe(filepath.Join(exportDir, table.Parquet))
		if err != nil {
			return "", "", errors.Wrapf(err, "describe parquet table %s", name)
		}
		if len(columns) == 0 {
			return "", "", errors.Errorf("table %s has no columns", name)
		}

		up.WriteString("CREATE TABLE IF NOT EXISTS ")
		up.WriteString(quoteIdent(cfg.Database))
		up.WriteString(".")
		up.WriteString(quoteIdent(table.Table))
		up.WriteString(" (\n")

		rendered := make([]string, 0, len(columns)+len(table.InjectColumns))
		for _, column := range columns {
			rendered = append(rendered, "  "+quoteIdent(column.Name)+" "+column.Type)
		}
		for _, injected := range sortedKeys(table.InjectColumns) {
			rendered = append(rendered, "  "+quoteIdent(injected)+" "+table.InjectColumns[injected])
		}
		up.WriteString(strings.Join(rendered, ",\n"))
		up.WriteString("\n)\n")
		up.WriteString("ENGINE = ")
		up.WriteString(table.Engine)
		up.WriteString("\n")
		if table.PartitionBy != "" {
			up.WriteString("PARTITION BY ")
			up.WriteString(table.PartitionBy)
			up.WriteString("\n")
		}
		up.WriteString("ORDER BY (")
		up.WriteString(joinQuoted(table.OrderBy))
		up.WriteString(");\n\n")

		down.WriteString("DROP TABLE IF EXISTS ")
		down.WriteString(quoteIdent(cfg.Database))
		down.WriteString(".")
		down.WriteString(quoteIdent(table.Table))
		down.WriteString(";\n")
	}

	return up.String(), down.String(), nil
}

func quoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func joinQuoted(values []string) string {
	quoted := make([]string, 0, len(values))
	for _, value := range values {
		quoted = append(quoted, quoteIdent(value))
	}
	return strings.Join(quoted, ", ")
}

func sortedKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
```

- [ ] **Step 7: Implement CLI**

Create `companycollect/corpscout/clickhouse/tools/parquetddl/main.go`:

```go
package main

import (
	"flag"
	"os"

	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	var source string
	var database string
	var exportDir string
	var configPath string
	var outPath string
	var downOutPath string
	var clickhouseLocal string
	flag.StringVar(&source, "source", "", "source name for logs")
	flag.StringVar(&database, "database", "", "ClickHouse database override")
	flag.StringVar(&exportDir, "export-dir", "", "source export directory")
	flag.StringVar(&configPath, "config", "", "source config YAML")
	flag.StringVar(&outPath, "out", "", "up migration output path")
	flag.StringVar(&downOutPath, "down-out", "", "down migration output path")
	flag.StringVar(&clickhouseLocal, "clickhouse-local", "clickhouse-local", "clickhouse-local binary")
	flag.Parse()

	if source == "" {
		return errors.New("source is required")
	}
	if exportDir == "" {
		return errors.New("export-dir is required")
	}
	if configPath == "" {
		return errors.New("config is required")
	}
	if outPath == "" {
		return errors.New("out is required")
	}
	if downOutPath == "" {
		return errors.New("down-out is required")
	}

	body, err := os.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, "read config")
	}
	cfg, err := parseConfig(body)
	if err != nil {
		return err
	}
	if database != "" {
		cfg.Database = database
	}

	up, down, err := generateMigrations(cfg, exportDir, ClickHouseLocalDescriber{Binary: clickhouseLocal})
	if err != nil {
		return err
	}
	if err := os.WriteFile(outPath, []byte(up), 0o644); err != nil {
		return errors.Wrap(err, "write up migration")
	}
	if err := os.WriteFile(downOutPath, []byte(down), 0o644); err != nil {
		return errors.Wrap(err, "write down migration")
	}
	return nil
}
```

- [ ] **Step 8: Add missing test dependency**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go get github.com/stretchr/testify@v1.9.0
GOWORK=off go mod tidy
```

Expected: `go.mod` and `go.sum` are updated.

- [ ] **Step 9: Run generator tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./tools/parquetddl -count=1
```

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/go.mod corpscout/clickhouse/go.sum corpscout/clickhouse/tools/parquetddl
git commit -m "feat: add clickhouse parquet ddl generator"
```

---

### Task 4: Add Finland PRH YTJ ClickHouse Source Config

**Files:**
- Create: `companycollect/corpscout/clickhouse/sources/finland_prhytj.yaml`

- [ ] **Step 1: Create source config**

Create `companycollect/corpscout/clickhouse/sources/finland_prhytj.yaml`:

```yaml
database: corpscout_sources
source_prefix: fi_prhytj
tables:
  raw_records:
    parquet: raw_records.parquet
    table: fi_prhytj_raw_records
    engine: ReplacingMergeTree
    order_by: [source_run_id, business_id, source_payload_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  companies:
    parquet: companies.parquet
    table: fi_prhytj_companies
    engine: ReplacingMergeTree
    order_by: [business_id, source_run_id]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  company_names:
    parquet: company_names.parquet
    table: fi_prhytj_company_names
    engine: ReplacingMergeTree
    order_by: [business_id, source_position, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  legal_forms:
    parquet: legal_forms.parquet
    table: fi_prhytj_legal_forms
    engine: ReplacingMergeTree
    order_by: [business_id, registered_on, legal_form_code, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  industries:
    parquet: industries.parquet
    table: fi_prhytj_industries
    engine: ReplacingMergeTree
    order_by: [business_id, source_industry_code, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  addresses:
    parquet: addresses.parquet
    table: fi_prhytj_addresses
    engine: ReplacingMergeTree
    order_by: [business_id, address_type_code, source_position, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  registered_entries:
    parquet: registered_entries.parquet
    table: fi_prhytj_registered_entries
    engine: ReplacingMergeTree
    order_by: [business_id, register_code, entry_type_code, registered_on, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  tax_registrations:
    parquet: tax_registrations.parquet
    table: fi_prhytj_tax_registrations
    engine: ReplacingMergeTree
    order_by: [business_id, registration_type, register_code, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
  websites:
    parquet: websites.parquet
    table: fi_prhytj_websites
    engine: ReplacingMergeTree
    order_by: [host, business_id, source_item_hash]
    inject_columns:
      source_export_id: UUID
      ingested_at: "DateTime64(3, 'UTC')"
```

- [ ] **Step 2: Validate config with tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./tools/parquetddl -count=1
```

Expected: PASS.

- [ ] **Step 3: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/sources/finland_prhytj.yaml
git commit -m "feat: configure finland prhytj clickhouse tables"
```

---

### Task 5: Preserve Full PRH YTJ Raw Payloads In Export

**Files:**
- Modify: `companycollect/companies/finland/prhytj/export_rows.go`
- Modify: `companycollect/companies/finland/prhytj/export.go`
- Modify: `companycollect/companies/finland/prhytj/parquet_writer.go`
- Modify: `companycollect/companies/finland/prhytj/export_rows_test.go`
- Modify: `companycollect/companies/finland/prhytj/export_test.go`

- [ ] **Step 1: Add raw record row type**

In `companycollect/companies/finland/prhytj/export_rows.go`, add `RawRecords` to `ExportRows`:

```go
type ExportRows struct {
	RawRecords        []RawRecordExportRow
	Companies         []CompanyExportRow
	CompanyNames      []CompanyNameExportRow
	LegalForms        []LegalFormExportRow
	Industries        []IndustryExportRow
	Addresses         []AddressExportRow
	RegisteredEntries []RegisteredEntryExportRow
	TaxRegistrations  []TaxRegistrationExportRow
	Websites          []WebsiteExportRow
}
```

Add this row type above `CompanyExportRow`:

```go
type RawRecordExportRow struct {
	CountryISO2         string `parquet:"country_iso2"`
	SourceSlug          string `parquet:"source_slug"`
	SourceRunID         string `parquet:"source_run_id"`
	SourceRecordID      string `parquet:"source_record_id"`
	BusinessID          string `parquet:"business_id"`
	SourcePayloadHash   string `parquet:"source_payload_hash"`
	SnapshotPath        string `parquet:"snapshot_path"`
	SnapshotSHA256      string `parquet:"snapshot_sha256"`
	SnapshotLineNumber  int64  `parquet:"snapshot_line_number"`
	RawPayloadJSON      string `parquet:"raw_payload_json"`
	SchemaVersion       string `parquet:"schema_version"`
	ExportedAt          string `parquet:"exported_at"`
}
```

- [ ] **Step 2: Add raw projection helper**

In `companycollect/companies/finland/prhytj/export_rows.go`, add this function:

```go
func ProjectRawRecordExportRow(record CompanyRecord, runID string, snapshotPath string, snapshotSHA256 string, lineNumber int64, exportedAt string) RawRecordExportRow {
	businessID := strings.TrimSpace(record.BusinessID.Value)
	return RawRecordExportRow{
		CountryISO2:        "FI",
		SourceSlug:         SourceSlug,
		SourceRunID:        runID,
		SourceRecordID:     businessID,
		BusinessID:         businessID,
		SourcePayloadHash:  record.PayloadHash,
		SnapshotPath:       snapshotPath,
		SnapshotSHA256:     snapshotSHA256,
		SnapshotLineNumber: lineNumber,
		RawPayloadJSON:     string(record.RawPayload),
		SchemaVersion:      SourceExportSchemaVersion,
		ExportedAt:         exportedAt,
	}
}
```

- [ ] **Step 3: Pass snapshot metadata through export reading**

Change the signature of `readExportRows` in `companycollect/companies/finland/prhytj/export.go` from:

```go
func readExportRows(ctx context.Context, snapshotPath string, runID string, limit int64) (ExportRows, int64, int64, int64, error)
```

to:

```go
func readExportRows(ctx context.Context, snapshotPath string, snapshotSHA256 string, runID string, limit int64) (ExportRows, int64, int64, int64, error)
```

In `Export`, compute `snapshotSHA` before calling `readExportRows`, then pass it into the function:

```go
snapshotSHA, _, err := countryimport.HashFileSHA256(snapshotPath)
if err != nil {
	return result, countryimport.WrapSourceError(
		countryimport.ErrorKindFileIO,
		SourceSlug,
		"",
		snapshotPath,
		0,
		errors.Wrap(err, "hash PRH snapshot"),
	)
}

rows, recordsSeen, recordsExported, decodeErrors, err := readExportRows(ctx, snapshotPath, snapshotSHA, runID, opts.Limit)
```

Remove the later duplicate `snapshotSHA` hashing block in `Export`.

- [ ] **Step 4: Append raw records during export reading**

Inside `readExportRows`, after `record.PayloadHash` is assigned and before `appendExportRows`, add:

```go
exportedAt := time.Now().UTC().Format(time.RFC3339)
projected := ProjectExportRows(record, runID)
projected.RawRecords = append(projected.RawRecords, ProjectRawRecordExportRow(record, runID, snapshotPath, snapshotSHA256, lineNumber, exportedAt))
appendExportRows(&rows, projected)
```

Replace the existing direct call:

```go
appendExportRows(&rows, ProjectExportRows(record, runID))
```

- [ ] **Step 5: Preserve raw rows in append**

In `appendExportRows`, add:

```go
dst.RawRecords = append(dst.RawRecords, src.RawRecords...)
```

- [ ] **Step 6: Write `raw_records.parquet`**

In `companycollect/companies/finland/prhytj/parquet_writer.go`, add raw records to the file writing list using the existing writer helper pattern:

```go
if len(rows.RawRecords) > 0 {
	file, err := writeParquetFile(ctx, exportDir, "raw_records.parquet", rows.RawRecords)
	if err != nil {
		return nil, err
	}
	files = append(files, file)
}
```

Place it before `companies.parquet` so the manifest order is stable.

- [ ] **Step 7: Add focused raw record tests**

In `companycollect/companies/finland/prhytj/export_rows_test.go`, add:

```go
func TestProjectRawRecordExportRowPreservesPayload(t *testing.T) {
	raw := []byte(`{"businessId":{"value":"1234567-8"},"extra":{"field":"kept"}}`)
	record := CompanyRecord{
		BusinessID:  Identifier{Value: "1234567-8"},
		RawPayload:  raw,
		PayloadHash: "hash",
	}

	row := ProjectRawRecordExportRow(record, "run-1", "/snap.ndjson", "snapshot-hash", 42, "2026-06-08T00:00:00Z")

	require.Equal(t, "FI", row.CountryISO2)
	require.Equal(t, SourceSlug, row.SourceSlug)
	require.Equal(t, "run-1", row.SourceRunID)
	require.Equal(t, "1234567-8", row.SourceRecordID)
	require.Equal(t, "/snap.ndjson", row.SnapshotPath)
	require.Equal(t, "snapshot-hash", row.SnapshotSHA256)
	require.Equal(t, int64(42), row.SnapshotLineNumber)
	require.JSONEq(t, string(raw), row.RawPayloadJSON)
}
```

- [ ] **Step 8: Run Finland package tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/finland
GOWORK=off go test ./prhytj -count=1
```

Expected: PASS.

- [ ] **Step 9: Re-export Finland PRH YTJ source data**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/finland
GOWORK=off go run ./cmd/finland-countrydata export-source --source prhytj --data-dir ../data/finland/countrydata/sources/prhytj
```

Expected: a new export directory is created under:

```text
companycollect/companies/data/finland/countrydata/sources/prhytj/exports/*-prhytj
```

The new export manifest includes `raw_records.parquet`.

Capture the latest export path for later tasks:

```bash
export LATEST_PRHYTJ_EXPORT_DIR="$(ls -td /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/countrydata/sources/prhytj/exports/*-prhytj | head -n 1)"
test -f "$LATEST_PRHYTJ_EXPORT_DIR/raw_records.parquet"
```

- [ ] **Step 10: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add companies/finland/prhytj
git commit -m "feat: export raw finland prhytj records"
```

---

### Task 6: Generate Finland PRH YTJ ClickHouse Migrations

**Files:**
- Create: `companycollect/corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql`
- Create: `companycollect/corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql`

- [ ] **Step 1: Generate migration from latest export**

Use the latest PRH YTJ export that includes `raw_records.parquet`:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
export LATEST_PRHYTJ_EXPORT_DIR="$(ls -td /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/countrydata/sources/prhytj/exports/*-prhytj | head -n 1)"
FINLAND_PRHYTJ_EXPORT_DIR="$LATEST_PRHYTJ_EXPORT_DIR" make clickhouse-generate-finland-prhytj
```

Expected:

```text
clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql
clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql
```

The up migration contains all nine tables:

```text
fi_prhytj_raw_records
fi_prhytj_companies
fi_prhytj_company_names
fi_prhytj_legal_forms
fi_prhytj_industries
fi_prhytj_addresses
fi_prhytj_registered_entries
fi_prhytj_tax_registrations
fi_prhytj_websites
```

- [ ] **Step 2: Run deterministic generation check**

Run the same command twice:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
export LATEST_PRHYTJ_EXPORT_DIR="$(ls -td /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/countrydata/sources/prhytj/exports/*-prhytj | head -n 1)"
FINLAND_PRHYTJ_EXPORT_DIR="$LATEST_PRHYTJ_EXPORT_DIR" make clickhouse-generate-finland-prhytj
git diff --exit-code -- clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql
```

Expected: no diff after the second generation.

- [ ] **Step 3: Apply migrations**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make clickhouse-migrate-up
```

Expected: migrations `000001` and `000002` apply successfully.

- [ ] **Step 4: Inspect ClickHouse tables**

Run:

```bash
docker compose exec clickhouse clickhouse-client --query "
SELECT name
FROM system.tables
WHERE database = 'corpscout_sources'
  AND startsWith(name, 'fi_prhytj_')
ORDER BY name
"
```

Expected output includes the nine `fi_prhytj_*` tables.

- [ ] **Step 5: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.up.sql corpscout/clickhouse/migrations/000002_create_finland_prhytj_tables.down.sql
git commit -m "feat: add finland prhytj clickhouse tables"
```

---

### Task 7: Add Finland PRH YTJ ClickHouse Import Command

**Files:**
- Create: `companycollect/corpscout/clickhouse/tools/chimport/importer.go`
- Create: `companycollect/corpscout/clickhouse/tools/chimport/importer_test.go`
- Create: `companycollect/corpscout/clickhouse/tools/chimport/main.go`

- [ ] **Step 1: Write importer SQL test**

Create `companycollect/corpscout/clickhouse/tools/chimport/importer_test.go`:

```go
package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildInsertSQL(t *testing.T) {
	table := TableConfig{
		Parquet: "companies.parquet",
		Table:   "fi_prhytj_companies",
		InjectColumns: map[string]string{
			"source_export_id": "UUID",
			"ingested_at":      "DateTime64(3, 'UTC')",
		},
	}

	sql := buildInsertSQL("corpscout_sources", table, "/exports/companies.parquet", "11111111-1111-1111-1111-111111111111")

	require.Contains(t, sql, "INSERT INTO `corpscout_sources`.`fi_prhytj_companies`")
	require.Contains(t, sql, "SELECT *, toUUID('11111111-1111-1111-1111-111111111111') AS `source_export_id`, now64(3) AS `ingested_at`")
	require.Contains(t, sql, "FROM file('/exports/companies.parquet', Parquet)")
}
```

- [ ] **Step 2: Implement importer SQL builder**

Create `companycollect/corpscout/clickhouse/tools/chimport/importer.go`:

```go
package main

import (
	"bytes"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
)

type TableConfig struct {
	Parquet       string            `yaml:"parquet"`
	Table         string            `yaml:"table"`
	InjectColumns map[string]string `yaml:"inject_columns"`
}

func buildInsertSQL(database string, table TableConfig, parquetPath string, sourceExportID string) string {
	var injected []string
	for name := range table.InjectColumns {
		injected = append(injected, name)
	}
	sort.Strings(injected)

	var selectParts []string
	selectParts = append(selectParts, "*")
	for _, name := range injected {
		switch name {
		case "source_export_id":
			selectParts = append(selectParts, "toUUID('"+escapeSQL(sourceExportID)+"') AS "+quoteIdent(name))
		case "ingested_at":
			selectParts = append(selectParts, "now64(3) AS "+quoteIdent(name))
		}
	}

	return "INSERT INTO " + quoteIdent(database) + "." + quoteIdent(table.Table) + "\n" +
		"SELECT " + strings.Join(selectParts, ", ") + "\n" +
		"FROM file('" + escapeSQL(filepath.Clean(parquetPath)) + "', Parquet);\n"
}

func executeQuery(clickhouseURL string, sql string) error {
	req, err := http.NewRequest(http.MethodPost, clickhouseURL, bytes.NewBufferString(sql))
	if err != nil {
		return errors.Wrap(err, "create clickhouse request")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "execute clickhouse query")
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errors.Errorf("clickhouse query failed status=%d", resp.StatusCode)
	}
	return nil
}

func quoteIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func escapeSQL(value string) string {
	return strings.ReplaceAll(value, "'", "\\'")
}
```

- [ ] **Step 3: Implement import CLI**

Create `companycollect/corpscout/clickhouse/tools/chimport/main.go`:

```go
package main

import (
	"flag"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Database string                 `yaml:"database"`
	Tables   map[string]TableConfig `yaml:"tables"`
}

func main() {
	if err := run(); err != nil {
		_, _ = os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}
}

func run() error {
	var clickhouseURL string
	var database string
	var sourceExportID string
	var exportDir string
	var configPath string
	flag.StringVar(&clickhouseURL, "clickhouse-url", "", "ClickHouse HTTP URL")
	flag.StringVar(&database, "database", "", "ClickHouse database override")
	flag.StringVar(&sourceExportID, "source-export-id", "", "Postgres registry source export UUID")
	flag.StringVar(&exportDir, "export-dir", "", "source export directory")
	flag.StringVar(&configPath, "config", "", "source config YAML")
	flag.Parse()

	if clickhouseURL == "" {
		return errors.New("clickhouse-url is required")
	}
	if sourceExportID == "" {
		return errors.New("source-export-id is required")
	}
	if exportDir == "" {
		return errors.New("export-dir is required")
	}
	if configPath == "" {
		return errors.New("config is required")
	}

	body, err := os.ReadFile(configPath)
	if err != nil {
		return errors.Wrap(err, "read config")
	}
	var cfg Config
	if err := yaml.Unmarshal(body, &cfg); err != nil {
		return errors.Wrap(err, "parse config")
	}
	if database != "" {
		cfg.Database = database
	}
	if cfg.Database == "" {
		return errors.New("database is required")
	}

	names := sortedTableNames(cfg.Tables)
	for _, name := range names {
		table := cfg.Tables[name]
		sql := buildInsertSQL(cfg.Database, table, filepath.Join(exportDir, table.Parquet), sourceExportID)
		if err := executeQuery(clickhouseURL, sql); err != nil {
			return errors.Wrapf(err, "import table %s", table.Table)
		}
	}
	return nil
}

func sortedTableNames(tables map[string]TableConfig) []string {
	names := make([]string, 0, len(tables))
	for name := range tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
```

- [ ] **Step 4: Run importer tests**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./tools/chimport -count=1
```

Expected: PASS.

- [ ] **Step 5: Import Finland PRH YTJ data**

Run:

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
export LATEST_PRHYTJ_EXPORT_DIR="$(ls -td /Users/graovic/pulsarpoint/ppoint/companycollect/companies/data/finland/countrydata/sources/prhytj/exports/*-prhytj | head -n 1)"
FINLAND_PRHYTJ_EXPORT_DIR="$LATEST_PRHYTJ_EXPORT_DIR" \
FINLAND_PRHYTJ_SOURCE_EXPORT_ID="00000000-0000-0000-0000-000000000000" \
make clickhouse-import-finland-prhytj
```

Expected: all nine tables load rows. The zero UUID is acceptable only for local pilot testing until the Postgres registry export row exists.

- [ ] **Step 6: Verify row counts**

Run:

```bash
docker compose exec clickhouse clickhouse-client --query "
SELECT table, sum(rows) AS rows
FROM system.parts
WHERE database = 'corpscout_sources'
  AND startsWith(table, 'fi_prhytj_')
  AND active
GROUP BY table
ORDER BY table
"
```

Expected: non-zero counts for all tables except optional source tables with no records in the export.

- [ ] **Step 7: Commit**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git add corpscout/clickhouse/tools/chimport corpscout/clickhouse/go.mod corpscout/clickhouse/go.sum
git commit -m "feat: import finland prhytj parquet into clickhouse"
```

---

### Task 8: Final Verification

**Files:**
- Generated or modified files from previous tasks.

- [ ] **Step 1: Run Finland source tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/companies/finland
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 2: Run ClickHouse tool tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout/clickhouse
GOWORK=off go test ./...
```

Expected: PASS.

- [ ] **Step 3: Run Corpscout scheduler tests**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
make test
```

Expected: PASS or only pre-existing failures unrelated to ClickHouse tooling. Fix any compile errors introduced by this work.

- [ ] **Step 4: Verify ClickHouse migrations from scratch**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect/corpscout
docker compose down
rm -rf data/clickhouse
docker compose up -d clickhouse
make clickhouse-migrate-up
```

Expected: both ClickHouse migrations apply on an empty ClickHouse data directory.

- [ ] **Step 5: Commit final verification notes if any files changed**

```bash
cd /Users/graovic/pulsarpoint/ppoint/companycollect
git status --short
```

If files changed from fixes during verification:

```bash
git add corpscout companies/finland
git commit -m "chore: verify finland prhytj clickhouse pilot"
```

---

## Self-Review

Spec coverage:

- Source-specific ClickHouse tables are covered by Tasks 4 and 6.
- Deterministic Parquet-to-DDL generation is covered by Task 3.
- ClickHouse `golang-migrate` support is covered by Task 2.
- Full PRH YTJ raw payload preservation is covered by Task 5.
- Parquet import as a separate job is covered by Task 7.
- Retiring the all-Postgres plan is covered by Task 1.
- Verification across Finland package, ClickHouse tools, migrations, and scheduler tests is covered by Task 8.

Placeholder scan:

- This plan avoids placeholder tokens and open-ended implementation instructions.
- The export path used by later tasks is derived with `LATEST_PRHYTJ_EXPORT_DIR` after Task 5 creates the new raw-preserving export.

Type consistency:

- `source_export_id` is injected into every ClickHouse source table.
- `ingested_at` is injected into every ClickHouse source table.
- Finland source table names match the design spec.
- The generator and importer share the same YAML config shape for table names, Parquet file names, and injected columns.
