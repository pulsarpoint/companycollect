// Package load inserts the worker's output rows into ClickHouse over the native protocol, so a
// box that produces Parquet can also push it to ClickHouse without the clickhouse-client binary.
package load

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/output"
)

// Each output kind has ONE fixed filename "<kind>.parquet" and one table. The worker writes these
// exact names; the loader reads them. No filename parsing.
var Tables = map[string]string{
	"registry":    "commoncrawl_domain_registry",
	"domains":     "commoncrawl_domains",
	"tech":        "commoncrawl_technologies",
	"identifiers": "commoncrawl_company_identifiers",
	"profiles":    "commoncrawl_company_profile",
}

// Kinds is the load order (registry first — the parent; then per-pass outputs).
var Kinds = []string{"registry", "domains", "tech", "identifiers", "profiles"}

// Result is one loaded file.
type Result struct {
	Path, Table string
	Rows        int
}

// chColumns returns the `ch` tag of each field of T, in declaration order — the explicit column
// list for the INSERT, so a table with extra (defaulted) columns the struct doesn't carry is fine.
func chColumns[T any]() []string {
	rt := reflect.TypeOf(*new(T))
	cols := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		if c := rt.Field(i).Tag.Get("ch"); c != "" {
			cols = append(cols, c)
		}
	}
	return cols
}

// Insert batch-inserts rows into table over the native protocol (AppendStruct uses the `ch` tags).
func Insert[T any](ctx context.Context, conn driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	query := "INSERT INTO " + table + " (" + strings.Join(chColumns[T](), ", ") + ")"
	batch, err := conn.PrepareBatch(ctx, query)
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", table, err)
	}
	for i := range rows {
		if err := batch.AppendStruct(&rows[i]); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append %s row %d: %w", table, i, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send %s: %w", table, err)
	}
	return len(rows), nil
}

// FromFile reads one output Parquet and inserts it. kind="" infers it from the basename, which must
// be exactly "<kind>.parquet" (domains|tech|identifiers|profiles).
func FromFile(ctx context.Context, conn driver.Conn, path, kind string) (string, int, error) {
	if kind == "" {
		kind = strings.TrimSuffix(filepath.Base(path), ".parquet")
	}
	table, ok := Tables[kind]
	if !ok {
		return "", 0, fmt.Errorf("%q is not an output file (want <%s>.parquet, or pass --kind)", path, strings.Join(Kinds, "|"))
	}
	switch kind {
	case "registry":
		rows, err := parquet.ReadFile[output.RegistryRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "domains":
		rows, err := parquet.ReadFile[output.DomainRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "tech":
		rows, err := parquet.ReadFile[output.TechRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "identifiers":
		rows, err := parquet.ReadFile[output.IdentifierRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "profiles":
		rows, err := parquet.ReadFile[output.ProfileRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	}
	return table, 0, fmt.Errorf("unhandled kind %q", kind)
}

// FromDir inserts every <kind>.parquet present in dir (whichever the run produced).
func FromDir(ctx context.Context, conn driver.Conn, dir string) ([]Result, error) {
	var out []Result
	for _, k := range Kinds {
		p := filepath.Join(dir, k+".parquet")
		if _, err := os.Stat(p); err != nil {
			continue
		}
		table, n, err := FromFile(ctx, conn, p, k)
		if err != nil {
			return out, fmt.Errorf("%s: %w", p, err)
		}
		out = append(out, Result{Path: p, Table: table, Rows: n})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no %s.parquet in %s", "{"+strings.Join(Kinds, ",")+"}", dir)
	}
	return out, nil
}
