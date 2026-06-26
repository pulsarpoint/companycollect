// Package load inserts the worker's output rows into ClickHouse over the native protocol, so a
// box that produces Parquet can also push it to ClickHouse without the clickhouse-client binary.
package load

import (
	"context"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/parquet-go/parquet-go"

	"cc-enrich-worker/internal/output"
)

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

// Tables maps each output kind to its ClickHouse table.
var Tables = map[string]string{
	"domains":     "commoncrawl_domains",
	"tech":        "commoncrawl_technologies",
	"identifiers": "commoncrawl_company_identifiers",
	"profiles":    "commoncrawl_company_profile",
}

// Kinds is the load order (domains for industry; the three tech outputs).
var Kinds = []string{"domains", "tech", "identifiers", "profiles"}

// KindForFile infers the output kind from a "<prefix>-<kind>.parquet" filename.
func KindForFile(path string) (string, bool) {
	base := strings.TrimSuffix(filepath.Base(path), ".parquet")
	for _, k := range Kinds {
		if strings.HasSuffix(base, "-"+k) {
			return k, true
		}
	}
	return "", false
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

// FromFile reads a Parquet output file and inserts it into its ClickHouse table. Pass kind="" to
// infer it from the filename. Returns (table, rows inserted).
func FromFile(ctx context.Context, conn driver.Conn, path, kind string) (string, int, error) {
	if kind == "" {
		k, ok := KindForFile(path)
		if !ok {
			return "", 0, fmt.Errorf("cannot infer kind from %q (expected …-{domains,tech,identifiers,profiles}.parquet); pass --kind", path)
		}
		kind = k
	}
	table, ok := Tables[kind]
	if !ok {
		return "", 0, fmt.Errorf("unknown kind %q (want one of %v)", kind, Kinds)
	}
	switch kind {
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
