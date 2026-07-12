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
	"domains":      "commoncrawl_domains",
	"industries":   "commoncrawl_industries",
	"page_signals": "commoncrawl_page_signals",
	"metadata":     "commoncrawl_page_metadata",
	"contacts":     "commoncrawl_domain_contact_info",
	"tech":         "commoncrawl_page_technologies",
	"identifiers":  "commoncrawl_domain_identifiers",
	"security":     "commoncrawl_domain_security",
	"page_meta":    "commoncrawl_domain_page_meta",
}

// Kinds is the load order (domains is the parent master, written by every pass).
var Kinds = []string{"domains", "industries", "page_signals", "metadata", "contacts", "tech", "identifiers", "security", "page_meta"}

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
	case "domains":
		// Old shard output has the fat single-table domains.parquet; fan it into the split tables.
		legacy, err := domainsParquetIsLegacy(path)
		if err != nil {
			return table, 0, err
		}
		if legacy {
			res, err := loadLegacyDomains(ctx, conn, path)
			total := 0
			for _, r := range res {
				total += r.Rows
			}
			return table, total, err
		}
		rows, err := parquet.ReadFile[output.DomainRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "industries":
		rows, err := parquet.ReadFile[output.IndustryRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "page_signals":
		rows, err := parquet.ReadFile[output.PageSignalRow](path)
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
	case "security":
		rows, err := parquet.ReadFile[output.SecurityRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "page_meta":
		rows, err := parquet.ReadFile[output.PageMetaRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "metadata":
		rows, err := parquet.ReadFile[output.MetadataRow](path)
		if err != nil {
			return table, 0, err
		}
		n, err := Insert(ctx, conn, table, rows)
		return table, n, err
	case "contacts":
		rows, err := parquet.ReadFile[output.ContactRow](path)
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
	// An old shard dir has only a fat domains.parquet (no industries/page_signals files): fan it into
	// all three split tables, then skip those kinds in the normal loop.
	skipSplit := false
	domPath := filepath.Join(dir, "domains.parquet")
	if _, err := os.Stat(domPath); err == nil {
		legacy, err := domainsParquetIsLegacy(domPath)
		if err != nil {
			return out, fmt.Errorf("%s: %w", domPath, err)
		}
		if legacy {
			res, err := loadLegacyDomains(ctx, conn, domPath)
			out = append(out, res...)
			if err != nil {
				return out, fmt.Errorf("%s: %w", domPath, err)
			}
			skipSplit = true
		}
	}
	for _, k := range Kinds {
		if skipSplit && (k == "domains" || k == "industries" || k == "page_signals") {
			continue
		}
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
