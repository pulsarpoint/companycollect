// Package load bulk-copies the SQLite stage into the two corpscout ClickHouse tables over the
// native protocol.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	recordsTable = "corpscout.commoncrawl_domain_dns_records"
	scanTable    = "corpscout.commoncrawl_domain_dns_scan"
)

// chColumns returns the ch tag of each field of T in declaration order.
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

func insert[T any](ctx context.Context, conn driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	q := "INSERT INTO " + table + " (" + strings.Join(chColumns[T](), ", ") + ")"
	batch, err := conn.PrepareBatch(ctx, q)
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

// FromStore reads the stage for scanID and inserts records + domain summaries into ClickHouse.
func FromStore(ctx context.Context, conn driver.Conn, st *store.Store, scanID string) (int, int, error) {
	recs, err := st.StagedRecords(ctx, scanID)
	if err != nil {
		return 0, 0, err
	}
	nr, err := insert(ctx, conn, recordsTable, recs)
	if err != nil {
		return 0, 0, err
	}
	doms, err := st.StagedDomains(ctx, scanID)
	if err != nil {
		return nr, 0, err
	}
	nd, err := insert(ctx, conn, scanTable, doms)
	return nr, nd, err
}
