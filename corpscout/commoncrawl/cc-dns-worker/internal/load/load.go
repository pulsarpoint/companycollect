// Package load bulk-copies the SQLite stage into the two corpscout ClickHouse tables over the
// native protocol.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	recordsTable   = "corpscout.commoncrawl_domain_dns_records"
	scanTable      = "corpscout.commoncrawl_domain_dns_scan"
	hostnamesTable = "corpscout.commoncrawl_domain_hostnames"
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

// FromStore reads the whole stage for scanID and inserts records + domain summaries into ClickHouse.
// Idempotent (the CH tables dedup on merge), so it is safe to re-run.
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

// Incremental loads every scan_records row committed since the persisted rowid watermark for scanID,
// in batches, advancing the watermark after each batch. It also loads the done-summaries for the
// domains in each batch. Because it walks scan_records by rowid (commit order), it never misses a
// late-finishing domain, and re-runs are safe (CH dedups). It returns the number of record rows
// loaded. Call it periodically during a scan and once more after the scan finishes.
func Incremental(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, batch int) (int, error) {
	if batch <= 0 {
		batch = 20000
	}
	w, err := st.LoadedRowid(ctx, scanID)
	if err != nil {
		return 0, err
	}
	total := 0
	for {
		recs, maxRowid, domains, err := st.RecordsAfter(ctx, scanID, w, batch)
		if err != nil {
			return total, err
		}
		if len(recs) == 0 {
			return total, nil
		}
		if _, err := insert(ctx, conn, recordsTable, recs); err != nil {
			return total, err
		}
		sums, err := st.SummariesFor(ctx, scanID, domains)
		if err != nil {
			return total, err
		}
		if _, err := insert(ctx, conn, scanTable, sums); err != nil {
			return total, err
		}
		// Advance the watermark only after CH accepted the batch — a crash before this just re-loads
		// the batch next time (idempotent).
		if err := st.SetLoadedRowid(ctx, scanID, maxRowid); err != nil {
			return total, err
		}
		w = maxRowid
		total += len(recs)
		if len(recs) < batch {
			return total, nil
		}
	}
}

// WriteHostnameRegistry upserts this cycle's non-static discovered hosts into the durable hostname
// registry. Blind INSERT — the AggregatingMergeTree folds first_seen=min, last_seen=max,
// last_resolved=max, discovery_source=min — so it is idempotent and needs no read-before-write.
func WriteHostnameRegistry(ctx context.Context, conn driver.Conn, st *store.Store, scanID string, now time.Time) (int, error) {
	rows, err := st.DiscoveredHostnames(ctx, scanID)
	if err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	now = now.UTC()
	for i := range rows {
		rows[i].FirstSeen, rows[i].LastSeen, rows[i].LastResolved = now, now, now
	}
	return insert(ctx, conn, hostnamesTable, rows)
}
