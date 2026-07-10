// Package input loads the list of root domains to scan from ClickHouse.
package input

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultQuery selects every distinct domain known to the commoncrawl pipeline. The ORDER BY makes
// the result deterministic so a --max-domains run returns the SAME domains every time; without it
// ClickHouse may return a different LIMIT-N slice per run, which breaks resume-by-rescan (the second
// run would seed a different set instead of finding all domains already done). For a full (unlimited)
// run the order is immaterial — every domain is seeded regardless.
const DefaultQuery = "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains ORDER BY root_domain"

func applyLimit(q string, limit int) string {
	if limit <= 0 {
		return q
	}
	return fmt.Sprintf("%s LIMIT %d", q, limit)
}

// StreamClickHouse runs query (default DefaultQuery) and invokes fn once per batch of up to
// batchSize non-empty root_domains, plus once for the final partial batch. It never materializes the
// whole result set — peak memory is one batch. fn must consume the batch before returning; the
// backing slice is reused across calls.
func StreamClickHouse(ctx context.Context, conn driver.Conn, query string, limit, batchSize int, fn func(batch []string) error) error {
	if query == "" {
		query = DefaultQuery
	}
	rows, err := conn.Query(ctx, applyLimit(query, limit))
	if err != nil {
		return fmt.Errorf("query domains: %w", err)
	}
	defer rows.Close()
	return drainRows(rows.Next, func(p *string) error { return rows.Scan(p) }, rows.Err, batchSize, fn)
}

// drainRows is the ClickHouse-free batching core of StreamClickHouse: it pulls values via
// next()/scan, drops empty strings, groups into batches of batchSize, and calls fn per full batch
// plus a final partial batch. The batch slice is reused, so fn must not retain it.
func drainRows(next func() bool, scan func(*string) error, rowsErr func() error, batchSize int, fn func([]string) error) error {
	if batchSize <= 0 {
		batchSize = 5000
	}
	batch := make([]string, 0, batchSize)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := fn(batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	for next() {
		var d string
		if err := scan(&d); err != nil {
			return err
		}
		if d == "" {
			continue
		}
		batch = append(batch, d)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := rowsErr(); err != nil {
		return err
	}
	return flush()
}
