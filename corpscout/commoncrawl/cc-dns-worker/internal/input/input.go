// Package input loads the list of root domains to scan from ClickHouse.
package input

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// DefaultQuery selects every distinct domain known to the commoncrawl pipeline.
const DefaultQuery = "SELECT DISTINCT root_domain FROM corpscout.commoncrawl_domains"

func applyLimit(q string, limit int) string {
	if limit <= 0 {
		return q
	}
	return fmt.Sprintf("%s LIMIT %d", q, limit)
}

// FromClickHouse runs query (default DefaultQuery) and returns the root_domain column.
func FromClickHouse(ctx context.Context, conn driver.Conn, query string, limit int) ([]string, error) {
	if query == "" {
		query = DefaultQuery
	}
	rows, err := conn.Query(ctx, applyLimit(query, limit))
	if err != nil {
		return nil, fmt.Errorf("query domains: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var d string
		if err := rows.Scan(&d); err != nil {
			return nil, err
		}
		if d != "" {
			out = append(out, d)
		}
	}
	return out, rows.Err()
}
