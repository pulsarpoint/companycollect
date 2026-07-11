// Package input reads bounded root-domain keyset pages from ClickHouse.
package input

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const pageQuery = `
SELECT root_domain
FROM
(
    SELECT root_domain
    FROM corpscout.commoncrawl_domains
    WHERE root_domain != '' AND root_domain > ?
    GROUP BY root_domain
)
ORDER BY root_domain
LIMIT ?`

func FetchPage(ctx context.Context, conn driver.Conn, cursor string, pageSize int) ([]string, error) {
	if pageSize <= 0 {
		pageSize = 5000
	}
	rows, err := conn.Query(ctx, pageQuery, cursor, pageSize)
	if err != nil {
		return nil, fmt.Errorf("query domain page: %w", err)
	}
	defer rows.Close()
	page := make([]string, 0, pageSize)
	for rows.Next() {
		var rootDomain string
		if err := rows.Scan(&rootDomain); err != nil {
			return nil, fmt.Errorf("scan domain page: %w", err)
		}
		page = append(page, rootDomain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read domain page: %w", err)
	}
	return page, nil
}
