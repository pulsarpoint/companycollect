// Package hostsource reads bounded hostname batches from the durable Common Crawl registry.
package hostsource

import (
	"context"
	"fmt"

	"cc-dns-scan/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const batchQuery = `
SELECT
    root_domain,
    label,
    multiIf(priority = 3, 'axfr', priority = 2, 'ct', priority = 1, 'static', 'unknown') AS source,
    live_cert
FROM
(
    SELECT
        root_domain,
        label,
        max(multiIf(discovery_source = 'axfr', 3, discovery_source = 'ct', 2,
            discovery_source = 'static', 1, 0)) AS priority,
        max(discovery_source = 'ct' AND last_not_after >= now()) AS live_cert,
        max(last_seen) AS recency
    FROM corpscout.commoncrawl_domain_hostnames
    WHERE root_domain IN (?)
      AND root_domain != ''
      AND label != ''
      AND position(label, '*') = 0
    GROUP BY root_domain, label
)
ORDER BY root_domain, priority DESC, live_cert DESC, recency DESC, label
LIMIT ? BY root_domain`

// Fetch returns the ranked hostname labels for exactly roots. The bound root array keeps the query
// independent of any scan-wide membership table, and LIMIT BY bounds each domain's in-memory result.
func Fetch(ctx context.Context, conn driver.Conn, roots []string, capPerRoot int) (map[string][]model.HostLabel, error) {
	if len(roots) == 0 {
		return map[string][]model.HostLabel{}, nil
	}
	if capPerRoot <= 0 {
		capPerRoot = 100
	}
	rows, err := conn.Query(ctx, batchQuery, roots, capPerRoot)
	if err != nil {
		return nil, fmt.Errorf("query hostname registry: %w", err)
	}
	defer rows.Close()

	hosts := make(map[string][]model.HostLabel, len(roots))
	for rows.Next() {
		var rootDomain string
		var host model.HostLabel
		var live uint8
		if err := rows.Scan(&rootDomain, &host.Label, &host.DiscoverySource, &live); err != nil {
			return nil, fmt.Errorf("scan hostname registry row: %w", err)
		}
		host.LiveCert = live != 0
		hosts[rootDomain] = append(hosts[rootDomain], host)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read hostname registry rows: %w", err)
	}
	return hosts, nil
}
