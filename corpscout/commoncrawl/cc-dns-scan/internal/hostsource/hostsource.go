// Package hostsource reads bounded batches from the confirmed DNS hostname view.
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
    discovery_source
FROM
(
    SELECT
        root_domain,
        label,
        discovery_source,
        last_seen,
        multiIf(discovery_source = 'axfr', 3, discovery_source = 'ct', 2,
            discovery_source = 'static', 1, 0) AS priority
    FROM corpscout.domain_hostnames
    WHERE root_domain IN (?)
      AND root_domain != ''
      AND label != ''
      AND position(label, '*') = 0
)
ORDER BY root_domain, priority DESC, last_seen DESC, label
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
		return nil, fmt.Errorf("query confirmed hostname view: %w", err)
	}
	defer rows.Close()

	hosts := make(map[string][]model.HostLabel, len(roots))
	for rows.Next() {
		var rootDomain string
		var host model.HostLabel
		if err := rows.Scan(&rootDomain, &host.Label, &host.DiscoverySource); err != nil {
			return nil, fmt.Errorf("scan confirmed hostname row: %w", err)
		}
		hosts[rootDomain] = append(hosts[rootDomain], host)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read confirmed hostname rows: %w", err)
	}
	return hosts, nil
}
