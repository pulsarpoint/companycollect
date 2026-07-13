package axfrscan

import (
	"context"
	"fmt"
	"time"

	"cc-dns-axfr/internal/axfrprobe"
	"cc-dns-axfr/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type sourceDomain struct {
	RootDomain string
	Endpoints  []model.NameserverEndpoint
	ObservedAt time.Time
}

func fetchSourcePage(ctx context.Context, connection driver.Conn, cursor string, limit int) ([]sourceDomain, error) {
	rows, err := connection.Query(ctx, `SELECT root_domain, ns_endpoint_names, ns_endpoint_ips, resolved_at
		FROM corpscout.commoncrawl_domain_dns_scan FINAL
		WHERE root_domain > ? AND status IN ('done', 'no_public_ns_endpoints')
		ORDER BY root_domain LIMIT ?`, cursor, limit)
	if err != nil {
		return nil, fmt.Errorf("query AXFR source after %q: %w", cursor, err)
	}
	defer rows.Close()

	var domains []sourceDomain
	for rows.Next() {
		var domain sourceDomain
		var names, ips []string
		if err := rows.Scan(&domain.RootDomain, &names, &ips, &domain.ObservedAt); err != nil {
			return nil, fmt.Errorf("scan AXFR source: %w", err)
		}
		if len(names) != len(ips) {
			return nil, fmt.Errorf("AXFR source %q has %d NS names and %d IPs", domain.RootDomain, len(names), len(ips))
		}
		for index := range names {
			scope, valid := axfrprobe.ClassifyString(ips[index])
			if !valid {
				scope = axfrprobe.ScopeReserved
			}
			domain.Endpoints = append(domain.Endpoints, model.NameserverEndpoint{
				Name: names[index], IP: ips[index], Scope: string(scope), Dialable: axfrprobe.Dialable(scope),
			})
		}
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read AXFR source: %w", err)
	}
	return domains, nil
}
