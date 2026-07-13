// Package clickhouseconn opens ClickHouse connections from the AXFR scanner's environment.
package clickhouseconn

import (
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Open connects using the scanner's CLICKHOUSE_* environment variables.
func Open() (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{environment("CLICKHOUSE_HOST", "localhost") + ":" + environment("CLICKHOUSE_NATIVE_PORT", "9000")},
		Auth: clickhouse.Auth{
			Database: environment("CLICKHOUSE_DATABASE", "corpscout"),
			Username: environment("CLICKHOUSE_USER", "default"),
			Password: environment("CLICKHOUSE_PASSWORD", ""),
		},
		MaxOpenConns: 24,
		MaxIdleConns: 24,
	})
}

func environment(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
