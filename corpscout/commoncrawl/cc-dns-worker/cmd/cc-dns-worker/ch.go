package main

import (
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// chConn connects to ClickHouse from CLICKHOUSE_* env. The variable names match cc-enrich-worker
// and cc-crawl exactly (CLICKHOUSE_HOST + CLICKHOUSE_NATIVE_PORT, CLICKHOUSE_DATABASE).
func chConn() (driver.Conn, error) {
	return clickhouse.Open(&clickhouse.Options{
		Addr: []string{envOr("CLICKHOUSE_HOST", "localhost") + ":" + envOr("CLICKHOUSE_NATIVE_PORT", "9000")},
		Auth: clickhouse.Auth{
			Database: envOr("CLICKHOUSE_DATABASE", "corpscout"),
			Username: envOr("CLICKHOUSE_USER", "default"),
			Password: envOr("CLICKHOUSE_PASSWORD", ""),
		},
	})
}
