// Package dnsscan runs the authoritative DNS observation pipeline.
//
// It owns its input cursor, SQLite work queue, resolver pool, ClickHouse outbox, and statistics. It
// neither imports nor coordinates with the AXFR scanner.
package dnsscan

import "time"

// Config controls one DNS scan cycle.
type Config struct {
	ScanID             string
	RunID              string
	MaxDomains         int
	Resolvers          []string
	DiscoveryQPS       float64
	DiscoveryInflight  int
	PerServerQPS       float64
	PerServerInflight  int
	HyperscalerQPS     float64
	Workers            int
	QueryTimeout       time.Duration
	BreakerThreshold   int
	BreakerCooldown    time.Duration
	StatsInterval      time.Duration
	HostnameEnrichment bool
	HostnameCap        int
	DomainPageSize     int
	WorkCapacity       int
	ClaimBatch         int
	FlushBatch         int
	FlushInterval      time.Duration
}

func (config Config) withDefaults() Config {
	if config.RunID == "" {
		config.RunID = config.ScanID
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.DiscoveryInflight <= 0 {
		config.DiscoveryInflight = 500
	}
	if config.DomainPageSize <= 0 {
		config.DomainPageSize = 5000
	}
	if config.WorkCapacity <= 0 {
		config.WorkCapacity = 20000
	}
	if config.ClaimBatch <= 0 {
		config.ClaimBatch = 2000
	}
	if config.FlushBatch <= 0 {
		config.FlushBatch = 500
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 5 * time.Second
	}
	return config
}
