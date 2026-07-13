// Package axfrscan runs the AXFR security-observation pipeline.
//
// It reads the latest DNS delegation summaries from ClickHouse and owns its SQLite queue, probe pool,
// ClickHouse outbox, and statistics.
package axfrscan

import "time"

// Config controls one AXFR scan cycle.
type Config struct {
	ScanID         string
	MaxDomains     int
	Workers        int
	PerServerQPS   float64
	MaxRecords     int
	MaxBytes       int
	Timeout        time.Duration
	StatsInterval  time.Duration
	DomainPageSize int
	WorkCapacity   int
	ClaimBatch     int
	FlushBatch     int
	FlushInterval  time.Duration
}

func (config Config) withDefaults() Config {
	if config.Workers <= 0 {
		config.Workers = 50
	}
	if config.PerServerQPS <= 0 {
		config.PerServerQPS = 5
	}
	if config.MaxRecords <= 0 {
		config.MaxRecords = 50000
	}
	if config.MaxBytes <= 0 {
		config.MaxBytes = 64 << 20
	}
	if config.DomainPageSize <= 0 {
		config.DomainPageSize = 1000
	}
	if config.WorkCapacity <= 0 {
		config.WorkCapacity = 5000
	}
	if config.ClaimBatch <= 0 {
		config.ClaimBatch = 100
	}
	if config.FlushBatch <= 0 {
		config.FlushBatch = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 5 * time.Second
	}
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Second
	}
	return config
}
