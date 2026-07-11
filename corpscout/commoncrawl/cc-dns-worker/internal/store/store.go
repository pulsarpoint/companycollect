// Package store owns the bounded, restartable SQLite work queues and output outboxes.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"cc-dns-worker/internal/model"

	_ "modernc.org/sqlite"
)

type Store struct {
	db   *sql.DB
	path string
}

func Open(path string) (*Store, error) {
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)"
	database, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open SQLite: %w", err)
	}
	database.SetMaxOpenConns(1)
	if _, err := database.ExecContext(context.Background(), boundedSchema); err != nil {
		_ = database.Close()
		return nil, fmt.Errorf("create bounded schema: %w", err)
	}
	return &Store{db: database, path: path}, nil
}

func (s *Store) Close() error { return s.db.Close() }

type OperationalStats struct {
	Source          SourceState
	DNS             WorkCounts
	AXFR            WorkCounts
	DNSRecords      int
	AXFRProbes      int
	AXFRZoneRecords int
	PageCount       int
	FreePages       int
	WALBytes        int64
}

func (s *Store) OperationalStats(ctx context.Context, scanID string) (OperationalStats, error) {
	var stats OperationalStats
	var err error
	if stats.Source, err = s.SourceState(ctx, scanID); err != nil {
		return OperationalStats{}, err
	}
	if stats.DNS, err = s.DNSWorkCounts(ctx, scanID); err != nil {
		return OperationalStats{}, err
	}
	if stats.AXFR, err = s.AXFRWorkCounts(ctx, scanID); err != nil {
		return OperationalStats{}, err
	}
	for table, destination := range map[string]*int{
		"dns_records": &stats.DNSRecords, "axfr_probes": &stats.AXFRProbes,
		"axfr_zone_records": &stats.AXFRZoneRecords,
	} {
		if err := s.db.QueryRowContext(ctx, "SELECT count(*) FROM "+table+" WHERE scan_id = ?", scanID).Scan(destination); err != nil {
			return OperationalStats{}, err
		}
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&stats.PageCount); err != nil {
		return OperationalStats{}, err
	}
	if err := s.db.QueryRowContext(ctx, `PRAGMA freelist_count`).Scan(&stats.FreePages); err != nil {
		return OperationalStats{}, err
	}
	if fileInfo, err := os.Stat(s.path + "-wal"); err == nil {
		stats.WALBytes = fileInfo.Size()
	}
	return stats, nil
}

type AXFRTarget struct {
	RootDomain string
	Endpoints  []model.NameserverEndpoint
}

type AXFREndpointKey struct {
	RootDomain   string
	NameServer   string
	NameServerIP string
}

type AXFRPriorState struct {
	HasDefinitive      bool
	AXFROpen           bool
	DefinitiveAt       time.Time
	DefinitiveScanID   string
	LastProbeVerdict   string
	LastProbeReason    string
	LastProbedAt       time.Time
	LastProbeRecords   uint64
	LastProbeBytes     uint64
	LastProbeTruncated bool
	DelegationActive   bool
	DelegationSeenAt   time.Time
}

type AXFRProbedEndpoint struct {
	AXFREndpointKey
	Verdict          string
	Reason           string
	ObservedAt       time.Time
	Records          uint64
	Bytes            uint64
	Truncated        bool
	StateObservedAt  time.Time
	Definitive       bool
	DelegationActive bool
}

func parseTS(value string) time.Time {
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}

func b2i(value bool) int {
	if value {
		return 1
	}
	return 0
}

func setScanRowEndpoints(row *model.ScanRow) {
	for _, endpoint := range row.Endpoints {
		row.NSEndpointNames = append(row.NSEndpointNames, endpoint.Name)
		row.NSEndpointIPs = append(row.NSEndpointIPs, endpoint.IP)
		row.NSEndpointScopes = append(row.NSEndpointScopes, endpoint.Scope)
		row.NSEndpointDialable = append(row.NSEndpointDialable, uint8(b2i(endpoint.Dialable)))
	}
}
