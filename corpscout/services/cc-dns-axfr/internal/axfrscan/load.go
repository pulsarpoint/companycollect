package axfrscan

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cc-dns-axfr/internal/axfrprobe"
	"cc-dns-axfr/internal/model"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	axfrLatestTable   = "corpscout.dns_axfr_latest"
	axfrChangesTable  = "corpscout.dns_axfr_state_changes"
	recordIngestTable = "corpscout.commoncrawl_domain_dns_record_ingest"
)

type endpointKey struct {
	RootDomain   string
	NameServer   string
	NameServerIP string
}

type priorState struct {
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
	DelegationSeenAt   time.Time
}

type observedEndpoint struct {
	endpointKey
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

type latestRow struct {
	RootDomain         string    `ch:"root_domain"`
	NameServer         string    `ch:"name_server"`
	NameServerIP       string    `ch:"name_server_ip"`
	UpdatedAt          time.Time `ch:"updated_at"`
	DelegationActive   uint8     `ch:"delegation_active"`
	DelegationSeenAt   time.Time `ch:"delegation_seen_at"`
	LastProbeVerdict   string    `ch:"last_probe_verdict"`
	LastProbeReason    string    `ch:"last_probe_reason"`
	LastProbedAt       time.Time `ch:"last_probed_at"`
	LastProbeRecords   uint64    `ch:"last_probe_records"`
	LastProbeBytes     uint64    `ch:"last_probe_bytes"`
	LastProbeTruncated uint8     `ch:"last_probe_truncated"`
	HasDefinitiveState uint8     `ch:"has_definitive_state"`
	AXFROpen           uint8     `ch:"axfr_open"`
	DefinitiveAt       time.Time `ch:"definitive_at"`
	DefinitiveScanID   string    `ch:"definitive_scan_id"`
}

type stateChangeRow struct {
	RootDomain   string    `ch:"root_domain"`
	NameServer   string    `ch:"name_server"`
	NameServerIP string    `ch:"name_server_ip"`
	AXFROpen     uint8     `ch:"axfr_open"`
	ScanID       string    `ch:"scan_id"`
	ChangedAt    time.Time `ch:"changed_at"`
}

func flushReady(ctx context.Context, connection driver.Conn, store *axfrStore, scanID string, batchSize int) (int, error) {
	domains, err := store.ready(ctx, scanID, batchSize)
	if err != nil {
		return 0, fmt.Errorf("read ready AXFR domains: %w", err)
	}
	if len(domains) == 0 {
		return 0, nil
	}
	roots := make([]string, len(domains))
	for index, domain := range domains {
		roots[index] = domain.RootDomain
	}
	prior, err := loadPrior(ctx, connection, roots)
	if err != nil {
		return 0, err
	}
	endpoints := buildObservedEndpoints(domains, prior)
	if _, err := insertRows(ctx, connection, axfrChangesTable, buildStateChanges(endpoints, prior, scanID)); err != nil {
		return 0, fmt.Errorf("write AXFR changes: %w", err)
	}
	if _, err := insertRows(ctx, connection, axfrLatestTable, buildLatestRows(endpoints, prior, scanID)); err != nil {
		return 0, fmt.Errorf("write AXFR latest state: %w", err)
	}
	records := buildRecordObservationRows(domains, scanID)
	if _, err := insertRows(ctx, connection, recordIngestTable, records); err != nil {
		return 0, fmt.Errorf("write AXFR record ingest: %w", err)
	}
	if err := store.acknowledge(ctx, scanID, domains); err != nil {
		return 0, fmt.Errorf("acknowledge AXFR domains: %w", err)
	}
	return len(domains), nil
}

func loadPrior(ctx context.Context, connection driver.Conn, roots []string) (map[endpointKey]priorState, error) {
	states := map[endpointKey]priorState{}
	if len(roots) == 0 {
		return states, nil
	}
	rows, err := connection.Query(ctx, `SELECT root_domain, name_server, name_server_ip,
		has_definitive_state, axfr_open, definitive_at, definitive_scan_id,
		last_probe_verdict, last_probe_reason, last_probed_at,
		last_probe_records, last_probe_bytes, last_probe_truncated, delegation_seen_at
		FROM `+axfrLatestTable+` FINAL WHERE has(?, root_domain)`, roots)
	if err != nil {
		return nil, fmt.Errorf("query AXFR prior state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var key endpointKey
		var state priorState
		var hasDefinitive, open, truncated uint8
		if err := rows.Scan(&key.RootDomain, &key.NameServer, &key.NameServerIP,
			&hasDefinitive, &open, &state.DefinitiveAt, &state.DefinitiveScanID,
			&state.LastProbeVerdict, &state.LastProbeReason, &state.LastProbedAt,
			&state.LastProbeRecords, &state.LastProbeBytes, &truncated, &state.DelegationSeenAt); err != nil {
			return nil, fmt.Errorf("scan AXFR prior state: %w", err)
		}
		state.HasDefinitive = hasDefinitive != 0
		state.AXFROpen = open != 0
		state.LastProbeTruncated = truncated != 0
		states[key] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read AXFR prior state: %w", err)
	}
	return states, nil
}

func buildObservedEndpoints(domains []readyDomain, prior map[endpointKey]priorState) []observedEndpoint {
	var observed []observedEndpoint
	for _, domain := range domains {
		current := map[endpointKey]bool{}
		probesByIP := map[string]axfrprobe.AXFROutcome{}
		for _, probe := range domain.Probes {
			probesByIP[probe.NSIP] = probe
		}
		for _, endpoint := range domain.Endpoints {
			key := endpointKey{RootDomain: domain.RootDomain, NameServer: endpoint.Name, NameServerIP: endpoint.IP}
			current[key] = true
			probe, probed := probesByIP[endpoint.IP]
			if !probed {
				if _, existed := prior[key]; existed {
					observed = append(observed, observedEndpoint{
						endpointKey: key, StateObservedAt: domain.DelegationObservedAt, DelegationActive: true,
					})
				}
				continue
			}
			observed = append(observed, observedEndpoint{
				endpointKey: key, Verdict: string(probe.Verdict), Reason: string(probe.Reason),
				ObservedAt: probe.ObservedAt, Records: uint64(probe.Records), Bytes: uint64(probe.Bytes),
				Truncated: probe.Truncated, StateObservedAt: probe.ObservedAt,
				Definitive: probe.IsDefinitive(), DelegationActive: true,
			})
		}
		for key := range prior {
			if key.RootDomain == domain.RootDomain && !current[key] {
				observed = append(observed, observedEndpoint{endpointKey: key, StateObservedAt: domain.DelegationObservedAt})
			}
		}
	}
	return observed
}

func buildLatestRows(endpoints []observedEndpoint, prior map[endpointKey]priorState, scanID string) []latestRow {
	rows := make([]latestRow, 0, len(endpoints))
	for _, endpoint := range endpoints {
		state := prior[endpoint.endpointKey]
		row := latestRow{
			RootDomain: endpoint.RootDomain, NameServer: endpoint.NameServer,
			NameServerIP: endpoint.NameServerIP, UpdatedAt: endpoint.StateObservedAt.UTC(),
			DelegationActive: boolByte(endpoint.DelegationActive), DelegationSeenAt: state.DelegationSeenAt,
			LastProbeVerdict: state.LastProbeVerdict, LastProbeReason: state.LastProbeReason,
			LastProbedAt: state.LastProbedAt, LastProbeRecords: state.LastProbeRecords,
			LastProbeBytes: state.LastProbeBytes, LastProbeTruncated: boolByte(state.LastProbeTruncated),
			HasDefinitiveState: boolByte(state.HasDefinitive), AXFROpen: boolByte(state.AXFROpen),
			DefinitiveAt: state.DefinitiveAt, DefinitiveScanID: state.DefinitiveScanID,
		}
		if endpoint.DelegationActive {
			row.DelegationSeenAt = endpoint.StateObservedAt.UTC()
		}
		if endpoint.Verdict != "" {
			row.LastProbeVerdict, row.LastProbeReason = endpoint.Verdict, endpoint.Reason
			row.LastProbedAt = endpoint.ObservedAt.UTC()
			row.LastProbeRecords, row.LastProbeBytes = endpoint.Records, endpoint.Bytes
			row.LastProbeTruncated = boolByte(endpoint.Truncated)
			if endpoint.Definitive {
				row.HasDefinitiveState = 1
				row.AXFROpen = boolByte(endpoint.Verdict == string(axfrprobe.VerdictOpen))
				row.DefinitiveAt, row.DefinitiveScanID = endpoint.ObservedAt.UTC(), scanID
			}
		}
		rows = append(rows, row)
	}
	return rows
}

func buildStateChanges(endpoints []observedEndpoint, prior map[endpointKey]priorState, scanID string) []stateChangeRow {
	var changes []stateChangeRow
	for _, endpoint := range endpoints {
		if !endpoint.Definitive {
			continue
		}
		open := endpoint.Verdict == string(axfrprobe.VerdictOpen)
		state, exists := prior[endpoint.endpointKey]
		if exists && state.HasDefinitive && state.AXFROpen == open {
			continue
		}
		if (!exists || !state.HasDefinitive) && !open {
			continue
		}
		changes = append(changes, stateChangeRow{
			RootDomain: endpoint.RootDomain, NameServer: endpoint.NameServer,
			NameServerIP: endpoint.NameServerIP, AXFROpen: boolByte(open), ScanID: scanID,
			ChangedAt: endpoint.ObservedAt.UTC(),
		})
	}
	return changes
}

func buildRecordObservationRows(domains []readyDomain, scanID string) []model.RecordObservationRow {
	loadedAt := time.Now().UTC()
	var records []model.RecordObservationRow
	for _, domain := range domains {
		for _, record := range domain.Zone {
			observedAt := domain.DelegationObservedAt
			for _, probe := range domain.Probes {
				if probe.NSIP == record.NameServerIP {
					observedAt = probe.ObservedAt
					break
				}
			}
			records = append(records, model.RecordObservationRow{
				RootDomain: domain.RootDomain, Name: record.Name, RecordType: record.RecordType,
				TypeCode: record.TypeCode, ClassCode: record.ClassCode, Slot: record.Slot,
				Value: record.Value, RDataWire: record.RDataWire, Source: "axfr", Discovery: "axfr",
				NameServer: record.NameServer, NameServerIP: record.NameServerIP, ScanID: scanID,
				TTL: record.TTL, Priority: record.Priority, Rcode: record.Rcode,
				ObservedAt: observedAt.UTC(), LoadedAt: loadedAt,
			})
		}
	}
	return records
}

func insertRows[T any](ctx context.Context, connection driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	typeOfRow := reflect.TypeOf(*new(T))
	columns := make([]string, 0, typeOfRow.NumField())
	for index := range typeOfRow.NumField() {
		if column := typeOfRow.Field(index).Tag.Get("ch"); column != "" {
			columns = append(columns, column)
		}
	}
	batch, err := connection.PrepareBatch(ctx, "INSERT INTO "+table+" ("+strings.Join(columns, ", ")+")")
	if err != nil {
		return 0, err
	}
	for index := range rows {
		if err := batch.AppendStruct(&rows[index]); err != nil {
			_ = batch.Abort()
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(rows), nil
}

func boolByte(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
