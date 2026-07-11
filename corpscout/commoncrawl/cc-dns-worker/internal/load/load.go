// Package load flushes bounded SQLite outboxes to retry-safe ClickHouse tables.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	recordObservationsTable = "corpscout.commoncrawl_domain_dns_record_observations"
	scanTable               = "corpscout.commoncrawl_domain_dns_scan"
	hostnamesTable          = "corpscout.commoncrawl_domain_hostnames"
	axfrLatestTable         = "corpscout.dns_axfr_latest"
	axfrChangesTable        = "corpscout.dns_axfr_state_changes"
)

func chColumns[T any]() []string {
	typeOfRow := reflect.TypeOf(*new(T))
	columns := make([]string, 0, typeOfRow.NumField())
	for index := range typeOfRow.NumField() {
		if column := typeOfRow.Field(index).Tag.Get("ch"); column != "" {
			columns = append(columns, column)
		}
	}
	return columns
}

func insert[T any](ctx context.Context, conn driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+table+" ("+strings.Join(chColumns[T](), ", ")+")")
	if err != nil {
		return 0, fmt.Errorf("prepare %s: %w", table, err)
	}
	for index := range rows {
		if err := batch.AppendStruct(&rows[index]); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append %s row %d: %w", table, index, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send %s: %w", table, err)
	}
	return len(rows), nil
}

func observationRows(records []model.StagedDNSRecord, scanID string, loadedAt time.Time) []model.RecordObservationRow {
	rows := make([]model.RecordObservationRow, len(records))
	for index, record := range records {
		rows[index] = model.RecordObservationRow{
			RootDomain: record.RootDomain, Name: record.Name, RecordType: record.RecordType,
			Slot: record.Slot, Value: record.Value, Source: record.Source,
			Discovery: record.Discovery, ScanID: scanID, TTL: record.TTL,
			Priority: record.Priority, Rcode: record.Rcode, ObservedAt: record.ObservedAt,
			LoadedAt: loadedAt.UTC(),
		}
	}
	return rows
}

// FlushDNS acknowledges local work only after record, summary, and registry sinks all succeed.
func FlushDNS(ctx context.Context, conn driver.Conn, localStore *store.Store, scanID string, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 500
	}
	ready, err := localStore.ReadyDNS(ctx, scanID, batchSize)
	if err != nil {
		return 0, fmt.Errorf("read ready DNS batch: %w", err)
	}
	if len(ready.Roots) == 0 {
		return 0, nil
	}
	if _, err := insert(ctx, conn, recordObservationsTable, observationRows(ready.Records, scanID, time.Now().UTC())); err != nil {
		return 0, fmt.Errorf("write DNS observations: %w", err)
	}
	if _, err := insert(ctx, conn, scanTable, ready.Summaries); err != nil {
		return 0, fmt.Errorf("write DNS summaries: %w", err)
	}
	if _, err := insert(ctx, conn, hostnamesTable, ready.Hostnames); err != nil {
		return 0, fmt.Errorf("write DNS hostnames: %w", err)
	}
	if err := localStore.AcknowledgeDNS(ctx, scanID, ready.Roots); err != nil {
		return 0, fmt.Errorf("acknowledge DNS batch: %w", err)
	}
	return len(ready.Roots), nil
}

func LoadAXFRPriorForRoots(ctx context.Context, conn driver.Conn, roots []string) (map[store.AXFREndpointKey]store.AXFRPriorState, error) {
	if len(roots) == 0 {
		return map[store.AXFREndpointKey]store.AXFRPriorState{}, nil
	}
	rows, err := conn.Query(ctx, `SELECT root_domain, name_server, name_server_ip,
		has_definitive_state, axfr_open, definitive_at, definitive_scan_id,
		last_probe_verdict, last_probe_reason, last_probed_at,
		last_probe_records, last_probe_bytes, last_probe_truncated,
		delegation_active, delegation_seen_at FROM `+axfrLatestTable+` FINAL
		WHERE has(?, root_domain)`, roots)
	if err != nil {
		return nil, fmt.Errorf("query scoped AXFR prior: %w", err)
	}
	defer rows.Close()
	prior := map[store.AXFREndpointKey]store.AXFRPriorState{}
	for rows.Next() {
		var key store.AXFREndpointKey
		var state store.AXFRPriorState
		var hasDefinitive, axfrOpen, truncated, delegationActive uint8
		if err := rows.Scan(&key.RootDomain, &key.NameServer, &key.NameServerIP,
			&hasDefinitive, &axfrOpen, &state.DefinitiveAt, &state.DefinitiveScanID,
			&state.LastProbeVerdict, &state.LastProbeReason, &state.LastProbedAt,
			&state.LastProbeRecords, &state.LastProbeBytes, &truncated,
			&delegationActive, &state.DelegationSeenAt); err != nil {
			return nil, fmt.Errorf("scan scoped AXFR prior: %w", err)
		}
		state.HasDefinitive = hasDefinitive != 0
		state.AXFROpen = axfrOpen != 0
		state.LastProbeTruncated = truncated != 0
		state.DelegationActive = delegationActive != 0
		prior[key] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read scoped AXFR prior: %w", err)
	}
	return prior, nil
}

// BuildAXFRLatestRows carries prior probe/definitive fields forward and uses stable observation time
// as the ReplacingMergeTree version.
func BuildAXFRLatestRows(endpoints []store.AXFRProbedEndpoint, prior map[store.AXFREndpointKey]store.AXFRPriorState, scanID string) []model.AXFRLatestRow {
	rows := make([]model.AXFRLatestRow, 0, len(endpoints))
	for _, endpoint := range endpoints {
		state := prior[endpoint.AXFREndpointKey]
		updatedAt := endpoint.StateObservedAt.UTC()
		row := model.AXFRLatestRow{
			RootDomain: endpoint.RootDomain, NameServer: endpoint.NameServer,
			NameServerIP: endpoint.NameServerIP, UpdatedAt: updatedAt,
			DelegationActive: boolToU8(endpoint.DelegationActive),
			DelegationSeenAt: state.DelegationSeenAt,
			LastProbeVerdict: state.LastProbeVerdict, LastProbeReason: state.LastProbeReason,
			LastProbedAt: state.LastProbedAt, LastProbeRecords: state.LastProbeRecords,
			LastProbeBytes:     state.LastProbeBytes,
			LastProbeTruncated: boolToU8(state.LastProbeTruncated),
			HasDefinitiveState: boolToU8(state.HasDefinitive), AXFROpen: boolToU8(state.AXFROpen),
			DefinitiveAt: state.DefinitiveAt, DefinitiveScanID: state.DefinitiveScanID,
		}
		if endpoint.DelegationActive {
			row.DelegationSeenAt = updatedAt
		}
		if endpoint.Verdict != "" {
			row.LastProbeVerdict, row.LastProbeReason = endpoint.Verdict, endpoint.Reason
			row.LastProbedAt = endpoint.ObservedAt
			row.LastProbeRecords, row.LastProbeBytes = endpoint.Records, endpoint.Bytes
			row.LastProbeTruncated = boolToU8(endpoint.Truncated)
			if endpoint.Definitive {
				row.HasDefinitiveState = 1
				row.AXFROpen = boolToU8(endpoint.Verdict == string(resolve.VerdictOpen))
				row.DefinitiveAt, row.DefinitiveScanID = endpoint.ObservedAt, scanID
			}
		}
		rows = append(rows, row)
	}
	return rows
}

// FlushAXFR writes changes before latest state, then transferred records and discovered hostnames.
func FlushAXFR(ctx context.Context, conn driver.Conn, localStore *store.Store, scanID string, batchSize int) (int, error) {
	if batchSize <= 0 {
		batchSize = 100
	}
	jobs, err := localStore.ReadyAXFR(ctx, scanID, batchSize)
	if err != nil {
		return 0, fmt.Errorf("read ready AXFR batch: %w", err)
	}
	if len(jobs) == 0 {
		return 0, nil
	}
	roots := make([]string, len(jobs))
	for index, job := range jobs {
		roots[index] = job.RootDomain
	}
	prior, err := LoadAXFRPriorForRoots(ctx, conn, roots)
	if err != nil {
		return 0, err
	}
	endpoints := boundedAXFREndpoints(jobs, prior)
	if _, err := insert(ctx, conn, axfrChangesTable, boundedAXFRChanges(endpoints, prior, scanID)); err != nil {
		return 0, fmt.Errorf("write AXFR changes: %w", err)
	}
	if _, err := insert(ctx, conn, axfrLatestTable, BuildAXFRLatestRows(endpoints, prior, scanID)); err != nil {
		return 0, fmt.Errorf("write AXFR latest: %w", err)
	}
	records, hostnames := boundedAXFROutputs(jobs, scanID)
	if _, err := insert(ctx, conn, recordObservationsTable, records); err != nil {
		return 0, fmt.Errorf("write AXFR observations: %w", err)
	}
	if _, err := insert(ctx, conn, hostnamesTable, hostnames); err != nil {
		return 0, fmt.Errorf("write AXFR hostnames: %w", err)
	}
	if err := localStore.AcknowledgeAXFR(ctx, scanID, roots); err != nil {
		return 0, fmt.Errorf("acknowledge AXFR batch: %w", err)
	}
	return len(jobs), nil
}

func boundedAXFREndpoints(jobs []store.ReadyAXFRJob, prior map[store.AXFREndpointKey]store.AXFRPriorState) []store.AXFRProbedEndpoint {
	var endpoints []store.AXFRProbedEndpoint
	touched := map[string]time.Time{}
	seen := map[store.AXFREndpointKey]bool{}
	for _, job := range jobs {
		touched[job.RootDomain] = job.DelegationObservedAt
		current := map[store.AXFREndpointKey]bool{}
		for _, endpoint := range job.Endpoints {
			current[store.AXFREndpointKey{RootDomain: job.RootDomain, NameServer: endpoint.Name, NameServerIP: endpoint.IP}] = true
		}
		for _, probe := range job.Probes {
			key := store.AXFREndpointKey{RootDomain: job.RootDomain, NameServer: probe.NSHost, NameServerIP: probe.NSIP}
			seen[key] = true
			endpoints = append(endpoints, store.AXFRProbedEndpoint{
				AXFREndpointKey: key, Verdict: string(probe.Verdict), Reason: string(probe.Reason),
				ObservedAt: probe.ObservedAt, Records: uint64(probe.Records), Bytes: uint64(probe.Bytes),
				Truncated: probe.Truncated, StateObservedAt: probe.ObservedAt,
				Definitive: probe.IsDefinitive(), DelegationActive: true,
			})
		}
		for key := range current {
			if seen[key] {
				continue
			}
			if _, exists := prior[key]; !exists {
				continue
			}
			seen[key] = true
			endpoints = append(endpoints, store.AXFRProbedEndpoint{
				AXFREndpointKey: key, DelegationActive: true, StateObservedAt: job.DelegationObservedAt,
			})
		}
	}
	for key := range prior {
		observedAt, touchedRoot := touched[key.RootDomain]
		if touchedRoot && !seen[key] {
			endpoints = append(endpoints, store.AXFRProbedEndpoint{
				AXFREndpointKey: key, StateObservedAt: observedAt,
			})
		}
	}
	return endpoints
}

func boundedAXFRChanges(endpoints []store.AXFRProbedEndpoint, prior map[store.AXFREndpointKey]store.AXFRPriorState, scanID string) []model.AXFRStateChangeRow {
	var changes []model.AXFRStateChangeRow
	for _, endpoint := range endpoints {
		if !endpoint.Definitive {
			continue
		}
		open := endpoint.Verdict == string(resolve.VerdictOpen)
		state, exists := prior[endpoint.AXFREndpointKey]
		if exists && state.HasDefinitive && state.AXFROpen == open {
			continue
		}
		if (!exists || !state.HasDefinitive) && !open {
			continue
		}
		changes = append(changes, model.AXFRStateChangeRow{
			RootDomain: endpoint.RootDomain, NameServer: endpoint.NameServer,
			NameServerIP: endpoint.NameServerIP, AXFROpen: boolToU8(open),
			ScanID: scanID, ChangedAt: endpoint.ObservedAt,
		})
	}
	return changes
}

func boundedAXFROutputs(jobs []store.ReadyAXFRJob, scanID string) ([]model.RecordObservationRow, []model.HostnameRow) {
	loadedAt := time.Now().UTC()
	var records []model.RecordObservationRow
	hostnames := map[string]model.HostnameRow{}
	for _, job := range jobs {
		observedAt := job.DelegationObservedAt
		for _, probe := range job.Probes {
			if probe.IsOpen() {
				observedAt = probe.ObservedAt
				break
			}
		}
		for _, record := range job.Zone {
			records = append(records, model.RecordObservationRow{
				RootDomain: job.RootDomain, Name: record.Name, RecordType: record.RecordType,
				Slot: record.Slot, Value: record.Value, Source: "axfr", Discovery: "axfr",
				ScanID: scanID, TTL: record.TTL, Priority: record.Priority, Rcode: record.Rcode,
				ObservedAt: observedAt, LoadedAt: loadedAt,
			})
			name, suffix := strings.ToLower(record.Name), "."+strings.ToLower(job.RootDomain)
			if name == strings.ToLower(job.RootDomain) || !strings.HasSuffix(name, suffix) {
				continue
			}
			label := strings.TrimSuffix(name, suffix)
			if label == "" || strings.Contains(label, "*") {
				continue
			}
			hostnames[job.RootDomain+"\x00"+label] = model.HostnameRow{
				RootDomain: job.RootDomain, Label: label, DiscoverySource: "axfr",
				FirstSeen: observedAt, LastSeen: observedAt, LastResolved: observedAt,
			}
		}
	}
	hostnameRows := make([]model.HostnameRow, 0, len(hostnames))
	for _, row := range hostnames {
		hostnameRows = append(hostnameRows, row)
	}
	return records, hostnameRows
}

func boolToU8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}
