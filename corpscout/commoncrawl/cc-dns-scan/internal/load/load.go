// Package load flushes the DNS SQLite outbox to retry-safe ClickHouse tables.
package load

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cc-dns-scan/internal/model"
	"cc-dns-scan/internal/store"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const (
	recordObservationsTable = "corpscout.commoncrawl_domain_dns_record_observations"
	scanTable               = "corpscout.commoncrawl_domain_dns_scan"
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

func insert[T any](ctx context.Context, connection driver.Conn, table string, rows []T) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := connection.PrepareBatch(ctx, "INSERT INTO "+table+" ("+strings.Join(chColumns[T](), ", ")+")")
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
			TypeCode: record.TypeCode, ClassCode: record.ClassCode, Slot: record.Slot,
			Value: record.Value, RDataWire: record.RDataWire, Source: record.Source,
			Discovery: record.Discovery, NameServer: record.NameServer,
			NameServerIP: record.NameServerIP, ScanID: scanID, TTL: record.TTL,
			Priority: record.Priority, Rcode: record.Rcode, ObservedAt: record.ObservedAt,
			LoadedAt: loadedAt.UTC(),
		}
	}
	return rows
}

// FlushDNS acknowledges local work only after its observation and summary sinks succeed.
func FlushDNS(ctx context.Context, connection driver.Conn, localStore *store.Store, scanID string, batchSize int) (int, error) {
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
	if _, err := insert(ctx, connection, recordObservationsTable, observationRows(ready.Records, scanID, time.Now().UTC())); err != nil {
		return 0, fmt.Errorf("write DNS observations: %w", err)
	}
	if _, err := insert(ctx, connection, scanTable, ready.Summaries); err != nil {
		return 0, fmt.Errorf("write DNS summaries: %w", err)
	}
	if err := localStore.AcknowledgeDNS(ctx, scanID, ready.Roots); err != nil {
		return 0, fmt.Errorf("acknowledge DNS batch: %w", err)
	}
	return len(ready.Roots), nil
}
