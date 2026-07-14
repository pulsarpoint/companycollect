package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

type recordingBatch struct {
	driver.Batch
	rows [][]any
}

func (b *recordingBatch) Append(values ...any) error {
	b.rows = append(b.rows, append([]any(nil), values...))
	return nil
}

func TestAppendHostnameRowsUsesOneIngestionTimestampForBatch(t *testing.T) {
	batch := &recordingBatch{}
	ingestedAt := time.Date(2026, time.July, 11, 12, 13, 14, 123_000_000, time.UTC)
	rows := []model.HostnameRow{
		{RegisteredDomain: "example.com", FQDN: "www.example.com"},
		{RegisteredDomain: "example.net", FQDN: "api.example.net"},
	}

	if err := appendHostnameRows(batch, rows, ingestedAt); err != nil {
		t.Fatalf("append hostname rows: %v", err)
	}

	if len(batch.rows) != len(rows) {
		t.Fatalf("appended %d rows, want %d", len(batch.rows), len(rows))
	}
	for rowIndex, values := range batch.rows {
		if len(values) != 8 {
			t.Fatalf("row %d has %d values, want 8", rowIndex, len(values))
		}
		if values[7] != ingestedAt {
			t.Fatalf("row %d ingestion timestamp = %v, want %v", rowIndex, values[7], ingestedAt)
		}
	}
}

func TestHostnameInsertIncludesIngestionWatermark(t *testing.T) {
	const expectedColumns = "last_not_after, source_logs, last_ingested_at"
	if !strings.Contains(hostnameInsertSQL, expectedColumns) {
		t.Fatalf("hostname insert does not contain %q", expectedColumns)
	}
}
