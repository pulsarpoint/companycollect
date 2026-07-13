package load

import (
	"testing"
	"time"

	"cc-dns-scan/internal/model"
)

func TestObservationRowsCarryUniversalRRFields(t *testing.T) {
	loadedAt := time.Unix(20, 0).UTC()
	rows := observationRows([]model.StagedDNSRecord{{
		RootDomain: "example.com", Name: "unknown.example.com", RecordType: "TYPE65400",
		TypeCode: 65400, ClassCode: 65280, Value: `\# 4 DEADBEEF`,
		RDataWire: string([]byte{0xde, 0xad, 0xbe, 0xef}), Source: "query", Discovery: "ct",
	}}, "scan", loadedAt)
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	row := rows[0]
	if row.TypeCode != 65400 || row.ClassCode != 65280 || row.RDataWire != string([]byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("universal RR fields lost: %+v", row)
	}
}
