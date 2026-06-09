package coloradoentities

import (
	"path/filepath"
	"testing"
)

func TestWriteParquetRowsRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "companies.parquet")
	rows := []CompanyExportRow{
		{EntityID: "20251665680", LegalName: "ALPHA"},
		{EntityID: "19871342214", LegalName: "BETA"},
	}
	if err := WriteParquetRows(path, rows); err != nil {
		t.Fatalf("WriteParquetRows: %v", err)
	}

	got := readParquetRows[CompanyExportRow](t, path)
	if len(got) != 2 || got[0].EntityID != "20251665680" || got[1].LegalName != "BETA" {
		t.Fatalf("round-trip rows = %#v", got)
	}
}
