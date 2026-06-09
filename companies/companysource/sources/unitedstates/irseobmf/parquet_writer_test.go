package irseobmf

import (
	"path/filepath"
	"testing"
)

func TestWriteParquetRowsRoundTrips(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "companies.parquet")
	rows := []CompanyExportRow{
		{EIN: "010011694", LegalName: "ALPHA"},
		{EIN: "010018830", LegalName: "BETA"},
	}
	if err := WriteParquetRows(path, rows); err != nil {
		t.Fatalf("WriteParquetRows: %v", err)
	}

	got := readParquetRows[CompanyExportRow](t, path)
	if len(got) != 2 || got[0].EIN != "010011694" || got[1].LegalName != "BETA" {
		t.Fatalf("round-trip rows = %#v", got)
	}
}
