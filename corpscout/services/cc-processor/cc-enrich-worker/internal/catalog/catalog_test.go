package catalog

import (
	"context"
	"database/sql"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
)

type catalogFixturePage struct {
	warcIndex uint32
	domain    string
	url       string
	rank      uint64
	offset    uint64
	length    uint64
}

func writeCatalogFixture(t *testing.T, warcs []Warc, pages []catalogFixturePage) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.duckdb")
	database, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE warcs (
			warc_index UINTEGER,
			warc_filename VARCHAR
		);
		CREATE TABLE pages (
			warc_index UINTEGER,
			root_domain VARCHAR,
			url VARCHAR,
			domain_page_rank UBIGINT,
			warc_record_offset UBIGINT,
			warc_record_length UBIGINT
		)
	`); err != nil {
		t.Fatal(err)
	}
	for _, warc := range warcs {
		if _, err := database.Exec("INSERT INTO warcs VALUES (?, ?)", warc.WarcIndex, warc.WarcFilename); err != nil {
			t.Fatal(err)
		}
	}
	for _, page := range pages {
		if _, err := database.Exec(
			"INSERT INTO pages VALUES (?, ?, ?, CAST(? AS UBIGINT), CAST(? AS UBIGINT), CAST(? AS UBIGINT))",
			page.warcIndex,
			page.domain,
			page.url,
			strconv.FormatUint(page.rank, 10),
			strconv.FormatUint(page.offset, 10),
			strconv.FormatUint(page.length, 10),
		); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := database.Exec("FORCE CHECKPOINT"); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadWARCReturnsOnlyRequestedWARC(t *testing.T) {
	path := writeCatalogFixture(t,
		[]Warc{
			{WarcIndex: 0, WarcFilename: "zero.warc.gz"},
			{WarcIndex: 1, WarcFilename: "one.warc.gz"},
			{WarcIndex: 2, WarcFilename: "two.warc.gz"},
		},
		[]catalogFixturePage{
			{warcIndex: 1, domain: "one.example", url: "https://one.example/", rank: 1, offset: 10, length: 10},
			{warcIndex: 2, domain: "two.example", url: "https://two.example/", rank: 1, offset: 5, length: 30},
			{warcIndex: 1, domain: "one.example", url: "https://one.example/about", rank: 2, offset: 30, length: 20},
		},
	)

	warc, items, err := LoadWARC(context.Background(), path, 1)
	if err != nil {
		t.Fatal(err)
	}
	if warc.WarcIndex != 1 || warc.WarcFilename != "one.warc.gz" {
		t.Fatalf("warc = %+v", warc)
	}
	if len(items) != 2 {
		t.Fatalf("items = %+v, want two pages", items)
	}
	if items[0].Offset != 10 || !items[0].Primary || items[0].WarcFilename != warc.WarcFilename {
		t.Fatalf("first item = %+v", items[0])
	}
	if items[1].Offset != 30 || items[1].Primary || items[1].WarcFilename != warc.WarcFilename {
		t.Fatalf("second item = %+v", items[1])
	}
}

func TestLoadWARCAcceptsZeroPageWARC(t *testing.T) {
	path := writeCatalogFixture(t, []Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz"}}, nil)

	warc, items, err := LoadWARC(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if warc.WarcIndex != 0 || warc.WarcFilename != "zero.warc.gz" || len(items) != 0 {
		t.Fatalf("warc = %+v items = %+v", warc, items)
	}
}

func TestLoadWARCRejectsAbsentWARC(t *testing.T) {
	path := writeCatalogFixture(t, []Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz"}}, nil)

	_, _, err := LoadWARC(context.Background(), path, 1)
	if err == nil || !strings.Contains(err.Error(), "WARC index 1 is absent") {
		t.Fatalf("error = %v", err)
	}
}

// TestLoadWARCAbsentIsErrWARCIndexAbsent pins that the absent-index error is detectable via
// errors.Is(err, ErrWARCIndexAbsent) — directly AND through the same cockroachdb Wrapf that
// warcinput.LoadPlan applies — while its human message stays byte-for-byte unchanged.
func TestLoadWARCAbsentIsErrWARCIndexAbsent(t *testing.T) {
	path := writeCatalogFixture(t, []Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz"}}, nil)

	_, _, err := LoadWARC(context.Background(), path, 1)
	if !errors.Is(err, ErrWARCIndexAbsent) {
		t.Fatalf("errors.Is(err, ErrWARCIndexAbsent) = false; err = %v", err)
	}
	if !strings.Contains(err.Error(), "WARC index 1 is absent from the catalog") {
		t.Fatalf("message changed: %v", err)
	}

	wrapped := errors.Wrapf(err, "load WARC catalog index %d", 1)
	if !errors.Is(wrapped, ErrWARCIndexAbsent) {
		t.Fatalf("errors.Is through Wrapf = false; wrapped = %v", wrapped)
	}
}

func TestLoadWARCRejectsInvalidPageValues(t *testing.T) {
	tests := []struct {
		name      string
		page      catalogFixturePage
		wantError string
	}{
		{
			name:      "missing domain",
			page:      catalogFixturePage{url: "https://example.com/", rank: 1, length: 1},
			wantError: "missing domain or URL",
		},
		{
			name:      "zero rank",
			page:      catalogFixturePage{domain: "example.com", url: "https://example.com/", length: 1},
			wantError: "invalid domain rank 0",
		},
		{
			name:      "rank exceeds uint16",
			page:      catalogFixturePage{domain: "example.com", url: "https://example.com/", rank: math.MaxUint16 + 1, length: 1},
			wantError: "invalid domain rank 65536",
		},
		{
			name:      "zero length",
			page:      catalogFixturePage{domain: "example.com", url: "https://example.com/", rank: 1},
			wantError: "invalid WARC coordinates",
		},
		{
			name:      "offset exceeds int64",
			page:      catalogFixturePage{domain: "example.com", url: "https://example.com/", rank: 1, offset: math.MaxInt64 + 1, length: 1},
			wantError: "invalid WARC coordinates",
		},
		{
			name:      "range end overflows int64",
			page:      catalogFixturePage{domain: "example.com", url: "https://example.com/", rank: 1, offset: math.MaxInt64, length: 2},
			wantError: "invalid WARC coordinates",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.page.warcIndex = 0
			path := writeCatalogFixture(
				t,
				[]Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz"}},
				[]catalogFixturePage{test.page},
			)
			_, _, err := LoadWARC(context.Background(), path, 0)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
		})
	}
}

func TestLoadWARCRequiresExpectedTables(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.duckdb")
	database, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec("CREATE TABLE unrelated (value INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, err = LoadWARC(context.Background(), path, 0)
	if err == nil || !strings.Contains(err.Error(), "query WARC index 0") {
		t.Fatalf("error = %v", err)
	}
}
