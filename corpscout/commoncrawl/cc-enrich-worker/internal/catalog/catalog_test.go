package catalog

import (
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func writeCatalogFixture(t *testing.T, warcs []Warc, pages []pageRow) (string, string) {
	t.Helper()
	directory := t.TempDir()
	warcsPath := filepath.Join(directory, "warcs.parquet")
	pagesPath := filepath.Join(directory, "pages.parquet")
	if err := parquet.WriteFile(warcsPath, warcs); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(pagesPath, pages, parquet.MaxRowsPerRowGroup(2)); err != nil {
		t.Fatal(err)
	}
	return warcsPath, pagesPath
}

func TestLoadRangeReturnsOnlyRequestedWarcs(t *testing.T) {
	warcsPath, pagesPath := writeCatalogFixture(t,
		[]Warc{
			{WarcIndex: 0, WarcFilename: "zero.warc.gz"},
			{WarcIndex: 1, WarcFilename: "one.warc.gz", SelectedPages: 2, SelectedBytes: 30},
			{WarcIndex: 2, WarcFilename: "two.warc.gz", SelectedPages: 1, SelectedBytes: 30},
		},
		[]pageRow{
			{WarcIndex: 1, RootDomain: "one.example", URL: "https://one.example/", DomainPageRank: 1, WarcRecordOffset: 10, WarcRecordLength: 10},
			{WarcIndex: 1, RootDomain: "one.example", URL: "https://one.example/about", DomainPageRank: 2, WarcRecordOffset: 30, WarcRecordLength: 20},
			{WarcIndex: 2, RootDomain: "two.example", URL: "https://two.example/", DomainPageRank: 1, WarcRecordOffset: 5, WarcRecordLength: 30},
		},
	)

	warcs, items, err := LoadRange(warcsPath, pagesPath, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(warcs) != 1 || warcs[0].WarcIndex != 1 {
		t.Fatalf("warcs = %+v, want only WARC index 1", warcs)
	}
	if len(items) != 2 {
		t.Fatalf("items = %+v, want two pages", items)
	}
	if items[0].WarcIndex != 1 || items[0].WarcFilename != "one.warc.gz" || !items[0].Primary {
		t.Fatalf("first item = %+v", items[0])
	}
	if items[1].WarcIndex != 1 || items[1].WarcFilename != "one.warc.gz" || items[1].Primary {
		t.Fatalf("second item = %+v", items[1])
	}
}

func TestLoadRangeAcceptsZeroPageWarc(t *testing.T) {
	warcsPath, pagesPath := writeCatalogFixture(t,
		[]Warc{
			{WarcIndex: 0, WarcFilename: "zero.warc.gz"},
			{WarcIndex: 1, WarcFilename: "one.warc.gz", SelectedPages: 1, SelectedBytes: 10},
		},
		[]pageRow{
			{WarcIndex: 1, RootDomain: "one.example", URL: "https://one.example/", DomainPageRank: 1, WarcRecordOffset: 10, WarcRecordLength: 10},
		},
	)

	warcs, items, err := LoadRange(warcsPath, pagesPath, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(warcs) != 1 || warcs[0].WarcIndex != 0 {
		t.Fatalf("warcs = %+v, want zero-page WARC", warcs)
	}
	if len(items) != 0 {
		t.Fatalf("items = %+v, want no pages", items)
	}
}

func TestLoadRangeRejectsInvalidPageSchema(t *testing.T) {
	type invalidPageRow struct {
		WarcIndex        uint32 `parquet:"warc_index"`
		RootDomain       string `parquet:"root_domain"`
		URL              uint64 `parquet:"url"`
		DomainPageRank   uint16 `parquet:"domain_page_rank"`
		WarcRecordOffset uint64 `parquet:"warc_record_offset"`
		WarcRecordLength uint64 `parquet:"warc_record_length"`
	}

	directory := t.TempDir()
	warcsPath := filepath.Join(directory, "warcs.parquet")
	pagesPath := filepath.Join(directory, "pages.parquet")
	if err := parquet.WriteFile(warcsPath, []Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz", SelectedPages: 1, SelectedBytes: 10}}); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(pagesPath, []invalidPageRow{{
		WarcIndex: 0, RootDomain: "one.example", URL: 1, DomainPageRank: 1,
		WarcRecordOffset: 10, WarcRecordLength: 10,
	}}); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadRange(warcsPath, pagesPath, 0, 0)
	if err == nil || !strings.Contains(err.Error(), `column "url" has type uint64, want string`) {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadWarcsRejectsInvalidSchema(t *testing.T) {
	type invalidWarcRow struct {
		WarcIndex     uint32 `parquet:"warc_index"`
		WarcFilename  string `parquet:"warc_filename"`
		SelectedPages uint64 `parquet:"selected_pages"`
	}

	path := filepath.Join(t.TempDir(), "warcs.parquet")
	if err := parquet.WriteFile(path, []invalidWarcRow{{WarcIndex: 1, WarcFilename: "one.warc.gz", SelectedPages: 1}}); err != nil {
		t.Fatal(err)
	}
	_, err := LoadWarcs(path)
	if err == nil || !strings.Contains(err.Error(), `missing required column "selected_bytes"`) {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRangeRequiresContentLanguagesColumn(t *testing.T) {
	type pageWithoutLanguages struct {
		WarcIndex        uint32 `parquet:"warc_index"`
		RootDomain       string `parquet:"root_domain"`
		URL              string `parquet:"url"`
		DomainPageRank   uint16 `parquet:"domain_page_rank"`
		WarcRecordOffset uint64 `parquet:"warc_record_offset"`
		WarcRecordLength uint64 `parquet:"warc_record_length"`
	}

	directory := t.TempDir()
	warcsPath := filepath.Join(directory, "warcs.parquet")
	pagesPath := filepath.Join(directory, "pages.parquet")
	if err := parquet.WriteFile(warcsPath, []Warc{{WarcFilename: "zero.warc.gz", SelectedPages: 1, SelectedBytes: 10}}); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(pagesPath, []pageWithoutLanguages{{
		RootDomain: "one.example", URL: "https://one.example/", DomainPageRank: 1,
		WarcRecordOffset: 10, WarcRecordLength: 10,
	}}); err != nil {
		t.Fatal(err)
	}

	_, _, err := LoadRange(warcsPath, pagesPath, 0, 0)
	if err == nil || !strings.Contains(err.Error(), `missing required column "content_languages"`) {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadWarcsRequiresOrderedContiguousIndexes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "warcs.parquet")
	if err := parquet.WriteFile(path, []Warc{
		{WarcIndex: 0, WarcFilename: "zero.warc.gz"},
		{WarcIndex: 2, WarcFilename: "two.warc.gz"},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := LoadWarcs(path)
	if err == nil || !strings.Contains(err.Error(), "indexes must be ordered and contiguous from zero") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRangeRejectsRangeLargerThanInventory(t *testing.T) {
	warcsPath, pagesPath := writeCatalogFixture(t,
		[]Warc{{WarcFilename: "zero.warc.gz"}},
		nil,
	)

	_, _, err := LoadRange(warcsPath, pagesPath, 0, math.MaxUint32)
	if err == nil || !strings.Contains(err.Error(), "exceeds inventory indexes") {
		t.Fatalf("error = %v", err)
	}
}

func TestLoadRangeRejectsInvalidCoordinates(t *testing.T) {
	tests := []struct {
		name   string
		offset uint64
		length uint64
	}{
		{name: "zero length", offset: 10, length: 0},
		{name: "offset exceeds int64", offset: math.MaxInt64 + 1, length: 1},
		{name: "length exceeds int64", offset: 0, length: math.MaxInt64 + 1},
		{name: "range end overflows int64", offset: math.MaxInt64, length: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selectedBytes := test.length
			if selectedBytes == 0 {
				selectedBytes = 1
			}
			warcsPath, pagesPath := writeCatalogFixture(t,
				[]Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz", SelectedPages: 1, SelectedBytes: selectedBytes}},
				[]pageRow{{
					WarcIndex: 0, RootDomain: "one.example", URL: "https://one.example/", DomainPageRank: 1,
					WarcRecordOffset: test.offset, WarcRecordLength: test.length,
				}},
			)

			_, _, err := LoadRange(warcsPath, pagesPath, 0, 0)
			if err == nil || !strings.Contains(err.Error(), "invalid WARC coordinates") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
