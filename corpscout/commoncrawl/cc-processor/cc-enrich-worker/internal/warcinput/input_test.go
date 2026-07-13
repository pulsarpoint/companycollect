package warcinput

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"cc-enrich-worker/internal/catalog"
	"cc-enrich-worker/internal/model"
)

type pageFixture struct {
	WARCIndex        uint32
	RootDomain       string
	URL              string
	DomainPageRank   uint16
	WARCRecordOffset uint64
	WARCRecordLength uint64
}

func writeCatalog(t *testing.T, baseDirectory, crawlID, selection string, warcs []catalog.Warc, pages []pageFixture) {
	t.Helper()
	directory := filepath.Join(baseDirectory, crawlID, "warc-index", selection)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	database, err := sql.Open("duckdb", filepath.Join(directory, "catalog.duckdb"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE warcs (warc_index UINTEGER, warc_filename VARCHAR);
		CREATE TABLE pages (
			warc_index UINTEGER,
			root_domain VARCHAR,
			url VARCHAR,
			domain_page_rank USMALLINT,
			warc_record_offset UBIGINT,
			warc_record_length UBIGINT
		)
	`); err != nil {
		database.Close()
		t.Fatal(err)
	}
	for _, warc := range warcs {
		if _, err := database.Exec("INSERT INTO warcs VALUES (?, ?)", warc.WarcIndex, warc.WarcFilename); err != nil {
			database.Close()
			t.Fatal(err)
		}
	}
	for _, page := range pages {
		if _, err := database.Exec(
			"INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?)",
			page.WARCIndex,
			page.RootDomain,
			page.URL,
			page.DomainPageRank,
			page.WARCRecordOffset,
			page.WARCRecordLength,
		); err != nil {
			database.Close()
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadPlanFiltersPrimaryPagesBeforeSummingBytes(t *testing.T) {
	baseDirectory := t.TempDir()
	writeCatalog(t, baseDirectory, "CC-MAIN-2026-25", "pages25",
		[]catalog.Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz"}},
		[]pageFixture{
			{WARCIndex: 0, RootDomain: "example.com", URL: "https://example.com/", DomainPageRank: 1, WARCRecordOffset: 10, WARCRecordLength: 10},
			{WARCIndex: 0, RootDomain: "example.com", URL: "https://example.com/about", DomainPageRank: 2, WARCRecordOffset: 30, WARCRecordLength: 20},
		},
	)

	allPages, err := LoadPlan(baseDirectory, "CC-MAIN-2026-25", "pages25", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(allPages.Items) != 2 || allPages.SelectedBytes != 30 {
		t.Fatalf("all-pages plan = %+v", allPages)
	}

	primaryPages, err := LoadPlan(baseDirectory, "CC-MAIN-2026-25", "pages25", 0, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(primaryPages.Items) != 1 || !primaryPages.Items[0].Primary || primaryPages.SelectedBytes != 10 {
		t.Fatalf("primary-pages plan = %+v", primaryPages)
	}
}

func TestEmptyPlanOpensWithoutObjectGetter(t *testing.T) {
	baseDirectory := t.TempDir()
	writeCatalog(t, baseDirectory, "CC-MAIN-2026-25", "pages25",
		[]catalog.Warc{{WarcIndex: 0, WarcFilename: "zero.warc.gz"}},
		nil,
	)
	plan, err := LoadPlan(baseDirectory, "CC-MAIN-2026-25", "pages25", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Empty() {
		t.Fatalf("plan = %+v, want empty", plan)
	}

	input, err := plan.Open(context.Background(), nil, "", 50, "")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if input.Mode != ModeEmpty || input.ObjectBytes != 0 || input.SelectedBytes != 0 || input.Getter == nil {
		t.Fatalf("input = %+v", input)
	}
	if _, err := input.Getter.GetRange(context.Background(), "commoncrawl", "zero.warc.gz", 0, 0); err == nil {
		t.Fatal("empty getter unexpectedly served a range")
	}
}

func TestOpenDownloadsWholeWARCAtInclusiveThreshold(t *testing.T) {
	data := make([]byte, 100)
	for index := range data {
		data[index] = byte(index)
	}
	objects := &fakeObjects{objectSize: int64(len(data)), downloadBody: data, rangeBody: data}
	plan := testPlan(10, 50)

	input, err := plan.Open(context.Background(), objects, "commoncrawl", 50, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if input.Mode != ModeWholeFile || input.SelectedBytes != 50 || input.ObjectBytes != 100 {
		t.Fatalf("input = %+v", input)
	}
	if objects.sizeCalls != 1 || objects.downloadCalls != 1 || objects.rangeCalls != 0 {
		t.Fatalf("object calls: size=%d download=%d range=%d", objects.sizeCalls, objects.downloadCalls, objects.rangeCalls)
	}
	got, err := input.Getter.GetRange(context.Background(), "commoncrawl", "zero.warc.gz", 10, 59)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[10:60]) {
		t.Fatalf("local range = %v, want %v", got, data[10:60])
	}
	if _, err := input.Getter.GetRange(context.Background(), "commoncrawl", "zero.warc.gz", 11, 59); err == nil ||
		!strings.Contains(err.Error(), "not a selected record") {
		t.Fatalf("unselected-range error = %v", err)
	}
	tempPath := objects.destinationPath
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf("downloaded object before close: %v", err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf("downloaded object still exists after close: %v", err)
	}
}

func TestOpenKeepsNetworkGetterBelowThreshold(t *testing.T) {
	data := bytes.Repeat([]byte{'x'}, 100)
	objects := &fakeObjects{objectSize: int64(len(data)), rangeBody: data}
	plan := testPlan(10, 50)

	input, err := plan.Open(context.Background(), objects, "commoncrawl", 50.01, "")
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	if input.Mode != ModeRange || input.SelectedBytes != 50 || input.ObjectBytes != 100 {
		t.Fatalf("input = %+v", input)
	}
	got, err := input.Getter.GetRange(context.Background(), "commoncrawl", "zero.warc.gz", 10, 59)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 50 || objects.sizeCalls != 1 || objects.downloadCalls != 0 || objects.rangeCalls != 1 {
		t.Fatalf("range bytes=%d calls: size=%d download=%d range=%d", len(got), objects.sizeCalls, objects.downloadCalls, objects.rangeCalls)
	}
}

func TestOpenRejectsInvalidThresholdWithoutObjectIO(t *testing.T) {
	for _, threshold := range []float64{-1, 101, math.NaN(), math.Inf(1)} {
		objects := &fakeObjects{objectSize: 100}
		_, err := testPlan(10, 10).Open(context.Background(), objects, "commoncrawl", threshold, "")
		if err == nil || !strings.Contains(err.Error(), "threshold must be between 0 and 100") {
			t.Fatalf("threshold %v error = %v", threshold, err)
		}
		if objects.sizeCalls != 0 || objects.downloadCalls != 0 || objects.rangeCalls != 0 {
			t.Fatalf("threshold %v performed object I/O", threshold)
		}
	}
}

func TestOpenValidatesObjectSizeAndSelectedRanges(t *testing.T) {
	tests := []struct {
		name       string
		plan       Plan
		objectSize int64
		wantError  string
	}{
		{name: "zero object", plan: testPlan(0, 1), objectSize: 0, wantError: "invalid size 0"},
		{name: "selected total exceeds object", plan: testPlan(0, 20), objectSize: 10, wantError: "selected bytes 20 exceed"},
		{name: "record exceeds object", plan: testPlan(90, 20), objectSize: 100, wantError: "exceeds WARC object size 100"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := &fakeObjects{objectSize: test.objectSize}
			_, err := test.plan.Open(context.Background(), objects, "commoncrawl", 50, "")
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
			if objects.downloadCalls != 0 || objects.rangeCalls != 0 {
				t.Fatalf("calls: download=%d range=%d", objects.downloadCalls, objects.rangeCalls)
			}
		})
	}
}

func TestWholeWARCDownloadFailureRemovesTemporaryFile(t *testing.T) {
	tests := []struct {
		name         string
		downloadBody []byte
		downloadErr  error
		wantError    string
	}{
		{name: "download error", downloadErr: errors.New("connection reset"), wantError: "connection reset"},
		{name: "short download", downloadBody: bytes.Repeat([]byte{'x'}, 99), wantError: "HEAD reported 100"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			objects := &fakeObjects{objectSize: 100, downloadBody: test.downloadBody, downloadErr: test.downloadErr}
			_, err := testPlan(10, 50).Open(context.Background(), objects, "commoncrawl", 50, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want %q", err, test.wantError)
			}
			if objects.destinationPath == "" {
				t.Fatal("download did not receive a temporary file")
			}
			if _, err := os.Stat(objects.destinationPath); !os.IsNotExist(err) {
				t.Fatalf("temporary file still exists: %v", err)
			}
		})
	}
}

func TestOpenAsForcesMode(t *testing.T) {
	data := make([]byte, 100)
	for index := range data {
		data[index] = byte(index)
	}

	// ModeWholeFile downloads even though the selected coverage is tiny (20/100 = 20%),
	// because the forced lane skips the threshold decision.
	t.Run("whole forces download at tiny coverage", func(t *testing.T) {
		plan := Plan{
			Items: []model.WorklistItem{
				{RootDomain: "example.com", URL: "https://example.com/", WarcIndex: 0, WarcFilename: "zero.warc.gz", Offset: 0, Length: 10, Primary: true},
				{RootDomain: "example.com", URL: "https://example.com/about", WarcIndex: 0, WarcFilename: "zero.warc.gz", Offset: 20, Length: 10, Primary: false},
			},
			WARCIndex:     0,
			WARCFilename:  "zero.warc.gz",
			SelectedBytes: 20,
		}
		objects := &fakeObjects{objectSize: int64(len(data)), downloadBody: data, rangeBody: data}
		input, err := plan.OpenAs(context.Background(), objects, "commoncrawl", ModeWholeFile, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer input.Close()
		if input.Mode != ModeWholeFile || input.ObjectBytes != 100 || input.SelectedBytes != 20 {
			t.Fatalf("input = %+v", input)
		}
		if objects.sizeCalls != 1 || objects.downloadCalls != 1 || objects.rangeCalls != 0 {
			t.Fatalf("object calls: size=%d download=%d range=%d", objects.sizeCalls, objects.downloadCalls, objects.rangeCalls)
		}
	})

	// ModeRange keeps the network getter and creates no temp file even at 100% coverage.
	t.Run("range forces no download at full coverage", func(t *testing.T) {
		plan := testPlan(0, 100) // selected == object size -> 100% coverage
		objects := &fakeObjects{objectSize: int64(len(data)), rangeBody: data}
		input, err := plan.OpenAs(context.Background(), objects, "commoncrawl", ModeRange, "")
		if err != nil {
			t.Fatal(err)
		}
		defer input.Close()
		if input.Mode != ModeRange || input.ObjectBytes != 100 || input.SelectedBytes != 100 {
			t.Fatalf("input = %+v", input)
		}
		if objects.sizeCalls != 1 || objects.downloadCalls != 0 {
			t.Fatalf("object calls: size=%d download=%d", objects.sizeCalls, objects.downloadCalls)
		}
		if objects.destinationPath != "" {
			t.Fatalf("ModeRange created a temp file: %s", objects.destinationPath)
		}
	})
}

func testPlan(offset, length int64) Plan {
	return Plan{
		Items: []model.WorklistItem{{
			RootDomain: "example.com", URL: "https://example.com/", WarcIndex: 0,
			WarcFilename: "zero.warc.gz", Offset: offset, Length: length, Primary: true,
		}},
		WARCIndex:     0,
		WARCFilename:  "zero.warc.gz",
		SelectedBytes: length,
	}
}

type fakeObjects struct {
	objectSize      int64
	downloadBody    []byte
	rangeBody       []byte
	downloadErr     error
	sizeCalls       int
	downloadCalls   int
	rangeCalls      int
	destinationPath string
}

func (objects *fakeObjects) ObjectSize(context.Context, string, string) (int64, error) {
	objects.sizeCalls++
	return objects.objectSize, nil
}

func (objects *fakeObjects) DownloadObject(_ context.Context, _, _ string, destination *os.File) error {
	objects.downloadCalls++
	objects.destinationPath = destination.Name()
	if objects.downloadErr != nil {
		return objects.downloadErr
	}
	_, err := destination.Write(objects.downloadBody)
	return err
}

func (objects *fakeObjects) GetRange(_ context.Context, _, _ string, start, end int64) ([]byte, error) {
	objects.rangeCalls++
	if start < 0 || end < start || end >= int64(len(objects.rangeBody)) {
		return nil, errors.New("invalid fake range")
	}
	return append([]byte(nil), objects.rangeBody[start:end+1]...), nil
}
