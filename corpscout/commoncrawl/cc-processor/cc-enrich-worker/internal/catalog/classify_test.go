package catalog

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
)

type classifyFixturePage struct {
	warcIndex uint32
	domain    string
	url       string
	rank      int64
	offset    int64
	length    int64
}

func writeClassifyFixture(t *testing.T, pages []classifyFixturePage) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "catalog.duckdb")
	database, err := sql.Open("duckdb", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := database.Exec(`
		CREATE TABLE main.warcs (
			warc_index INT,
			warc_filename VARCHAR
		);
		CREATE TABLE main.pages (
			warc_index INT,
			root_domain VARCHAR,
			url VARCHAR,
			domain_page_rank INT,
			warc_record_offset BIGINT,
			warc_record_length BIGINT
		)
	`); err != nil {
		t.Fatal(err)
	}
	for _, page := range pages {
		if _, err := database.Exec(
			"INSERT INTO main.pages VALUES (?, ?, ?, ?, ?, ?)",
			page.warcIndex, page.domain, page.url, page.rank, page.offset, page.length,
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

func TestLoadPartStatsAggregatesPagesPerPart(t *testing.T) {
	path := writeClassifyFixture(t, []classifyFixturePage{
		// part 0: 3 pages
		{warcIndex: 0, domain: "a.example", url: "https://a.example/1", rank: 1, offset: 0, length: 100},
		{warcIndex: 0, domain: "a.example", url: "https://a.example/2", rank: 2, offset: 100, length: 200},
		{warcIndex: 0, domain: "a.example", url: "https://a.example/3", rank: 3, offset: 300, length: 300},
		// part 1: 1 page
		{warcIndex: 1, domain: "b.example", url: "https://b.example/1", rank: 1, offset: 0, length: 50},
		// part 2: absent
		// part 3: 5 pages
		{warcIndex: 3, domain: "c.example", url: "https://c.example/1", rank: 1, offset: 0, length: 10},
		{warcIndex: 3, domain: "c.example", url: "https://c.example/2", rank: 2, offset: 10, length: 20},
		{warcIndex: 3, domain: "c.example", url: "https://c.example/3", rank: 3, offset: 30, length: 30},
		{warcIndex: 3, domain: "c.example", url: "https://c.example/4", rank: 4, offset: 60, length: 40},
		{warcIndex: 3, domain: "c.example", url: "https://c.example/5", rank: 5, offset: 100, length: 50},
	})

	stats, err := LoadPartStats(context.Background(), path, 0, 3)
	if err != nil {
		t.Fatal(err)
	}

	want := []PartStats{
		{WarcIndex: 0, Pages: 3, SelectedBytes: 600},
		{WarcIndex: 1, Pages: 1, SelectedBytes: 50},
		{WarcIndex: 3, Pages: 5, SelectedBytes: 150},
	}
	if !reflect.DeepEqual(stats, want) {
		t.Fatalf("stats = %+v, want %+v", stats, want)
	}

	classification := ClassifyParts(stats, 0, 3, 2)
	if !reflect.DeepEqual(classification.Remote, []uint32{1}) {
		t.Fatalf("Remote = %v, want [1]", classification.Remote)
	}
	if !reflect.DeepEqual(classification.Local, []uint32{0, 3}) {
		t.Fatalf("Local = %v, want [0,3]", classification.Local)
	}
	if !reflect.DeepEqual(classification.Empty, []uint32{2}) {
		t.Fatalf("Empty = %v, want [2]", classification.Empty)
	}
}
