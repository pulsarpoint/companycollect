package output

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

// The text-heavy outputs compress 3-5x with zstd; uncompressed parquet costs multiple GB per part.
func TestWritersProduceZstdCompressedParquet(t *testing.T) {
	p := filepath.Join(t.TempDir(), "tech.parquet")
	rows := []TechRow{{CrawlID: "c", RootDomain: "acme.com", Technology: "WordPress"}}
	if err := WriteTech(p, rows); err != nil {
		t.Fatal(err)
	}
	if codec := fileCodec(t, p); codec != format.Zstd {
		t.Fatalf("tech.parquet codec = %v, want Zstd", codec)
	}
}

func fileCodec(t *testing.T, path string) format.CompressionCodec {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, st.Size())
	if err != nil {
		t.Fatal(err)
	}
	return pf.Metadata().RowGroups[0].Columns[0].MetaData.Codec
}

// Every output row writes to ClickHouse two ways — the Parquet file (parquet tag) and the native
// INSERT (ch tag). They MUST name the same column, or the `load` command silently maps to the
// wrong/missing column. Pin ch == the parquet column name (the part before any ,option).
func TestRowTagsConsistent(t *testing.T) {
	for _, v := range []any{DomainRow{}, IndustryRow{}, PageSignalRow{}, MetadataRow{}, ContactRow{}, TechRow{}, IdentifierRow{}, SecurityRow{}, PageMetaRow{}} {
		rt := reflect.TypeOf(v)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			pq := strings.Split(f.Tag.Get("parquet"), ",")[0]
			ch := f.Tag.Get("ch")
			switch {
			case pq == "":
				t.Errorf("%s.%s: missing parquet tag", rt.Name(), f.Name)
			case ch == "":
				t.Errorf("%s.%s: missing ch tag", rt.Name(), f.Name)
			case ch != pq:
				t.Errorf("%s.%s: ch=%q != parquet column %q", rt.Name(), f.Name, ch, pq)
			}
		}
	}
}

func TestPageEvidenceColumnsMatchClickHouse(t *testing.T) {
	tests := []struct {
		name string
		row  any
		want []string
	}{
		{
			name: "technologies",
			row:  TechRow{},
			want: []string{
				"crawl_id", "root_domain", "page_url", "subdomain",
				"warc_index", "warc_filename", "warc_record_offset", "warc_record_length",
				"technology", "category", "version", "confidence", "source_run_id", "resolved_at",
			},
		},
		{
			name: "metadata",
			row:  MetadataRow{},
			want: []string{
				"crawl_id", "root_domain", "page_url", "subdomain",
				"warc_index", "warc_filename", "warc_record_offset", "warc_record_length",
				"name", "description", "logo", "country", "founding_year", "employee_count", "source",
				"source_run_id", "resolved_at",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := reflect.TypeOf(test.row)
			got := make([]string, 0, rt.NumField())
			for i := 0; i < rt.NumField(); i++ {
				got = append(got, rt.Field(i).Tag.Get("ch"))
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("columns = %v, want %v", got, test.want)
			}
		})
	}
}
