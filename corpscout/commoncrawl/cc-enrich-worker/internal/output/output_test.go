package output

import (
	"reflect"
	"strings"
	"testing"
)

// Every output row writes to ClickHouse two ways — the Parquet file (parquet tag) and the native
// INSERT (ch tag). They MUST name the same column, or the `load` command silently maps to the
// wrong/missing column. Pin ch == the parquet column name (the part before any ,option).
func TestRowTagsConsistent(t *testing.T) {
	for _, v := range []any{DomainRow{}, IndustryRow{}, PageSignalRow{}, MetadataRow{}, ContactRow{}, TechRow{}, IdentifierRow{}} {
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
