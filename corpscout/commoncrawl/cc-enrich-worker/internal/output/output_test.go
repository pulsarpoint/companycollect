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
	for _, v := range []any{DomainRow{}, TechRow{}, IdentifierRow{}, ProfileRow{}, RegistryRow{}} {
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
