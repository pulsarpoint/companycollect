package model

import (
	"reflect"
	"testing"
)

func chCols(v any) map[string]bool {
	rt := reflect.TypeOf(v)
	out := map[string]bool{}
	for i := 0; i < rt.NumField(); i++ {
		if c := rt.Field(i).Tag.Get("ch"); c != "" {
			out[c] = true
		}
	}
	return out
}

func TestRecordRowColumns(t *testing.T) {
	cols := chCols(RecordRow{})
	for _, c := range []string{"scan_id", "root_domain", "name", "record_type", "slot", "value", "ttl", "priority", "rcode", "source_run_id", "resolved_at"} {
		if !cols[c] {
			t.Errorf("RecordRow missing ch column %q", c)
		}
	}
}

func TestScanRowColumns(t *testing.T) {
	cols := chCols(ScanRow{})
	for _, c := range []string{"scan_id", "root_domain", "etld", "nameservers", "ns_ips", "dnssec_signed", "ds_present", "status", "error", "queries_total", "queries_ok", "source_run_id", "resolved_at"} {
		if !cols[c] {
			t.Errorf("ScanRow missing ch column %q", c)
		}
	}
}
