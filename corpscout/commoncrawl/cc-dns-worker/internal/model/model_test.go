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

func TestScanRowColumns(t *testing.T) {
	cols := chCols(ScanRow{})
	for _, c := range []string{
		"root_domain", "etld", "nameservers", "ns_ips", "ns_endpoint_names", "ns_endpoint_ips",
		"ns_endpoint_scopes", "ns_endpoint_dialable", "dnssec_signed", "ds_present", "status",
		"queries_total", "queries_ok", "last_run_id", "resolved_at",
	} {
		if !cols[c] {
			t.Errorf("ScanRow missing ch column %q", c)
		}
	}
}

func TestRecordObservationRowColumns(t *testing.T) {
	cols := chCols(RecordObservationRow{})
	for _, c := range []string{
		"root_domain", "name", "record_type", "slot", "value", "source", "discovery", "scan_id",
		"ttl", "priority", "rcode", "observed_at", "loaded_at",
	} {
		if !cols[c] {
			t.Errorf("RecordObservationRow missing ch column %q", c)
		}
	}
}
