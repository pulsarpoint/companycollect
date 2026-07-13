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

func TestRecordObservationRowColumns(t *testing.T) {
	cols := chCols(RecordObservationRow{})
	for _, c := range []string{
		"root_domain", "name", "record_type", "record_type_code", "record_class_code", "slot",
		"value", "rdata_wire", "source", "discovery", "name_server", "name_server_ip", "scan_id",
		"ttl", "priority", "rcode", "observed_at", "loaded_at",
	} {
		if !cols[c] {
			t.Errorf("RecordObservationRow missing ch column %q", c)
		}
	}
}

func TestHostnameRowColumns(t *testing.T) {
	cols := chCols(HostnameRow{})
	for _, column := range []string{
		"root_domain", "label", "discovery_source", "first_seen", "last_seen", "last_resolved",
	} {
		if !cols[column] {
			t.Errorf("HostnameRow missing ch column %q", column)
		}
	}
}
