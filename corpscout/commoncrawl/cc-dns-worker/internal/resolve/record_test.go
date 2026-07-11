package resolve

import (
	"bytes"
	"testing"
)

func TestRecordFromRRRetainsPresentationAndWireForEveryType(t *testing.T) {
	tests := []string{
		"example.com. 300 IN A 192.0.2.1",
		"example.com. 300 IN AAAA 2001:db8::1",
		`example.com. 300 IN CAA 0 issue "letsencrypt.org"`,
		"1.2.0.192.in-addr.arpa. 300 IN PTR host.example.com.",
		`example.com. 300 IN NAPTR 10 20 "S" "SIP+D2U" "" _sip._udp.example.com.`,
		"_443._tcp.example.com. 300 IN TLSA 3 1 1 DEADBEEF",
		"example.com. 300 IN SSHFP 1 1 DEADBEEF",
		"example.com. 300 IN DS 12345 13 2 DEADBEEF",
		"example.com. 300 IN DNSKEY 257 3 13 AwEAAQ==",
		"example.com. 300 IN NSEC next.example.com. A NS SOA RRSIG NSEC DNSKEY",
		`example.com. 300 IN SVCB 1 svc.example.com. alpn="h2"`,
		`example.com. 300 IN HTTPS 1 . alpn="h2,h3"`,
		`example.com. 300 IN TYPE65400 \# 4 DEADBEEF`,
	}

	for _, text := range tests {
		rr := mustRR(t, text)
		record := recordFromRR(rr, "", "NOERROR", "axfr", "axfr")
		if record.TypeCode != rr.Header().Rrtype || record.ClassCode != rr.Header().Class {
			t.Errorf("%s: numeric identity lost: %+v", text, record)
		}
		if record.RecordType == "" || record.Value == "" || record.RDataWire == "" {
			t.Errorf("%s: incomplete representation: type=%q value=%q wire=%x", text, record.RecordType, record.Value, record.RDataWire)
		}
	}
}

func TestRecordFromRRPreservesTXTChunks(t *testing.T) {
	rr := mustRR(t, `example.com. 300 IN TXT "part one" "part two"`)
	record := recordFromRR(rr, "", "NOERROR", "axfr", "axfr")
	if record.Value != `"part one" "part two"` {
		t.Errorf("TXT presentation = %q", record.Value)
	}
	want := append([]byte{8}, []byte("part one")...)
	want = append(want, byte(8))
	want = append(want, []byte("part two")...)
	if !bytes.Equal([]byte(record.RDataWire), want) {
		t.Errorf("TXT wire = %x, want %x", record.RDataWire, want)
	}
}

func TestRecordFromRRRetainsRFC3597Bytes(t *testing.T) {
	rr := mustRR(t, `unknown.example.com. 60 CLASS65280 TYPE65400 \# 4 DEADBEEF`)
	record := recordFromRR(rr, "", "NOERROR", "axfr", "axfr")
	if record.TypeCode != 65400 || record.ClassCode != 65280 || record.RecordType != "TYPE65400" {
		t.Fatalf("unknown identity lost: %+v", record)
	}
	if !bytes.Equal([]byte(record.RDataWire), []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Errorf("unknown RDATA = %x", record.RDataWire)
	}
}
