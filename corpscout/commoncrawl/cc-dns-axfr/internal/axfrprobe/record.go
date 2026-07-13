package axfrprobe

import (
	"encoding/binary"
	"strings"

	"cc-dns-axfr/internal/model"

	"github.com/miekg/dns"
)

// recordFromRR converts every DNS RR through the same type-agnostic path. Numeric type/class and
// binary RDATA are the lossless protocol representation; RecordType and Value remain convenient for
// operators and ClickHouse queries even when the RR is an RFC3597 unknown type.
func recordFromRR(rr dns.RR, slot, rcode, source, discovery string) model.DNSRecord {
	header := rr.Header()
	record := model.DNSRecord{
		Name:       strings.TrimSuffix(strings.ToLower(header.Name), "."),
		RecordType: dns.Type(header.Rrtype).String(),
		TypeCode:   header.Rrtype,
		ClassCode:  header.Class,
		Slot:       slot,
		Value:      rdataPresentation(rr),
		RDataWire:  rdataWire(rr),
		Rcode:      rcode,
		TTL:        header.Ttl,
		Source:     source,
		Discovery:  discovery,
	}

	switch value := rr.(type) {
	case *dns.A:
		record.Finding = addressFinding(value.A.String())
	case *dns.AAAA:
		record.Finding = addressFinding(value.AAAA.String())
	case *dns.MX:
		record.Priority = value.Preference
	case *dns.SRV:
		record.Priority = value.Priority
	case *dns.KX:
		record.Priority = value.Preference
	case *dns.RT:
		record.Priority = value.Preference
	case *dns.SVCB:
		record.Priority = value.Priority
	case *dns.HTTPS:
		record.Priority = value.Priority
	}
	return record
}

// addressFinding flags a non-public A/AAAA value returned by a public authoritative server. It is
// observation metadata only; record values never become scan targets.
func addressFinding(value string) string {
	if scope, ok := ClassifyString(value); ok && scope != ScopePublic {
		return "public_dns_private_address"
	}
	return ""
}

func rdataPresentation(rr dns.RR) string {
	presentation := rr.String()
	remaining := presentation
	for range 4 {
		separator := strings.IndexByte(remaining, '\t')
		if separator < 0 {
			return strings.TrimSpace(presentation)
		}
		remaining = remaining[separator+1:]
	}
	return strings.TrimSpace(remaining)
}

// rdataWire packs one standalone RR without DNS name compression, then removes its owner and fixed
// header. Returning a Go string is intentional: Go strings, SQLite BLOB, and ClickHouse String all
// preserve arbitrary bytes including NUL.
func rdataWire(rr dns.RR) string {
	wire := make([]byte, dns.Len(rr))
	end, err := dns.PackRR(rr, wire, 0, nil, false)
	if err != nil {
		return ""
	}
	_, ownerEnd, err := dns.UnpackDomainName(wire[:end], 0)
	if err != nil || ownerEnd+10 > end {
		return ""
	}
	rdataLength := int(binary.BigEndian.Uint16(wire[ownerEnd+8 : ownerEnd+10]))
	rdataStart := ownerEnd + 10
	if rdataStart+rdataLength > end {
		return ""
	}
	return string(wire[rdataStart : rdataStart+rdataLength])
}
