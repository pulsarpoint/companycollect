package resolve

import (
	"context"
	"strconv"
	"strings"
	"time"

	"cc-dns-worker/internal/model"

	"github.com/miekg/dns"
)

// AXFRCaps bounds a single transfer so a hostile or huge zone cannot stall a worker or exhaust
// memory while it is drained. Every cap is enforced per-RR, not per-envelope.
type AXFRCaps struct {
	MaxRecords int           // stop appending past this many RRs
	MaxBytes   int           // stop once the running sum of RR sizes reaches this
	Deadline   time.Duration // whole-transfer timeout
}

// AXFRResult is the outcome of probing one zone. Zone holds the transferred records (tagged
// Source="axfr"); it is retained and folded into the domain's record set by the caller.
type AXFRResult struct {
	Open      bool
	Server    string
	Records   int
	Truncated bool
	Zone      []model.DNSRecord
}

// transferAXFR runs one TCP AXFR against nsIP for zone, draining up to the caps. A REFUSED/NOTAUTH
// response or any transport error yields Open=false with an empty Zone. err is non-nil only on a
// transport/setup failure (so the caller can rotate servers); a clean REFUSED returns (Open:false, nil).
//
// Resource safety: miekg's transfer goroutine sends on an unbuffered channel, so abandoning the channel
// on an early exit (cap hit, ctx cancel) would block that goroutine forever and leak its TCP socket. A
// watchdog closes the connection when ctx fires (whole-transfer deadline, caller cancel, or our own
// cancel on the way out), and a deferred drain reads the channel to completion so the producer always
// terminates. Together they make Deadline a real whole-transfer ceiling and guarantee no leak on any path.
func transferAXFR(ctx context.Context, zone, nsIP string, caps AXFRCaps) (AXFRResult, error) {
	res := AXFRResult{Server: nsIP}
	deadline := caps.Deadline
	if deadline <= 0 {
		deadline = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, deadline)

	m := new(dns.Msg)
	m.SetAxfr(dns.Fqdn(zone))
	tr := &dns.Transfer{DialTimeout: deadline, ReadTimeout: deadline}
	ch, err := tr.In(m, withPort(nsIP))
	if err != nil {
		cancel()
		return res, err // transport/dial failure — caller rotates
	}
	// Watchdog: when ctx is done, close the conn so the producer's blocked ReadMsg/send errors out,
	// letting its goroutine return and close ch.
	go func() {
		<-ctx.Done()
		if tr.Conn != nil {
			_ = tr.Conn.Close()
		}
	}()
	// Always drain to completion on the way out: cancel() trips the watchdog (fast stop even mid-zone),
	// then draining unblocks the producer's pending send so it can finish and close ch. On normal
	// completion ch is already closed, so this is a no-op.
	defer func() {
		cancel()
		for range ch {
		}
	}()

	bytes := 0
	for env := range ch {
		if env.Error != nil {
			// REFUSED / NOTAUTH / malformed: producer is already returning; not a transport error.
			return finalize(res), nil
		}
		for _, rr := range env.RR {
			if (caps.MaxRecords > 0 && res.Records >= caps.MaxRecords) ||
				(caps.MaxBytes > 0 && bytes >= caps.MaxBytes) {
				res.Truncated = true
				return finalize(res), nil
			}
			bytes += dns.Len(rr)
			if rec, ok := axfrRecord(rr); ok {
				res.Zone = append(res.Zone, rec)
			}
			res.Records++
		}
	}
	return finalize(res), nil
}

// finalize marks a transfer open iff it collected at least one RR (a well-formed AXFR always includes
// the SOA).
func finalize(res AXFRResult) AXFRResult {
	res.Open = res.Records > 0
	return res
}

// axfrRecord converts one transferred RR into a model.DNSRecord tagged Source="axfr". The slot is
// empty (AXFR names are not tied to the query-plan slots); the name is the record owner, no trailing
// dot. Unsupported RR types are skipped (ok=false).
func axfrRecord(rr dns.RR) (model.DNSRecord, bool) {
	name := strings.TrimSuffix(strings.ToLower(rr.Header().Name), ".")
	rec := model.DNSRecord{Name: name, Slot: "", Rcode: "NOERROR", TTL: rr.Header().Ttl, Source: "axfr"}
	switch v := rr.(type) {
	case *dns.A:
		rec.RecordType, rec.Value = "A", v.A.String()
	case *dns.AAAA:
		rec.RecordType, rec.Value = "AAAA", v.AAAA.String()
	case *dns.CNAME:
		rec.RecordType, rec.Value = "CNAME", strings.TrimSuffix(strings.ToLower(v.Target), ".")
	case *dns.MX:
		rec.RecordType = "MX"
		rec.Priority = v.Preference
		rec.Value = strconv.Itoa(int(v.Preference)) + " " + strings.TrimSuffix(strings.ToLower(v.Mx), ".")
	case *dns.NS:
		rec.RecordType, rec.Value = "NS", strings.TrimSuffix(strings.ToLower(v.Ns), ".")
	case *dns.TXT:
		rec.RecordType, rec.Value = "TXT", strings.Join(v.Txt, "")
	case *dns.SRV:
		rec.RecordType = "SRV"
		rec.Priority = v.Priority
		rec.Value = strings.TrimSpace(v.String()[len(v.Hdr.String()):])
	case *dns.SOA:
		rec.RecordType, rec.Value = "SOA", strings.TrimSpace(v.String()[len(v.Hdr.String()):])
	default:
		return rec, false
	}
	return rec, true
}
