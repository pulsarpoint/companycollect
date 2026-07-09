package resolve

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/scheduler"

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
	Records   int // count of every RR seen (including SOA and unsupported types); axfr_records in the summary may exceed persisted rows (only supported types land in Zone)
	Truncated bool
	Zone      []model.DNSRecord
}

// transferAXFR runs one TCP AXFR against nsIP for zone, draining up to the caps. A REFUSED/NOTAUTH
// response or any mid-stream error yields Open=false. err is non-nil only on a transport/setup
// failure (so the caller can rotate servers); a clean REFUSED returns (Open:false, nil).
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
			// REFUSED / NOTAUTH / mid-stream read error / watchdog-forced close: not an open zone.
			return res, nil
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

// AXFRProber applies probe policy over the low-level transfer: it skips NS sets that are entirely
// hyperscaler (they never allow AXFR), remembers per-NS-set REFUSED verdicts so a chronic refuser is
// probed at most once (a refusal is not a transport error, so the scheduler's breaker never trips on
// it — this dedup is what caps the volume), paces every dial through the AXFR scheduler lane, and
// bounds total concurrent transfers with an aggregate semaphore.
type AXFRProber struct {
	sched *scheduler.Scheduler
	caps  AXFRCaps
	sem   chan struct{}

	refused sync.Map // nsSetKey -> struct{}: NS sets that returned no open zone (refusal OR transport error / exhausted retries) — skip re-probing. NOTE: a transient failure permanently suppresses that NS set for the life of the process.
}

// NewAXFRProber builds a prober over the AXFR scheduler lane. maxInflight bounds total concurrent
// transfers across all domains (aggregate held-open TCP connections); <=0 defaults to 50.
func NewAXFRProber(sched *scheduler.Scheduler, caps AXFRCaps, maxInflight int) *AXFRProber {
	if maxInflight <= 0 {
		maxInflight = 50
	}
	return &AXFRProber{sched: sched, caps: caps, sem: make(chan struct{}, maxInflight)}
}

// Probe transfers zone from the first NS IP that yields data. It skips an all-hyperscaler NS set and
// short-circuits an NS set already known to refuse. A zero-value result (Open=false, Server="") means
// skipped or nothing answered.
func (p *AXFRProber) Probe(ctx context.Context, zone string, nsIPs []string) AXFRResult {
	targets := make([]string, 0, len(nsIPs))
	for _, ip := range nsIPs {
		if !scheduler.IsHyperscaler(ip) {
			targets = append(targets, ip)
		}
	}
	if len(targets) == 0 {
		return AXFRResult{} // all-hyperscaler (or empty): skip
	}
	key := nsSetKey(nsIPs)
	if _, refused := p.refused.Load(key); refused {
		return AXFRResult{}
	}

	select {
	case p.sem <- struct{}{}:
	case <-ctx.Done():
		return AXFRResult{}
	}
	defer func() { <-p.sem }()

	for _, ip := range targets {
		var res AXFRResult
		err := p.sched.Do(ctx, ip, func() error {
			r, e := transferAXFR(ctx, zone, ip, p.caps)
			res = r
			return e
		})
		if err == nil && res.Open {
			return res
		}
	}
	// Nothing answered across the whole NS set — remember it as a refuser so peers skip it.
	p.refused.Store(key, struct{}{})
	return AXFRResult{Server: targets[len(targets)-1]}
}

// nsSetKey is the order-independent identity of an NS IP set (dedup is a property of the server set,
// not the domain).
func nsSetKey(nsIPs []string) string {
	c := append([]string(nil), nsIPs...)
	sort.Strings(c)
	return strings.Join(c, ",")
}
