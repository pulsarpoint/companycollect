package resolve

import (
	"context"
	"errors"
	"net"
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
	Deadline   time.Duration // per-connection timeout (the preflight and, if open, the collection each get one)
}

// AXFRVerdict is the definitive classification of one AXFR probe against one nameserver. Only Open and
// Closed are definitive; Unknown means the probe never reached a real answer (timeout, transport
// failure, SERVFAIL, malformed reply, circuit breaker, cancellation, or a skipped target) and must never
// be treated as either Open or Closed, and must never overwrite a previously observed Open/Closed state.
type AXFRVerdict string

const (
	// VerdictOpen means a valid AXFR leading SOA was observed. The zone stays Open even if the
	// subsequent record collection is capped, partial, or fails outright — Truncated carries that.
	VerdictOpen AXFRVerdict = "open"
	// VerdictClosed means the server gave a definitive "no zone transfer here" (REFUSED/NOTAUTH).
	VerdictClosed AXFRVerdict = "closed"
	// VerdictUnknown means no definitive answer was reached.
	VerdictUnknown AXFRVerdict = "unknown"
)

// AXFRReason narrows why an AXFROutcome's Verdict landed where it did.
type AXFRReason string

const (
	ReasonTransferred    AXFRReason = "transferred"     // preflight saw RCODE NOERROR with a leading SOA: the server opened the zone
	ReasonRefused        AXFRReason = "refused"         // preflight RCODE REFUSED
	ReasonNotAuth        AXFRReason = "notauth"         // preflight RCODE NOTAUTH
	ReasonServfail       AXFRReason = "servfail"        // preflight RCODE SERVFAIL
	ReasonTimeout        AXFRReason = "timeout"         // a dial/read/write deadline (ours or derived from ctx) elapsed
	ReasonTransportError AXFRReason = "transport_error" // dial/read/write failed for any other reason
	ReasonCircuitOpen    AXFRReason = "circuit_open"    // the scheduler's per-server breaker is open
	ReasonCancelled      AXFRReason = "cancelled"       // the caller's context was cancelled (not a deadline)
	ReasonMalformed      AXFRReason = "malformed"       // RCODE NOERROR but the first RR was not a SOA, or an unhandled non-NOERROR rcode
	ReasonSkipped        AXFRReason = "skipped"         // the target was never dialed (a caller-side skip, e.g. hyperscaler/non-dialable)
)

// AXFROutcome is the typed, definitive-vs-unknown result of probing one authoritative nameserver for
// zone. Zone holds the transferred records (tagged Source="axfr"); it is retained and folded into the
// domain's record set by the caller.
//
// Verdict/Reason answer "did this NS allow a zone transfer", independent of whether the full zone was
// captured: Truncated (a capped or mid-stream-dropped collection) can be true alongside Verdict==Open
// without changing the verdict — an open zone stays open even when only part of it was collected.
type AXFROutcome struct {
	Verdict AXFRVerdict
	Reason  AXFRReason

	NSHost string // authoritative NS hostname, if known to the caller (may be empty)
	NSIP   string // the IP actually dialed

	Records   int // count of every RR collected (including SOA); equals len(Zone)
	Bytes     int // running sum of collected RR wire sizes, bounded by AXFRCaps.MaxBytes
	Truncated bool
	Zone      []model.DNSRecord

	ObservedAt time.Time
}

// IsOpen reports whether the probe proved the zone transfers.
func (o AXFROutcome) IsOpen() bool { return o.Verdict == VerdictOpen }

// IsClosed reports whether the probe proved the server refuses a transfer.
func (o AXFROutcome) IsClosed() bool { return o.Verdict == VerdictClosed }

// IsDefinitive reports whether the probe reached a definitive answer (Open or Closed) rather than
// Unknown. Callers must gate any axfr_open state-change recording on this: an Unknown outcome must
// never flip, or be compared against, a previously observed state.
func (o AXFROutcome) IsDefinitive() bool {
	return o.Verdict == VerdictOpen || o.Verdict == VerdictClosed
}

// transferAXFR probes nsIP for zone. A definitive TCP preflight (preflightAXFR) decides Open/Closed/
// Unknown from the first DNS message's RCODE alone — it never depends on miekg/dns's unexported "bad
// xfr rcode" error text. Only when the preflight proves Open does it dial a second connection
// (collectZone) to stream the zone up to caps; open servers are rare, so the extra round trip is cheap
// in aggregate. Any trouble during collection (dial failure, mid-stream drop, or a cap) never changes
// Verdict away from Open — it only sets Truncated.
func transferAXFR(ctx context.Context, zone, nsIP string, caps AXFRCaps) AXFROutcome {
	deadline := caps.Deadline
	if deadline <= 0 {
		deadline = 20 * time.Second
	}

	out := AXFROutcome{NSIP: nsIP}
	out.Verdict, out.Reason = preflightAXFR(ctx, zone, nsIP, deadline)
	out.ObservedAt = time.Now().UTC()
	if out.Verdict != VerdictOpen {
		return out
	}
	collectZone(ctx, zone, nsIP, deadline, caps, &out)
	return out
}

// preflightAXFR sends one AXFR query over its own TCP connection and reads ONLY the first framed DNS
// message: dns.Client.ExchangeContext dials, writes, reads exactly once, and closes the connection — it
// never loops the way dns.Transfer.In's multi-envelope reader does. The verdict therefore comes straight
// from that message's RCODE (and, for NOERROR, its first RR), never from parsing an internal error
// string.
func preflightAXFR(ctx context.Context, zone, nsIP string, deadline time.Duration) (AXFRVerdict, AXFRReason) {
	m := new(dns.Msg)
	m.SetAxfr(dns.Fqdn(zone))
	pctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()

	c := &dns.Client{Net: "tcp", Timeout: deadline}
	r, _, err := c.ExchangeContext(pctx, m, withPort(nsIP))
	if err != nil {
		return VerdictUnknown, classifyErr(pctx, err)
	}

	switch r.Rcode {
	case dns.RcodeSuccess:
		if len(r.Answer) > 0 {
			if _, ok := r.Answer[0].(*dns.SOA); ok {
				return VerdictOpen, ReasonTransferred
			}
		}
		return VerdictUnknown, ReasonMalformed // NOERROR but no leading SOA: not a well-formed AXFR answer
	case dns.RcodeRefused:
		return VerdictClosed, ReasonRefused
	case dns.RcodeNotAuth:
		return VerdictClosed, ReasonNotAuth
	case dns.RcodeServerFailure:
		return VerdictUnknown, ReasonServfail
	default:
		return VerdictUnknown, ReasonMalformed
	}
}

// classifyErr narrows a preflight dial/exchange error into the reason that best explains it: our own
// context cancellation, a deadline firing (ours or the server's), or a genuine dial/read/write failure.
func classifyErr(ctx context.Context, err error) AXFRReason {
	if errors.Is(err, context.Canceled) || ctx.Err() == context.Canceled {
		return ReasonCancelled
	}
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() == context.DeadlineExceeded {
		return ReasonTimeout
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return ReasonTimeout
	}
	return ReasonTransportError
}

// collectZone dials a second TCP connection to nsIP and streams the zone up to caps, folding
// Records/Bytes/Truncated/Zone into out. It never touches out.Verdict/out.Reason: the preflight already
// proved this NS opens the zone, so any trouble here (dial failure, mid-stream drop, or a cap) is a
// partial pull of an open zone, not a different verdict.
//
// Resource safety: miekg's transfer goroutine sends on an unbuffered channel, so abandoning the channel
// on an early exit (cap hit, ctx cancel) would block that goroutine forever and leak its TCP socket. A
// watchdog closes the connection when ctx fires (whole-collection deadline or caller cancel), and a
// deferred drain reads the channel to completion so the producer always terminates. Together they make
// deadline a real ceiling on this connection and guarantee no leak on any path.
func collectZone(ctx context.Context, zone, nsIP string, deadline time.Duration, caps AXFRCaps, out *AXFROutcome) {
	ctx, cancel := context.WithTimeout(ctx, deadline)

	m := new(dns.Msg)
	m.SetAxfr(dns.Fqdn(zone))
	tr := &dns.Transfer{DialTimeout: deadline, ReadTimeout: deadline}
	ch, err := tr.In(m, withPort(nsIP))
	if err != nil {
		cancel()
		out.Truncated = true // couldn't even open the second connection — partial pull of a known-open zone
		return
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

	for env := range ch {
		if env.Error != nil {
			out.Truncated = true // mid-stream read error (or the watchdog forcing a close): partial pull
			return
		}
		for _, rr := range env.RR {
			if (caps.MaxRecords > 0 && out.Records >= caps.MaxRecords) ||
				(caps.MaxBytes > 0 && out.Bytes >= caps.MaxBytes) {
				out.Truncated = true
				return
			}
			out.Bytes += dns.Len(rr)
			out.Zone = append(out.Zone, recordFromRR(rr, "", "NOERROR", "axfr", "axfr"))
			out.Records++
		}
	}
}

// AXFRProber applies probe policy over the low-level transfer and paces every dial through the AXFR
// scheduler lane. It does not cache verdicts across calls — a REFUSED or a transient failure on one
// call says nothing definitive about the next (it may be a different zone serial, a transient network
// blip, or a server that just came back), so every call re-probes; only a genuine, sustained transport
// failure trips the scheduler's own per-server circuit breaker.
type AXFRProber struct {
	sched *scheduler.Scheduler
	caps  AXFRCaps
}

// NewAXFRProber builds a prober over the AXFR scheduler lane.
func NewAXFRProber(sched *scheduler.Scheduler, caps AXFRCaps) *AXFRProber {
	return &AXFRProber{sched: sched, caps: caps}
}

// ProbeServer transfers zone from a single NS IP — the per-NS entry point for the standalone AXFR
// pipeline, which records an outcome per nameserver. It reclassifies the target here, at the last public
// method before the socket opens, so stale persisted Dialable metadata or a future caller cannot bypass
// the public-only policy. NSHost/NSIP are always set on the returned outcome so the caller can key its
// dns_axfr row even when the target is skipped or the scheduler rejects it.
func (p *AXFRProber) ProbeServer(ctx context.Context, zone, nsHost, nsIP string) AXFROutcome {
	fill := func(o AXFROutcome) AXFROutcome {
		o.NSHost, o.NSIP = nsHost, nsIP
		if o.ObservedAt.IsZero() {
			o.ObservedAt = time.Now().UTC()
		}
		return o
	}
	target := hostOnly(nsIP)
	if scope, ok := ClassifyString(target); !ok || !Dialable(scope) {
		return fill(AXFROutcome{Verdict: VerdictUnknown, Reason: ReasonSkipped})
	}

	var out AXFROutcome
	err := p.sched.Do(ctx, nsIP, func() error {
		out = transferAXFR(ctx, zone, nsIP, p.caps)
		return breakerErr(out)
	})
	if err != nil && out.Verdict == "" {
		// sched.Do returned before fn ran at all: the breaker was open, or ctx died while waiting on
		// the rate limiter/in-flight slot.
		reason := classifyErr(ctx, err)
		if errors.Is(err, scheduler.ErrCircuitOpen) {
			reason = ReasonCircuitOpen
		}
		out = AXFROutcome{Verdict: VerdictUnknown, Reason: reason}
	}
	return fill(out)
}

// breakerErr reports whether outcome represents a transport failure that should count against the
// scheduler's per-server circuit breaker — dial/read/write trouble or an unresponsive server. A
// well-formed DNS answer (refused/notauth/servfail/malformed) is not a transport failure, and our own
// context cancellation is not the server's fault, so neither trips the breaker.
func breakerErr(o AXFROutcome) error {
	if o.Verdict == VerdictUnknown && (o.Reason == ReasonTimeout || o.Reason == ReasonTransportError) {
		return errors.New("axfr: " + string(o.Reason))
	}
	return nil
}
