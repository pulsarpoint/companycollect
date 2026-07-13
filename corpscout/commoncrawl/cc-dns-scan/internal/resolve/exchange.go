// Package resolve queries DNS in two tiers: Tier-1 discovery via recursive resolvers (discover.go)
// and Tier-2 record queries directly against authoritative servers (query.go). exchange.go is the
// shared transport: one UDP query (TCP on truncation) routed through a per-server scheduler so no
// server is hit too fast.
package resolve

import (
	"context"
	"net"
	"time"

	"cc-dns-scan/internal/metrics"
	"cc-dns-scan/internal/scheduler"

	"github.com/miekg/dns"
)

// Exchanger sends one DNS message to one server IP and returns the reply.
type Exchanger interface {
	Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error)
}

type client struct {
	sched   *scheduler.Scheduler
	stats   *metrics.Stats // optional; counts queries actually sent + their errors
	udp     *dns.Client
	tcp     *dns.Client
	timeout time.Duration
}

// NewExchanger returns an Exchanger that paces every send through sched. serverIP may be a bare IP
// (port 53 assumed) or ip:port. The caller sets RecursionDesired on m (true for discovery, false
// for direct-authoritative record queries).
func NewExchanger(sched *scheduler.Scheduler, timeout time.Duration) Exchanger {
	return NewExchangerWithStats(sched, timeout, nil)
}

// NewExchangerWithStats is NewExchanger plus a metrics counter: every DNS query actually sent (past
// the scheduler's rate/breaker gate) increments stats.Queries, and a failed send increments
// stats.QueryErrors. stats may be nil (no metrics).
func NewExchangerWithStats(sched *scheduler.Scheduler, timeout time.Duration, stats *metrics.Stats) Exchanger {
	return &client{
		sched:   sched,
		stats:   stats,
		udp:     &dns.Client{Net: "udp", Timeout: timeout, UDPSize: 1232},
		tcp:     &dns.Client{Net: "tcp", Timeout: timeout},
		timeout: timeout,
	}
}

func withPort(ip string) string {
	if _, _, err := net.SplitHostPort(ip); err == nil {
		return ip
	}
	return net.JoinHostPort(ip, "53")
}

func hostOnly(ip string) string {
	if h, _, err := net.SplitHostPort(ip); err == nil {
		return h
	}
	return ip
}

func (c *client) Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error) {
	m.SetEdns0(1232, true) // DO bit + large UDP buffer on every query
	addr := withPort(serverIP)
	var resp *dns.Msg
	err := c.sched.Do(ctx, hostOnly(serverIP), func() error {
		// Inside Do => past the rate/breaker gate, so a real query is being sent (an open circuit
		// returns before this runs and is not counted as traffic).
		if c.stats != nil {
			c.stats.Queries.Add(1)
		}
		r, _, err := c.udp.ExchangeContext(ctx, m, addr)
		if err == nil && r.Truncated {
			r, _, err = c.tcp.ExchangeContext(ctx, m, addr)
		}
		if err != nil {
			if c.stats != nil {
				c.stats.QueryErrors.Add(1)
			}
			return err
		}
		// NXDOMAIN and NOERROR/NODATA are valid negative DNS outcomes. Every other non-success RCODE
		// means the sent query reached a server but failed and therefore belongs in the health error
		// rate alongside transport failures and timeouts.
		if r.Rcode != dns.RcodeSuccess && r.Rcode != dns.RcodeNameError {
			if c.stats != nil {
				c.stats.QueryErrors.Add(1)
			}
		}
		resp = r
		return nil
	})
	return resp, err
}
