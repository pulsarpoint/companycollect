// Package resolve queries DNS in two tiers: Tier-1 discovery via recursive resolvers (discover.go)
// and Tier-2 record queries directly against authoritative servers (query.go). exchange.go is the
// shared transport: one UDP query (TCP on truncation) routed through a per-server scheduler so no
// server is hit too fast.
package resolve

import (
	"context"
	"net"
	"time"

	"cc-dns-worker/internal/scheduler"

	"github.com/miekg/dns"
)

// Exchanger sends one DNS message to one server IP and returns the reply.
type Exchanger interface {
	Exchange(ctx context.Context, m *dns.Msg, serverIP string) (*dns.Msg, error)
}

type client struct {
	sched   *scheduler.Scheduler
	udp     *dns.Client
	tcp     *dns.Client
	timeout time.Duration
}

// NewExchanger returns an Exchanger that paces every send through sched. serverIP may be a bare IP
// (port 53 assumed) or ip:port. The caller sets RecursionDesired on m (true for discovery, false
// for direct-authoritative record queries).
func NewExchanger(sched *scheduler.Scheduler, timeout time.Duration) Exchanger {
	return &client{
		sched:   sched,
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
		r, _, err := c.udp.ExchangeContext(ctx, m, addr)
		if err != nil {
			return err
		}
		if r.Truncated {
			r, _, err = c.tcp.ExchangeContext(ctx, m, addr)
			if err != nil {
				return err
			}
		}
		resp = r
		return nil
	})
	return resp, err
}
