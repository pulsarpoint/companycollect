package main

import (
	"context"
	"time"

	"cc-dns-worker/internal/model"
	"cc-dns-worker/internal/resolve"
	"cc-dns-worker/internal/scheduler"
	"cc-dns-worker/internal/store"
)

type axfrProber interface {
	ProbeServer(ctx context.Context, zone, nsHost, nsIP string) resolve.AXFROutcome
}

type axfrDomainResult struct {
	domain     string
	probes     []resolve.AXFROutcome
	zone       []model.DNSRecord
	observedAt time.Time
}

// processAXFRTarget probes each dialable, non-hyperscaler IP once while preserving every NS hostname
// endpoint identity in the local result.
func processAXFRTarget(ctx context.Context, prober axfrProber, target store.AXFRTarget) axfrDomainResult {
	now := time.Now().UTC()
	var zone []model.DNSRecord
	var probes []resolve.AXFROutcome
	probesByIP := map[string]resolve.AXFROutcome{}
	for _, endpoint := range target.Endpoints {
		if !endpoint.Dialable || scheduler.IsHyperscaler(endpoint.IP) {
			continue
		}
		outcome, exists := probesByIP[endpoint.IP]
		if !exists {
			outcome = prober.ProbeServer(ctx, target.RootDomain, endpoint.Name, endpoint.IP)
			probesByIP[endpoint.IP] = outcome
		}
		outcome.NSHost, outcome.NSIP = endpoint.Name, endpoint.IP
		probes = append(probes, outcome)
		if outcome.IsOpen() && zone == nil {
			zone = outcome.Zone
		}
	}
	return axfrDomainResult{domain: target.RootDomain, probes: probes, zone: zone, observedAt: now}
}
