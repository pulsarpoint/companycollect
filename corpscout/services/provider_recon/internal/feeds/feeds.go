// Package feeds fetches official provider IP-range publications. Each
// collector turns one publication into tagged prefixes; mapping tags to
// services is the definitions' job, not the collector's.
package feeds

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/netip"
	"strings"
)

// Range is one published prefix with the publisher's tag and region.
type Range struct {
	Prefix netip.Prefix
	Tag    string
	Region string
}

// Result is one collector run.
type Result struct {
	SourceURL     string
	SourceVersion string
	Ranges        []Range
	Skipped       int
}

// Collector fetches one publication.
type Collector interface {
	Name() string
	ValidateParams(params map[string]string) error
	Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error)
}

// ErrEmptyFeed means a publication parsed but yielded no usable ranges.
var ErrEmptyFeed = errors.New("feed returned no usable ranges")

// Registry returns every production collector keyed by name.
func Registry() map[string]Collector {
	all := []Collector{
		NewAWS(),
		NewGoogleCloud(),
		NewGoogleGoog(),
		NewCloudflare(),
		NewFastly(),
		NewBunny(),
		NewAzure(),
		NewOracle(),
		NewGitHub(),
		NewGeofeed(),
		NewRIPEstat(),
	}
	out := make(map[string]Collector, len(all))
	for _, c := range all {
		out[c.Name()] = c
	}
	return out
}

type noParams struct{}

func (noParams) ValidateParams(params map[string]string) error {
	if len(params) > 0 {
		return errors.New("collector takes no params")
	}
	return nil
}

// parsePrefix accepts CIDRs (host bits are masked) and bare addresses
// (become /32 or /128).
func parsePrefix(s string) (netip.Prefix, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return netip.Prefix{}, false
	}
	if strings.Contains(s, "/") {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return netip.Prefix{}, false
		}
		return p.Masked(), true
	}
	a, err := netip.ParseAddr(s)
	if err != nil {
		return netip.Prefix{}, false
	}
	return netip.PrefixFrom(a, a.BitLen()), true
}

// bodyVersion is a short content hash for publications without a version field.
func bodyVersion(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:8])
}

type rangeBuilder struct{ res Result }

func (b *rangeBuilder) add(raw, tag, region string) {
	p, ok := parsePrefix(raw)
	if !ok {
		b.res.Skipped++
		return
	}
	b.res.Ranges = append(b.res.Ranges, Range{Prefix: p, Tag: tag, Region: region})
}

func finish(res Result) (Result, error) {
	if len(res.Ranges) == 0 {
		return res, ErrEmptyFeed
	}
	return res, nil
}
