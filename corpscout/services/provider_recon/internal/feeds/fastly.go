package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

// Fastly collects the edge network ranges.
type Fastly struct {
	noParams
	URL string
}

// NewFastly returns the collector for the official feed.
func NewFastly() *Fastly { return &Fastly{URL: "https://api.fastly.com/public-ip-list"} }

// Name implements Collector.
func (*Fastly) Name() string { return "fastly_public_ips" }

// Format implements Collector.
func (*Fastly) Format() string { return "JSON API" }

// Collect implements Collector.
func (c *Fastly) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		Addresses []string `json:"addresses"`
		IPv6      []string `json:"ipv6_addresses"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("fastly: decode: %w", err)
	}
	if len(feed.Addresses) == 0 || len(feed.IPv6) == 0 {
		return Result{}, shapeErr("fastly: expected addresses and ipv6_addresses, got %d and %d", len(feed.Addresses), len(feed.IPv6))
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: bodyVersion(resp.Body)}}
	for _, s := range append(feed.Addresses, feed.IPv6...) {
		b.add(s, "FASTLY", "")
	}
	return finish(b.res)
}
