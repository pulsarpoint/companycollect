package feeds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
)

// Cloudflare collects the proxy network ranges from the public API.
type Cloudflare struct {
	noParams
	URL string
}

// NewCloudflare returns the collector for the official feed.
func NewCloudflare() *Cloudflare { return &Cloudflare{URL: "https://api.cloudflare.com/client/v4/ips"} }

// Name implements Collector.
func (*Cloudflare) Name() string { return "cloudflare_ips" }

// Format implements Collector.
func (*Cloudflare) Format() string { return "JSON API" }

// Collect implements Collector.
func (c *Cloudflare) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		Success bool `json:"success"`
		Result  *struct {
			IPv4 []string `json:"ipv4_cidrs"`
			IPv6 []string `json:"ipv6_cidrs"`
			ETag string   `json:"etag"`
		} `json:"result"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("cloudflare: decode: %w", err)
	}
	if !feed.Success || feed.Result == nil {
		return Result{}, errors.New("cloudflare: API reported success=false")
	}
	if len(feed.Result.IPv4) == 0 || len(feed.Result.IPv6) == 0 {
		return Result{}, shapeErr("cloudflare: expected ipv4_cidrs and ipv6_cidrs, got %d and %d", len(feed.Result.IPv4), len(feed.Result.IPv6))
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: "etag=" + feed.Result.ETag}}
	for _, s := range append(feed.Result.IPv4, feed.Result.IPv6...) {
		b.add(s, "CLOUDFLARE", "")
	}
	return finish(b.res)
}
