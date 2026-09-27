package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

type googleFeed struct {
	SyncToken string `json:"syncToken"`
	Prefixes  []struct {
		IPv4Prefix string `json:"ipv4Prefix"`
		IPv6Prefix string `json:"ipv6Prefix"`
		Service    string `json:"service"`
		Scope      string `json:"scope"`
	} `json:"prefixes"`
}

func collectGoogle(ctx context.Context, f *Fetcher, url, fixedTag string) (Result, error) {
	resp, err := f.Get(ctx, url, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed googleFeed
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("google: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: url, SourceVersion: "syncToken=" + feed.SyncToken}}
	for _, p := range feed.Prefixes {
		tag := fixedTag
		if tag == "" {
			tag = p.Service
		}
		raw := p.IPv4Prefix
		if raw == "" {
			raw = p.IPv6Prefix
		}
		b.add(raw, tag, p.Scope)
	}
	return finish(b.res)
}

// GoogleCloud collects cloud.json: ranges customers can use in Google Cloud.
type GoogleCloud struct {
	noParams
	URL string
}

// NewGoogleCloud returns the collector for the official feed.
func NewGoogleCloud() *GoogleCloud {
	return &GoogleCloud{URL: "https://www.gstatic.com/ipranges/cloud.json"}
}

// Name implements Collector.
func (*GoogleCloud) Name() string { return "google_cloud" }

// Collect implements Collector.
func (c *GoogleCloud) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	return collectGoogle(ctx, f, c.URL, "")
}

// GoogleGoog collects goog.json: all Google-owned ranges (a superset of
// cloud.json). Longest-prefix lookup lets the more specific cloud ranges win.
type GoogleGoog struct {
	noParams
	URL string
}

// NewGoogleGoog returns the collector for the official feed.
func NewGoogleGoog() *GoogleGoog {
	return &GoogleGoog{URL: "https://www.gstatic.com/ipranges/goog.json"}
}

// Name implements Collector.
func (*GoogleGoog) Name() string { return "google_goog" }

// Collect implements Collector.
func (c *GoogleGoog) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	return collectGoogle(ctx, f, c.URL, "GOOG")
}
