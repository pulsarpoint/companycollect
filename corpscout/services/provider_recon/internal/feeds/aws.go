package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

const awsURL = "https://ip-ranges.amazonaws.com/ip-ranges.json"

// AWS collects ip-ranges.json; tags are AWS service names (EC2, CLOUDFRONT, AMAZON…).
type AWS struct {
	noParams
	URL string
}

// NewAWS returns the collector for the official feed.
func NewAWS() *AWS { return &AWS{URL: awsURL} }

// Name implements Collector.
func (*AWS) Name() string { return "aws_ip_ranges" }

// Collect implements Collector.
func (c *AWS) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		SyncToken string `json:"syncToken"`
		Prefixes  []struct {
			IPPrefix string `json:"ip_prefix"`
			Region   string `json:"region"`
			Service  string `json:"service"`
		} `json:"prefixes"`
		IPv6Prefixes []struct {
			IPv6Prefix string `json:"ipv6_prefix"`
			Region     string `json:"region"`
			Service    string `json:"service"`
		} `json:"ipv6_prefixes"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("aws: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: "syncToken=" + feed.SyncToken}}
	for _, p := range feed.Prefixes {
		b.add(p.IPPrefix, p.Service, p.Region)
	}
	for _, p := range feed.IPv6Prefixes {
		b.add(p.IPv6Prefix, p.Service, p.Region)
	}
	return finish(b.res)
}
