package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

// Oracle collects OCI public ranges; each CIDR carries one or more tags.
type Oracle struct {
	noParams
	URL string
}

// NewOracle returns the collector for the official feed.
func NewOracle() *Oracle {
	return &Oracle{URL: "https://docs.oracle.com/en-us/iaas/tools/public_ip_ranges.json"}
}

// Name implements Collector.
func (*Oracle) Name() string { return "oracle_public_ip_ranges" }

// Format implements Collector.
func (*Oracle) Format() string { return "JSON" }

// Collect implements Collector.
func (c *Oracle) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		LastUpdated string `json:"last_updated_timestamp"`
		Regions     []struct {
			Region string `json:"region"`
			CIDRs  []struct {
				CIDR string   `json:"cidr"`
				Tags []string `json:"tags"`
			} `json:"cidrs"`
		} `json:"regions"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("oracle: decode: %w", err)
	}
	b := rangeBuilder{res: Result{SourceURL: c.URL, SourceVersion: "last_updated=" + feed.LastUpdated}}
	hasOCI := false
	for _, r := range feed.Regions {
		for _, cidr := range r.CIDRs {
			for _, tag := range cidr.Tags {
				if tag == "OCI" {
					hasOCI = true
				}
				b.add(cidr.CIDR, tag, r.Region)
			}
		}
	}
	if !hasOCI {
		return Result{}, shapeErr("oracle: no range carries the OCI tag")
	}
	return finish(b.res)
}
