package feeds

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var asnRE = regexp.MustCompile(`^[0-9]+$`)

// RIPEstat collects the prefixes an ASN currently announces. Used for
// providers without an official range feed (Hetzner, OVH, Akamai…); the
// ranges are provider-level evidence with source "bgp".
type RIPEstat struct {
	URLFormat string
}

// NewRIPEstat returns the collector for the public RIPEstat API.
func NewRIPEstat() *RIPEstat {
	return &RIPEstat{URLFormat: "https://stat.ripe.net/data/announced-prefixes/data.json?resource=AS%s&sourceapp=corpscout-provider-recon"}
}

// Name implements Collector.
func (*RIPEstat) Name() string { return "ripestat_announced" }

// ValidateParams requires exactly one numeric asn.
func (*RIPEstat) ValidateParams(params map[string]string) error {
	if len(params) != 1 || !asnRE.MatchString(params["asn"]) {
		return errors.New(`ripestat_announced takes exactly one param: asn (digits, no "AS" prefix)`)
	}
	return nil
}

// Collect implements Collector.
func (c *RIPEstat) Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error) {
	url := fmt.Sprintf(c.URLFormat, params["asn"])
	resp, err := f.Get(ctx, url, "application/json")
	if err != nil {
		return Result{}, err
	}
	var feed struct {
		Status string `json:"status"`
		Data   struct {
			Prefixes []struct {
				Prefix string `json:"prefix"`
			} `json:"prefixes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(resp.Body, &feed); err != nil {
		return Result{}, fmt.Errorf("ripestat: decode: %w", err)
	}
	if feed.Status != "ok" {
		return Result{}, fmt.Errorf("ripestat: status %q for AS%s", feed.Status, params["asn"])
	}
	prefixes := make([]string, 0, len(feed.Data.Prefixes))
	b := rangeBuilder{res: Result{SourceURL: url}}
	for _, p := range feed.Data.Prefixes {
		b.add(p.Prefix, "BGP", "")
		prefixes = append(prefixes, p.Prefix)
	}
	// The response embeds query timestamps; version on the prefix set only.
	sort.Strings(prefixes)
	b.res.SourceVersion = bodyVersion([]byte(strings.Join(prefixes, "\n")))
	return finish(b.res)
}
