package feeds

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// GitHub collects /meta. Only keys whose value is a list containing at least
// one CIDR become tags; ssh_keys, domains and flags are ignored.
type GitHub struct {
	noParams
	URL string
}

// NewGitHub returns the collector for the official endpoint.
func NewGitHub() *GitHub { return &GitHub{URL: "https://api.github.com/meta"} }

// Name implements Collector.
func (*GitHub) Name() string { return "github_meta" }

// Collect implements Collector.
func (c *GitHub) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	resp, err := f.Get(ctx, c.URL, "application/vnd.github+json")
	if err != nil {
		return Result{}, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(resp.Body, &raw); err != nil {
		return Result{}, fmt.Errorf("github: decode: %w", err)
	}
	version := bodyVersion(resp.Body)
	if resp.ETag != "" {
		version = "etag=" + resp.ETag
	}
	res := Result{SourceURL: c.URL, SourceVersion: version}
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		var list []string
		if json.Unmarshal(raw[key], &list) != nil {
			continue
		}
		var ranges []Range
		bad := 0
		for _, s := range list {
			if p, ok := parsePrefix(s); ok {
				ranges = append(ranges, Range{Prefix: p, Tag: key})
			} else {
				bad++
			}
		}
		if len(ranges) == 0 {
			continue // not a CIDR list (ssh_keys and friends)
		}
		res.Ranges = append(res.Ranges, ranges...)
		res.Skipped += bad
	}
	return finish(res)
}
