package feeds

import (
	"context"
	"encoding/json"
	"fmt"
)

// Bunny collects edge server addresses (bare IPs → /32, /128). The API
// answers XML unless asked for JSON.
type Bunny struct {
	noParams
	URLv4 string
	URLv6 string
}

// NewBunny returns the collector for the official lists.
func NewBunny() *Bunny {
	return &Bunny{
		URLv4: "https://bunnycdn.com/api/system/edgeserverlist",
		URLv6: "https://bunnycdn.com/api/system/edgeserverlist/ipv6",
	}
}

// Name implements Collector.
func (*Bunny) Name() string { return "bunny_edge_servers" }

// Format implements Collector.
func (*Bunny) Format() string { return "JSON API" }

// Collect implements Collector.
func (c *Bunny) Collect(ctx context.Context, f *Fetcher, _ map[string]string) (Result, error) {
	b := rangeBuilder{res: Result{SourceURL: c.URLv4}}
	var bodies []byte
	for _, url := range []string{c.URLv4, c.URLv6} {
		resp, err := f.Get(ctx, url, "application/json")
		if err != nil {
			return Result{}, err
		}
		var ips []string
		if err := json.Unmarshal(resp.Body, &ips); err != nil {
			return Result{}, fmt.Errorf("bunny: %s is not a JSON list: %w", url, err)
		}
		if len(ips) == 0 {
			return Result{}, shapeErr("bunny: %s returned an empty list", url)
		}
		for _, ip := range ips {
			b.add(ip, "EDGE", "")
		}
		bodies = append(bodies, resp.Body...)
	}
	b.res.SourceVersion = bodyVersion(bodies)
	return finish(b.res)
}
