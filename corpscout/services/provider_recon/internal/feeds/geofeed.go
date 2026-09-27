package feeds

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"strings"
)

// Geofeed collects an RFC 8805 self-published geofeed (DigitalOcean, Linode…).
type Geofeed struct{}

// NewGeofeed returns the generic geofeed collector; the URL is a param.
func NewGeofeed() *Geofeed { return &Geofeed{} }

// Name implements Collector.
func (*Geofeed) Name() string { return "geofeed" }

// ValidateParams requires exactly one https url.
func (*Geofeed) ValidateParams(params map[string]string) error {
	if len(params) != 1 || !strings.HasPrefix(params["url"], "https://") {
		return errors.New("geofeed takes exactly one param: url (https)")
	}
	return nil
}

// Collect implements Collector.
func (*Geofeed) Collect(ctx context.Context, f *Fetcher, params map[string]string) (Result, error) {
	url := params["url"]
	resp, err := f.Get(ctx, url, "text/csv")
	if err != nil {
		return Result{}, err
	}
	r := csv.NewReader(bytes.NewReader(resp.Body))
	r.FieldsPerRecord = -1
	r.Comment = '#'
	r.LazyQuotes = true
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return Result{}, fmt.Errorf("geofeed: parse %s: %w", url, err)
	}
	b := rangeBuilder{res: Result{SourceURL: url, SourceVersion: bodyVersion(resp.Body)}}
	for _, rec := range records {
		if len(rec) == 0 || strings.TrimSpace(rec[0]) == "" {
			continue
		}
		region := ""
		if len(rec) > 2 && strings.TrimSpace(rec[2]) != "" {
			region = strings.TrimSpace(rec[2])
		} else if len(rec) > 1 {
			region = strings.TrimSpace(rec[1])
		}
		b.add(rec[0], "GEOFEED", region)
	}
	return finish(b.res)
}
