// Package loglist enumerates CT log shards from the published Chrome log list,
// supporting both tiled logs (operators[].tiled_logs[]) and RFC 6962 logs
// (operators[].logs[]).
package loglist

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/certificate-transparency-go/loglist3"
)

// Phase describes a log's lifecycle relative to a reference time.
type Phase string

const (
	// PhaseFrozen means the expiry window has fully passed: the log is sealed,
	// immutable, and complete. Drain once, never poll again.
	PhaseFrozen Phase = "frozen"
	// PhaseActive means the log is still within (or before the end of) its
	// expiry window and accepting submissions: it grows and must be tailed.
	PhaseActive Phase = "active"
)

// CTLog is a single CT log shard with full metadata.
type CTLog struct {
	ID            string    `json:"id"` // friendly id (set by caller via idFn)
	Description   string    `json:"description"`
	LogID         string    `json:"log_id"` // canonical base64 SHA-256(pubkey)
	Type          string    `json:"type"`   // tiled | rfc6962
	Source        string    `json:"source"` // source name
	MonitoringURL string    `json:"monitoring_url"`
	SubmissionURL string    `json:"submission_url"`
	URL           string    `json:"url"`
	State         string    `json:"state"`
	MMD           int       `json:"mmd"`
	Start         time.Time `json:"start"` // temporal_interval.start_inclusive (by cert expiry)
	End           time.Time `json:"end"`   // temporal_interval.end_exclusive
}

// Phase returns the log's lifecycle phase at time now. A log is frozen once
// now is at or past its end_exclusive (every cert in it has expired).
func (c CTLog) Phase(now time.Time) Phase {
	if !now.Before(c.End) {
		return PhaseFrozen
	}
	return PhaseActive
}

// fetchList fetches and parses the v3 log list from listURL using
// certificate-transparency-go/loglist3, the canonical parser for the schema.
func fetchList(ctx context.Context, hc *http.Client, listURL string) (*loglist3.LogList, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, listURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build log list request: %w", err)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch log list: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch log list: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read log list: %w", err)
	}
	ll, err := loglist3.NewFromJSON(body)
	if err != nil {
		return nil, fmt.Errorf("parse log list: %w", err)
	}
	return ll, nil
}

// CTLogs fetches listURL and returns the CT logs matching the given source.
// For tiled sources it reads tiled_logs[]; for rfc6962 it reads logs[] and
// sets MonitoringURL to the log's url field. Both operator and log_prefix
// filters from s are applied (empty matches all). The caller-supplied idFn
// computes a friendly ID for each log. Results are sorted by start time.
func CTLogs(ctx context.Context, hc *http.Client, listURL string, s Source, idFn func(CTLog) string) ([]CTLog, error) {
	ll, err := fetchList(ctx, hc, listURL)
	if err != nil {
		return nil, err
	}

	var out []CTLog
	for _, op := range ll.Operators {
		if s.Operator != "" && op.Name != s.Operator {
			continue
		}

		switch s.Type {
		case "tiled":
			for _, l := range op.TiledLogs {
				if s.LogPrefix != "" && !strings.Contains(l.Description, s.LogPrefix) {
					continue
				}
				start, end := window(l.TemporalInterval)
				c := CTLog{
					Description:   l.Description,
					LogID:         base64.StdEncoding.EncodeToString(l.LogID),
					Type:          s.Type,
					Source:        s.Name,
					MonitoringURL: l.MonitoringURL,
					SubmissionURL: l.SubmissionURL,
					State:         stateName(l.State),
					MMD:           int(l.MMD),
					Start:         start,
					End:           end,
				}
				c.ID = idFn(c)
				out = append(out, c)
			}
		case "rfc6962":
			for _, l := range op.Logs {
				if s.LogPrefix != "" && !strings.Contains(l.Description, s.LogPrefix) {
					continue
				}
				start, end := window(l.TemporalInterval)
				c := CTLog{
					Description:   l.Description,
					LogID:         base64.StdEncoding.EncodeToString(l.LogID),
					Type:          s.Type,
					Source:        s.Name,
					MonitoringURL: l.URL,
					URL:           l.URL,
					State:         "rfc6962",
					MMD:           int(l.MMD),
					Start:         start,
					End:           end,
				}
				c.ID = idFn(c)
				out = append(out, c)
			}
		}
	}

	sortByStart(out)
	return out, nil
}

// window returns a temporal interval's start/end, or zero times when absent
// (loglist3 models temporal_interval as an optional pointer).
func window(ti *loglist3.TemporalInterval) (start, end time.Time) {
	if ti == nil {
		return time.Time{}, time.Time{}
	}
	return ti.StartInclusive, ti.EndExclusive
}

// stateName returns the lowercase log-list state name (e.g. "usable"), or
// "(none)" when no state is set, preserving the prior tiled-state string.
func stateName(ls *loglist3.LogStates) string {
	if ls == nil {
		return "(none)"
	}
	switch {
	case ls.Pending != nil:
		return "pending"
	case ls.Qualified != nil:
		return "qualified"
	case ls.Usable != nil:
		return "usable"
	case ls.ReadOnly != nil:
		return "readonly"
	case ls.Retired != nil:
		return "retired"
	case ls.Rejected != nil:
		return "rejected"
	default:
		return "(none)"
	}
}

func sortByStart(s []CTLog) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].Start.Before(s[j-1].Start); j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
