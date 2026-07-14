package loglist

import (
	"context"
	"net/http"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/ctclient"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/tileclient"
)

// CTLogStatus extends CTLog with runtime status fields populated by the list
// command: reachability, current head, processing cursor, and derived metrics.
type CTLogStatus struct {
	CTLog
	Phase        string  `json:"phase"`
	Reachable    bool    `json:"reachable"`
	Head         int64   `json:"head"`
	Tracked      bool    `json:"tracked"`
	Status       string  `json:"status"`
	Cursor       int64   `json:"cursor"`
	CertsWritten int64   `json:"certs_written"`
	SANsWritten  int64   `json:"sans_written"`
	PercentDone  float64 `json:"percent_done"`
}

// PercentDone returns what fraction of the log (0–100) has been processed.
// Returns 0 when head <= 0 to avoid division by zero.
func PercentDone(cursor, head int64) float64 {
	if head <= 0 {
		return 0
	}
	return float64(cursor) / float64(head) * 100
}

// Head probes c's current entry count and reachability. It uses the tiled
// client for "tiled" logs and the RFC 6962 client for all other types.
// Returns (0, false) on any error.
func Head(ctx context.Context, hc *http.Client, c CTLog, retries int) (uint64, bool) {
	if c.Type == "rfc6962" {
		cl, err := ctclient.New(c.MonitoringURL, c.Description, hc, retries)
		if err != nil {
			return 0, false
		}
		n, err := cl.TreeSize(ctx)
		return n, err == nil
	}
	n, err := tileclient.New(c.MonitoringURL, c.Description, hc, retries).TreeSize(ctx)
	return n, err == nil
}
