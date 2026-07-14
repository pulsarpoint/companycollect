package loglist

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

const fixture = `{"operators":[
 {"name":"Let's Encrypt",
  "logs":[{"description":"LE OldRFC2025","log_id":"AAA=","url":"https://old.example/","mmd":86400,"temporal_interval":{"start_inclusive":"2025-01-01T00:00:00Z","end_exclusive":"2025-07-01T00:00:00Z"}}],
  "tiled_logs":[{"description":"Let's Encrypt 'Sycamore2025h2d'","log_id":"TgJ3oMtvarf2feceaghbLRgMKXeCS/tMK72dLNQR874=","submission_url":"https://sub.example/2025h2d/","monitoring_url":"https://mon.example/2025h2d/","mmd":60,"state":{"usable":{}},"temporal_interval":{"start_inclusive":"2025-06-19T00:00:00Z","end_exclusive":"2025-12-18T00:00:00Z"}}]},
 {"name":"Google",
  "logs":[{"description":"Google 'Xenon2025h2'","log_id":"CCC=","url":"https://ct.googleapis.com/logs/xenon2025h2/","mmd":86400,"temporal_interval":{"start_inclusive":"2025-07-01T00:00:00Z","end_exclusive":"2026-01-01T00:00:00Z"}}]}
]}`

func serve(t *testing.T) (*http.Client, string) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte(fixture)) }))
	t.Cleanup(s.Close)
	return s.Client(), s.URL
}

func id(c CTLog) string { return "ID:" + c.Description }

func TestCTLogsTiled(t *testing.T) {
	hc, url := serve(t)
	got, err := CTLogs(context.Background(), hc, url, Source{Type: "tiled", Operator: "Let's Encrypt", LogPrefix: "Sycamore"}, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("len=%d", len(got))
	}
	c := got[0]
	if c.MonitoringURL != "https://mon.example/2025h2d/" || c.LogID != "TgJ3oMtvarf2feceaghbLRgMKXeCS/tMK72dLNQR874=" || c.MMD != 60 || c.Type != "tiled" {
		t.Fatalf("ctlog=%+v", c)
	}
	if c.ID != "ID:Let's Encrypt 'Sycamore2025h2d'" {
		t.Errorf("id=%q", c.ID)
	}
}

func TestCTLogsRFC6962(t *testing.T) {
	hc, url := serve(t)
	got, err := CTLogs(context.Background(), hc, url, Source{Type: "rfc6962", Operator: "Google", LogPrefix: "Xenon2025"}, id)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].MonitoringURL != "https://ct.googleapis.com/logs/xenon2025h2/" || got[0].Type != "rfc6962" {
		t.Fatalf("ctlog=%+v", got)
	}
}
