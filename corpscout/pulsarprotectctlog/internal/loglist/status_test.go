package loglist

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPercentDone(t *testing.T) {
	tests := []struct {
		cursor int64
		head   int64
		want   float64
	}{
		{cursor: 0, head: 100, want: 0},
		{cursor: 50, head: 100, want: 50},
		{cursor: 100, head: 100, want: 100},
		{cursor: 0, head: 0, want: 0},
	}
	for _, tc := range tests {
		got := PercentDone(tc.cursor, tc.head)
		if got != tc.want {
			t.Errorf("PercentDone(%d, %d) = %v, want %v", tc.cursor, tc.head, got, tc.want)
		}
	}
}

// TestCTLogStatusJSONKeys verifies that CTLog fields embedded in CTLogStatus
// are marshaled with snake_case JSON keys, not Go field names.
func TestCTLogStatusJSONKeys(t *testing.T) {
	s := CTLogStatus{
		CTLog: CTLog{
			ID:            "argon2024h1",
			Description:   "Argon 2024 H1",
			LogID:         "abc123==",
			Type:          "tiled",
			Source:        "google",
			MonitoringURL: "https://ct.googleapis.com/logs/argon2024h1/",
			SubmissionURL: "https://ct.googleapis.com/logs/argon2024h1/",
			URL:           "https://ct.googleapis.com/logs/argon2024h1/",
			State:         "active",
			MMD:           86400,
			Start:         time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			End:           time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		Phase:        "active",
		Reachable:    true,
		Head:         1000000,
		Tracked:      true,
		Status:       "ok",
		Cursor:       500000,
		CertsWritten: 499000,
		SANsWritten:  750000,
		PercentDone:  50.0,
	}

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("json.Marshal CTLogStatus: %v", err)
	}
	out := string(b)

	// Keys that MUST appear (snake_case).
	wantKeys := []string{
		`"id"`,
		`"log_id"`,
		`"monitoring_url"`,
		`"mmd"`,
		`"state"`,
		`"percent_done"`,
	}
	for _, key := range wantKeys {
		if !strings.Contains(out, key) {
			t.Errorf("marshaled JSON missing expected key %s\nJSON: %s", key, out)
		}
	}

	// Keys that must NOT appear (Go-cased field names).
	bannedKeys := []string{
		`"MonitoringURL"`,
		`"LogID"`,
		`"MMD"`,
		`"SubmissionURL"`,
	}
	for _, key := range bannedKeys {
		if strings.Contains(out, key) {
			t.Errorf("marshaled JSON contains Go-cased key %s (json tags missing?)\nJSON: %s", key, out)
		}
	}
}
