package rawdownload

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"cc-raw/rawstore"
)

func TestDownloadProgressReportsRatesAndCounts(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	downloader := &Downloader{
		Logger: logger,
		Config: Config{
			CrawlID:          "CC-MAIN-2026-25",
			Selection:        "pages25",
			Part:             7,
			Concurrency:      64,
			ProgressInterval: 0,
		},
	}
	cooldown := &throttleCooldown{}
	progress := newDownloadProgress(downloader, 100, cooldown)
	stop := progress.start(context.Background())
	progress.recordStarted()
	progress.recordAttempted(1)
	progress.recordFinished(recordDownload{status: rawstore.Downloaded, raw: []byte("record")})
	stop()

	logLine := output.String()
	for _, field := range []string{
		`"msg":"download progress"`,
		`"requested_records":100`,
		`"completed_records":1`,
		`"downloaded_records":1`,
		`"configured_concurrency":64`,
		`"records_per_second":`,
		`"source_mib_per_second":`,
		`"record_attempts":1`,
	} {
		if !strings.Contains(logLine, field) {
			t.Fatalf("progress log %q does not contain %q", logLine, field)
		}
	}
}

func TestThrottleCooldownPausesNewAttempts(t *testing.T) {
	cooldown := &throttleCooldown{}
	if delay := cooldown.slowDown(1); delay != time.Second {
		t.Fatalf("first throttle delay=%s, want 1s", delay)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := cooldown.wait(ctx); err == nil {
		t.Fatal("throttle cooldown did not pause a new attempt")
	}
	if remaining := cooldown.remaining(); remaining <= 0 {
		t.Fatalf("cooldown remaining=%s, want positive", remaining)
	}
}

func TestThrottleCooldownBackoffIsCapped(t *testing.T) {
	cooldown := &throttleCooldown{}
	if delay := cooldown.slowDown(10); delay != 16*time.Second {
		t.Fatalf("capped throttle delay=%s, want 16s", delay)
	}
}
