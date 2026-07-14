// Package ingest orchestrates a single work unit: it reads a contiguous index
// range from a CT log Source, applies the retention rule, and writes metadata +
// SAN rows to ClickHouse, persisting a resumable cursor.
package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/retention"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/source"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/store/clickhouse"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/store/control"
)

// Ingester runs work units against one CT log Source.
type Ingester struct {
	src        source.Source
	ch         *clickhouse.Store
	ctrl       *control.Store
	writeBatch int
	limit      int  // max entries to process (0 = unlimited); for smoke tests
	finalize   bool // mark the work unit done when the range completes
	now        func() time.Time
}

// New constructs an Ingester. writeBatch is the row threshold that triggers a
// ClickHouse flush.
func New(src source.Source, ch *clickhouse.Store, ctrl *control.Store, writeBatch int) *Ingester {
	return &Ingester{
		src:        src,
		ch:         ch,
		ctrl:       ctrl,
		writeBatch: writeBatch,
		finalize:   true,
		now:        control.Now,
	}
}

// WithFinalize controls whether a completed range marks the work unit done.
// Set false for the active-shard tail loop, which re-runs against a growing
// head and must keep the work unit resumable.
func (i *Ingester) WithFinalize(v bool) *Ingester {
	i.finalize = v
	return i
}

// WithLimit caps the number of entries processed (0 = unlimited). Intended for
// smoke tests against large windows.
func (i *Ingester) WithLimit(n int) *Ingester {
	i.limit = n
	return i
}

// dry reports whether the Ingester runs without persistence (no ClickHouse or
// control store). Used to validate fetching, parsing, and classification.
func (i *Ingester) dry() bool { return i.ch == nil }

// Stats summarizes a completed work unit run.
type Stats struct {
	EntriesProcessed int
	CertsWritten     int
	SANsWritten      int
	ParseErrors      int
}

// Run executes the work unit, resuming from the persisted cursor if present.
// It is idempotent: ClickHouse ReplacingMergeTree collapses any rows re-written
// after a mid-unit restart.
func (i *Ingester) Run(ctx context.Context, w model.WorkUnit) (Stats, error) {
	var stats Stats

	cursor := int64(w.StartIndex)
	if !i.dry() {
		resume, status, err := i.ctrl.EnsureWorkUnit(ctx, w)
		if err != nil {
			return stats, err
		}
		if status == control.StatusDone {
			slog.Info("work unit already complete", "id", w.ID)
			return stats, nil
		}
		if err := i.ctrl.MarkRunning(ctx, w.ID); err != nil {
			return stats, err
		}
		cursor = resume
	}
	end := int64(w.EndIndex)
	certBuf := make([]model.CertMeta, 0, i.writeBatch)
	hostBuf := make([]model.HostnameRow, 0, i.writeBatch)

	flush := func() error {
		if !i.dry() {
			if err := i.ch.WriteCerts(ctx, certBuf); err != nil {
				return err
			}
			if err := i.ch.WriteHostnames(ctx, hostBuf); err != nil {
				return err
			}
			if err := i.ctrl.SaveProgress(ctx, w.ID, cursor, len(certBuf), len(hostBuf)); err != nil {
				return err
			}
		}
		stats.CertsWritten += len(certBuf)
		stats.SANsWritten += len(hostBuf)
		certBuf = certBuf[:0]
		hostBuf = hostBuf[:0]
		return nil
	}

	markFailed := func(msg string) {
		if !i.dry() {
			_ = i.ctrl.MarkFailed(ctx, w.ID, msg)
		}
	}

	for cursor < end {
		metas, next, parseErrors, err := i.src.FetchRange(ctx, cursor, end)
		if err != nil {
			// Graceful stop only when OUR context is done (SIGINT/SIGTERM).
			// Matching the error class alone is wrong: an HTTP client timeout
			// also satisfies errors.Is(err, context.DeadlineExceeded) and must
			// surface as a failure, not silently end the drain mid-shard.
			if ctx.Err() != nil {
				slog.Info("ingest cancelled; stopping", "id", w.ID, "cursor", cursor)
				return stats, nil
			}
			markFailed(err.Error())
			return stats, fmt.Errorf("fetch from %d: %w", cursor, err)
		}
		if next <= cursor {
			markFailed("no progress")
			return stats, fmt.Errorf("source made no progress at index %d", cursor)
		}

		now := i.now()
		for _, meta := range metas {
			hostBuf = append(hostBuf, retention.HostnameRows(meta)...)
			if retention.KeepMetadata(meta, now) {
				certBuf = append(certBuf, meta)
			}
		}
		stats.EntriesProcessed += len(metas)
		stats.ParseErrors += parseErrors
		cursor = next

		if i.limit > 0 && stats.EntriesProcessed >= i.limit {
			if err := flush(); err != nil {
				markFailed(err.Error())
				return stats, err
			}
			slog.Info("entry limit reached, stopping", "limit", i.limit)
			return stats, nil
		}

		if len(certBuf) >= i.writeBatch || len(hostBuf) >= i.writeBatch {
			if err := flush(); err != nil {
				markFailed(err.Error())
				return stats, err
			}
		}
	}

	if err := flush(); err != nil {
		markFailed(err.Error())
		return stats, err
	}
	if !i.dry() && i.finalize {
		if err := i.ctrl.MarkDone(ctx, w.ID); err != nil {
			return stats, err
		}
	}
	slog.Info("work unit complete", "id", w.ID,
		"entries", stats.EntriesProcessed, "certs", stats.CertsWritten,
		"sans", stats.SANsWritten, "parse_errors", stats.ParseErrors)
	return stats, nil
}
