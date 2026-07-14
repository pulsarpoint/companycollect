package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// errSource returns a fixed error from FetchRange.
type errSource struct{ err error }

func (errSource) Name() string                             { return "err" }
func (errSource) TreeSize(context.Context) (uint64, error) { return 100, nil }
func (s errSource) FetchRange(ctx context.Context, start, end int64) ([]model.CertMeta, int64, int, error) {
	if err := ctx.Err(); err != nil {
		return nil, start, 0, err
	}
	return nil, start, 0, s.err
}

func TestRunReturnsCleanOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()                                                    // simulate SIGINT: the run's own context is cancelled
	ing := New(errSource{err: context.Canceled}, nil, nil, 100) // dry mode (ch==nil): no control writes
	_, err := ing.Run(ctx, model.WorkUnit{ID: "x", StartIndex: 0, EndIndex: 100})
	if err != nil {
		t.Fatalf("cancel should return nil error, got %v", err)
	}
}

func TestRunReturnsErrorOnFetchTimeout(t *testing.T) {
	// An HTTP client timeout satisfies errors.Is(err, context.DeadlineExceeded)
	// even though the run's context is alive. That is a fetch failure, not a
	// shutdown: Run must report it so the shard is not silently skipped.
	timeout := fmt.Errorf("fetch tile: %w", context.DeadlineExceeded)
	ing := New(errSource{err: timeout}, nil, nil, 100)
	_, err := ing.Run(context.Background(), model.WorkUnit{ID: "x", StartIndex: 0, EndIndex: 100})
	if err == nil {
		t.Fatal("fetch timeout with live context should return an error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error should wrap the fetch failure, got %v", err)
	}
}
