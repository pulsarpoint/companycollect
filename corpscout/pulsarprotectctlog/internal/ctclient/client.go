// Package ctclient is a thin RFC 6962 Certificate Transparency log client. It
// fetches the signed tree head (get-sth) and raw log entries (get-entries) with
// exponential-backoff retry, wrapping google/certificate-transparency-go. It is
// the fetch backend for RFC 6962 (non-tiled) logs; tiled logs use tileclient.
package ctclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	ct "github.com/google/certificate-transparency-go"
	"github.com/google/certificate-transparency-go/client"
	"github.com/google/certificate-transparency-go/jsonclient"
)

// Client fetches signed tree heads and raw entries from a single CT log shard.
type Client struct {
	log        *client.LogClient
	name       string
	maxRetries int
}

// New constructs a Client for the given log base URL.
func New(logURL, name string, hc *http.Client, maxRetries int) (*Client, error) {
	lc, err := client.New(logURL, hc, jsonclient.Options{UserAgent: "pulsarprotect-ctlog/0.1"})
	if err != nil {
		return nil, fmt.Errorf("create log client: %w", err)
	}
	return &Client{log: lc, name: name, maxRetries: maxRetries}, nil
}

// Name returns the log's friendly name.
func (c *Client) Name() string { return c.name }

// TreeSize returns the current number of entries in the log (from get-sth).
func (c *Client) TreeSize(ctx context.Context) (uint64, error) {
	sth, err := c.withRetry(ctx, func() (any, error) { return c.log.GetSTH(ctx) })
	if err != nil {
		return 0, fmt.Errorf("get-sth: %w", err)
	}
	return sth.(*ct.SignedTreeHead).TreeSize, nil
}

// GetRawEntries fetches raw log entries in [start, end] (inclusive, as the CT
// API defines it). Logs may return fewer entries than requested; callers must
// advance by the number actually returned.
func (c *Client) GetRawEntries(ctx context.Context, start, end int64) ([]ct.LeafEntry, error) {
	resp, err := c.withRetry(ctx, func() (any, error) { return c.log.GetRawEntries(ctx, start, end) })
	if err != nil {
		return nil, fmt.Errorf("get-entries [%d,%d]: %w", start, end, err)
	}
	return resp.(*ct.GetEntriesResponse).Entries, nil
}

// withRetry runs fn with exponential backoff on transient errors.
func (c *Client) withRetry(ctx context.Context, fn func() (any, error)) (any, error) {
	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<(attempt-1)) * time.Second
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		v, err := fn()
		if err == nil {
			return v, nil
		}
		lastErr = err
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
	}
	return nil, fmt.Errorf("exhausted %d retries: %w", c.maxRetries, lastErr)
}
