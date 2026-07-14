// Package source abstracts a CT log entry source behind a single interface so
// the ingester can read from either a static-ct (tiled) log or an RFC 6962 log.
package source

import (
	"context"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
)

// Source is a readable CT log shard. Indices are absolute log positions.
type Source interface {
	// Name returns the log's friendly name.
	Name() string
	// TreeSize returns the current number of entries (from STH/checkpoint).
	TreeSize(ctx context.Context) (uint64, error)
	// FetchRange parses a contiguous chunk beginning at start and not crossing
	// end, returning the parsed metadata, the next index to resume from, and a
	// per-chunk parse-error count. Implementations choose the chunk size (one
	// data tile for tiled logs, one get-entries batch for RFC 6962).
	FetchRange(ctx context.Context, start, end int64) (metas []model.CertMeta, next int64, parseErrors int, err error)
}
