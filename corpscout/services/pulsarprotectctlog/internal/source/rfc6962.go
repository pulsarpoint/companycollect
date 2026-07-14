package source

import (
	"context"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/ctclient"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/parse"
)

// RFC6962 is a Source backed by a classic RFC 6962 log via get-entries. It is
// the fallback for logs not yet covered by tiled logs.
type RFC6962 struct {
	client    *ctclient.Client
	batchSize int64
}

// NewRFC6962 wraps a ctclient.Client as a Source. batchSize is the get-entries
// request size (the log may return fewer).
func NewRFC6962(client *ctclient.Client, batchSize int) *RFC6962 {
	return &RFC6962{client: client, batchSize: int64(batchSize)}
}

// Name returns the log's friendly name.
func (r *RFC6962) Name() string { return r.client.Name() }

// TreeSize returns the current tree size from get-sth.
func (r *RFC6962) TreeSize(ctx context.Context) (uint64, error) {
	return r.client.TreeSize(ctx)
}

// FetchRange fetches one get-entries batch beginning at start and parses it.
func (r *RFC6962) FetchRange(ctx context.Context, start, end int64) ([]model.CertMeta, int64, int, error) {
	batchEnd := start + r.batchSize - 1
	if batchEnd >= end {
		batchEnd = end - 1
	}
	entries, err := r.client.GetRawEntries(ctx, start, batchEnd)
	if err != nil {
		return nil, start, 0, err
	}
	metas := make([]model.CertMeta, 0, len(entries))
	parseErrors := 0
	for k := range entries {
		idx := start + int64(k)
		meta, perr := parse.Entry(idx, &entries[k], r.client.Name())
		if perr != nil {
			parseErrors++
			continue
		}
		metas = append(metas, meta)
	}
	return metas, start + int64(len(entries)), parseErrors, nil
}
