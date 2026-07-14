package source

import (
	"context"
	"log/slog"
	"sync"

	"github.com/pulsarpoint/pulsarprotectctlog/internal/model"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/parse"
	"github.com/pulsarpoint/pulsarprotectctlog/internal/tileclient"
)

// Tile is a Source backed by a static-ct-api (tiled) log. Each FetchRange call
// fetches up to `parallel` data tiles concurrently and returns their entries in
// index order. Tiles are fetched without caching to bound memory.
type Tile struct {
	client   *tileclient.Client
	parallel int

	mu       sync.Mutex
	treeSize uint64
}

// NewTile wraps a tileclient.Client as a Source. parallel is the number of data
// tiles fetched concurrently per FetchRange (clamped to >= 1).
func NewTile(client *tileclient.Client, parallel int) *Tile {
	if parallel < 1 {
		parallel = 1
	}
	return &Tile{client: client, parallel: parallel}
}

// Name returns the log's friendly name.
func (t *Tile) Name() string { return t.client.Name() }

// TreeSize fetches and caches the checkpoint tree size.
func (t *Tile) TreeSize(ctx context.Context) (uint64, error) {
	size, err := t.client.TreeSize(ctx)
	if err != nil {
		return 0, err
	}
	t.mu.Lock()
	t.treeSize = size
	t.mu.Unlock()
	return size, nil
}

// tileResult holds the parsed output of one concurrently-fetched data tile.
type tileResult struct {
	metas       []model.CertMeta
	parseErrors int
	fetchErr    error // tile could not be fetched (transient) -> abort
	parseErr    error // tile framing broke partway -> log + keep partial + continue
}

// FetchRange fetches up to `parallel` consecutive data tiles starting at the
// tile containing `start`, in parallel, and returns their entries within
// [start, end) in index order. next is the first index after the last fetched
// tile, clamped to end. Tiles are fetched without caching to bound memory.
func (t *Tile) FetchRange(ctx context.Context, start, end int64) ([]model.CertMeta, int64, int, error) {
	treeSize, err := t.ensureTreeSize(ctx)
	if err != nil {
		return nil, start, 0, err
	}

	firstTile := uint64(start) / tileclient.EntriesPerTile
	lastTileNeeded := uint64(end-1) / tileclient.EntriesPerTile
	n := uint64(t.parallel)
	if avail := lastTileNeeded - firstTile + 1; n > avail {
		n = avail
	}

	results := make([]tileResult, n)
	var wg sync.WaitGroup
	for i := uint64(0); i < n; i++ {
		wg.Add(1)
		go func(i uint64) {
			defer wg.Done()
			tn := firstTile + i
			tile, err := t.client.DataTile(ctx, tn, tileclient.TileWidth(tn, treeSize))
			if err != nil {
				results[i] = tileResult{fetchErr: err}
				return
			}
			metas, perr, derr := parse.DataTile(tile, int64(tn*tileclient.EntriesPerTile), t.client.Name())
			results[i] = tileResult{metas: metas, parseErrors: perr, parseErr: derr}
		}(i)
	}
	wg.Wait()

	var all []model.CertMeta
	var parseErrors int
	for i := uint64(0); i < n; i++ {
		if results[i].fetchErr != nil {
			return nil, start, parseErrors, results[i].fetchErr
		}
		all = append(all, results[i].metas...)
		parseErrors += results[i].parseErrors
		if results[i].parseErr != nil {
			slog.Warn("tile framing error; remainder of tile skipped",
				"tile", firstTile+i, "error", results[i].parseErr)
			parseErrors++
		}
	}

	next := min(int64((firstTile+n)*tileclient.EntriesPerTile), end)
	// Keep only entries within [start, end).
	out := all[:0:0]
	for _, m := range all {
		idx := int64(m.LogIndex)
		if idx >= start && idx < end {
			out = append(out, m)
		}
	}
	return out, next, parseErrors, nil
}

func (t *Tile) ensureTreeSize(ctx context.Context) (uint64, error) {
	t.mu.Lock()
	size := t.treeSize
	t.mu.Unlock()
	if size != 0 {
		return size, nil
	}
	return t.TreeSize(ctx)
}
