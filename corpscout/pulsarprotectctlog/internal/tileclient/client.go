// Package tileclient fetches checkpoints and data tiles from a static-ct-api
// (tiled) CT log, per c2sp.org/static-ct-api and c2sp.org/tlog-tiles. It is a
// pure transport layer; leaf parsing lives in the parse package.
package tileclient

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// EntriesPerTile is the fixed width of a full level-0 data tile.
const EntriesPerTile = 256

// Client reads a single static-ct log shard via its monitoring prefix.
type Client struct {
	base       string // monitoring prefix, no trailing slash
	name       string
	http       *http.Client
	maxRetries int
}

// New constructs a tile Client for the given monitoring prefix base URL.
func New(base, name string, hc *http.Client, maxRetries int) *Client {
	return &Client{
		base:       strings.TrimRight(base, "/"),
		name:       name,
		http:       hc,
		maxRetries: maxRetries,
	}
}

// Name returns the log's friendly name.
func (c *Client) Name() string { return c.name }

// TreeSize fetches the current tree size from the checkpoint.
func (c *Client) TreeSize(ctx context.Context) (uint64, error) {
	body, err := c.get(ctx, c.base+"/checkpoint")
	if err != nil {
		return 0, fmt.Errorf("fetch checkpoint: %w", err)
	}
	return parseCheckpointSize(body)
}

// DataTile fetches the raw data tile with the given number. width is the entry
// count of the tile (EntriesPerTile for a full tile, fewer for the final
// partial tile); it selects the partial-tile path suffix.
func (c *Client) DataTile(ctx context.Context, tileNumber uint64, width int) ([]byte, error) {
	url := c.base + "/" + dataTilePath(tileNumber, width)
	body, err := c.get(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("fetch data tile %d: %w", tileNumber, err)
	}
	return body, nil
}

// TileWidth returns the entry count of the given tile relative to treeSize:
// EntriesPerTile for full tiles, the remainder for the final partial tile.
func TileWidth(tileNumber, treeSize uint64) int {
	if tileNumber < treeSize/EntriesPerTile {
		return EntriesPerTile
	}
	return int(treeSize % EntriesPerTile)
}

// get performs an HTTP GET with exponential-backoff retry on transient errors.
func (c *Client) get(ctx context.Context, url string) ([]byte, error) {
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
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build request: %w", err)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			continue
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %d", resp.StatusCode)
			continue
		}
		if readErr != nil {
			lastErr = readErr
			continue
		}
		return body, nil
	}
	return nil, fmt.Errorf("exhausted %d retries: %w", c.maxRetries, lastErr)
}

// dataTilePath encodes the path for a data tile per c2sp.org/tlog-tiles:
// least-significant 3-digit group unprefixed, higher groups "x%03d/" prefixed,
// with a ".p/<width>" suffix for a partial (non-full) tile.
func dataTilePath(n uint64, width int) string {
	const base = 1000
	s := fmt.Sprintf("%03d", n%base)
	for n >= base {
		n /= base
		s = fmt.Sprintf("x%03d/%s", n%base, s)
	}
	if width != EntriesPerTile {
		s += ".p/" + strconv.Itoa(width)
	}
	return "tile/data/" + s
}

// parseCheckpointSize extracts the tree size (second line) from a checkpoint.
func parseCheckpointSize(body []byte) (uint64, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	var lines []string
	for scanner.Scan() && len(lines) < 2 {
		lines = append(lines, scanner.Text())
	}
	if len(lines) < 2 {
		return 0, fmt.Errorf("checkpoint too short")
	}
	size, err := strconv.ParseUint(strings.TrimSpace(lines[1]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse tree size %q: %w", lines[1], err)
	}
	return size, nil
}
