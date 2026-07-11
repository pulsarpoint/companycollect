package warcsize

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

type Cache map[string]int64

type Summary struct {
	Objects    int
	Bytes      int64
	CacheHits  int
	HTTPChecks int
}

func LoadCache(path string) (Cache, error) {
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return make(Cache), nil
	}
	if err != nil {
		return nil, errors.Wrap(err, "read WARC size cache")
	}
	cache := make(Cache)
	if err := json.Unmarshal(body, &cache); err != nil {
		return nil, errors.Wrap(err, "decode WARC size cache")
	}
	return cache, nil
}

func SaveCache(path string, cache Cache) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return errors.Wrap(err, "create WARC size cache directory")
	}
	body, err := json.Marshal(cache)
	if err != nil {
		return errors.Wrap(err, "encode WARC size cache")
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".warc-sizes-*.json")
	if err != nil {
		return errors.Wrap(err, "create temporary WARC size cache")
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if _, err := temporary.Write(body); err != nil {
		_ = temporary.Close()
		return errors.Wrap(err, "write temporary WARC size cache")
	}
	if err := temporary.Close(); err != nil {
		return errors.Wrap(err, "close temporary WARC size cache")
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return errors.Wrap(err, "commit WARC size cache")
	}
	return nil
}

func Measure(ctx context.Context, files []string, baseURL string, concurrency int, cache Cache) (Summary, error) {
	if concurrency <= 0 {
		return Summary{}, errors.New("WARC size concurrency must be positive")
	}
	if cache == nil {
		return Summary{}, errors.New("WARC size cache is required")
	}
	baseURL = strings.TrimRight(baseURL, "/") + "/"
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency * 2,
			MaxIdleConnsPerHost: concurrency,
			MaxConnsPerHost:     concurrency,
		},
	}

	var mutex sync.Mutex
	var group errgroup.Group
	group.SetLimit(concurrency)
	cacheHits := 0
	for _, filename := range files {
		filename := filename
		if cache[filename] > 0 {
			cacheHits++
			continue
		}
		group.Go(func() error {
			size, err := fetchObjectSize(ctx, client, baseURL+filename)
			if err != nil {
				return errors.Wrapf(err, "measure WARC %s", filename)
			}
			mutex.Lock()
			cache[filename] = size
			mutex.Unlock()
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return Summary{}, err
	}

	summary := Summary{Objects: len(files), CacheHits: cacheHits, HTTPChecks: len(files) - cacheHits}
	for _, filename := range files {
		summary.Bytes += cache[filename]
	}
	return summary, nil
}

func fetchObjectSize(ctx context.Context, client *http.Client, url string) (int64, error) {
	var lastErr error
	for attempt := 1; attempt <= 3; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return 0, errors.Wrap(err, "create WARC HEAD request")
		}
		response, err := client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 && response.ContentLength > 0 {
				return response.ContentLength, nil
			}
			lastErr = errors.Newf("HEAD returned HTTP %d with content length %d", response.StatusCode, response.ContentLength)
		} else {
			lastErr = err
		}

		request, err = http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return 0, errors.Wrap(err, "create WARC size range request")
		}
		request.Header.Set("Range", "bytes=0-0")
		response, err = client.Do(request)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusPartialContent {
				if size, parseErr := contentRangeSize(response.Header.Get("Content-Range")); parseErr == nil {
					return size, nil
				} else {
					lastErr = parseErr
				}
			} else {
				lastErr = errors.Newf("size range returned HTTP %d", response.StatusCode)
			}
		} else {
			lastErr = err
		}
		if attempt < 3 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(time.Duration(attempt) * 250 * time.Millisecond):
			}
		}
	}
	return 0, errors.Wrap(lastErr, "determine WARC object size")
}

func contentRangeSize(value string) (int64, error) {
	_, sizeText, found := strings.Cut(value, "/")
	if !found || sizeText == "" || sizeText == "*" {
		return 0, errors.Newf("invalid Content-Range %q", value)
	}
	size, err := strconv.ParseInt(sizeText, 10, 64)
	if err != nil || size <= 0 {
		return 0, errors.Newf("invalid Content-Range size %q", sizeText)
	}
	return size, nil
}
