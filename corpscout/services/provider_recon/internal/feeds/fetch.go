package feeds

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	userAgent = "corpscout-provider-recon/1"
	maxBody   = 64 << 20
)

// Response is a successful fetch.
type Response struct {
	Body []byte
	ETag string
	URL  string
}

// Fetcher performs GETs with retry on network errors, 429 and 5xx.
type Fetcher struct {
	Client  *http.Client
	Retries int
	Backoff time.Duration
}

// NewFetcher returns the production fetcher.
func NewFetcher() *Fetcher {
	return &Fetcher{Client: &http.Client{Timeout: 2 * time.Minute}, Retries: 3, Backoff: 2 * time.Second}
}

// Get fetches url; accept sets the Accept header when non-empty.
func (f *Fetcher) Get(ctx context.Context, url, accept string) (Response, error) {
	var lastErr error
	for attempt := 0; attempt <= f.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return Response{}, ctx.Err()
			case <-time.After(f.Backoff * time.Duration(attempt)):
			}
		}
		resp, retry, err := f.once(ctx, url, accept)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !retry {
			break
		}
	}
	return Response{}, fmt.Errorf("GET %s: %w", url, lastErr)
}

func (f *Fetcher) once(ctx context.Context, url, accept string) (Response, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Response{}, false, err
	}
	req.Header.Set("User-Agent", userAgent)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	res, err := f.Client.Do(req)
	if err != nil {
		return Response{}, true, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, maxBody+1))
	if err != nil {
		return Response{}, true, err
	}
	if len(body) > maxBody {
		return Response{}, false, fmt.Errorf("body exceeds %d bytes", maxBody)
	}
	if res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500 {
		return Response{}, true, fmt.Errorf("status %d", res.StatusCode)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return Response{}, false, fmt.Errorf("status %d", res.StatusCode)
	}
	return Response{Body: body, ETag: strings.Trim(res.Header.Get("ETag"), `"`), URL: url}, false, nil
}
