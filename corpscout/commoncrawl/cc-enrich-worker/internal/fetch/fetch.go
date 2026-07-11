package fetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/errors"
)

const maxS3BodyReadAttempts = 3

// S3Stats separates SDK/API latency, actual HTTP attempts, and response-body reads. Values are
// cumulative for the lifetime of an S3Getter; Delta isolates one worker phase or chunk.
type S3Stats struct {
	GetObjectCalls   int64
	GetObjectTime    time.Duration
	HTTPAttempts     int64
	HTTP503s         int64
	HTTPHeaderTime   time.Duration
	BodyReadAttempts int64
	BodyReadErrors   int64
	BodyReadRetries  int64
	BodyReadTime     time.Duration
	BodyBytes        int64
}

func (s S3Stats) Delta(previous S3Stats) S3Stats {
	return S3Stats{
		GetObjectCalls:   s.GetObjectCalls - previous.GetObjectCalls,
		GetObjectTime:    s.GetObjectTime - previous.GetObjectTime,
		HTTPAttempts:     s.HTTPAttempts - previous.HTTPAttempts,
		HTTP503s:         s.HTTP503s - previous.HTTP503s,
		HTTPHeaderTime:   s.HTTPHeaderTime - previous.HTTPHeaderTime,
		BodyReadAttempts: s.BodyReadAttempts - previous.BodyReadAttempts,
		BodyReadErrors:   s.BodyReadErrors - previous.BodyReadErrors,
		BodyReadRetries:  s.BodyReadRetries - previous.BodyReadRetries,
		BodyReadTime:     s.BodyReadTime - previous.BodyReadTime,
		BodyBytes:        s.BodyBytes - previous.BodyBytes,
	}
}

type s3Counters struct {
	getObjectCalls, getObjectNs      atomic.Int64
	httpAttempts, http503s, headerNs atomic.Int64
	bodyAttempts, bodyErrors         atomic.Int64
	bodyRetries, bodyNs, bodyBytes   atomic.Int64
}

type s3StatsTransport struct {
	base  http.RoundTripper
	stats *s3Counters
}

func (t s3StatsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	started := time.Now()
	response, err := t.base.RoundTrip(req)
	t.stats.httpAttempts.Add(1)
	t.stats.headerNs.Add(time.Since(started).Nanoseconds())
	if response != nil && response.StatusCode == http.StatusServiceUnavailable {
		t.stats.http503s.Add(1)
	}
	return response, err
}

// S3Getter is the production RangeGetter backed by aws-sdk-go-v2.
type S3Getter struct {
	Client *s3.Client
	stats  *s3Counters
}

func (s *S3Getter) Stats() S3Stats {
	if s.stats == nil {
		return S3Stats{}
	}
	return S3Stats{
		GetObjectCalls:   s.stats.getObjectCalls.Load(),
		GetObjectTime:    time.Duration(s.stats.getObjectNs.Load()),
		HTTPAttempts:     s.stats.httpAttempts.Load(),
		HTTP503s:         s.stats.http503s.Load(),
		HTTPHeaderTime:   time.Duration(s.stats.headerNs.Load()),
		BodyReadAttempts: s.stats.bodyAttempts.Load(),
		BodyReadErrors:   s.stats.bodyErrors.Load(),
		BodyReadRetries:  s.stats.bodyRetries.Load(),
		BodyReadTime:     time.Duration(s.stats.bodyNs.Load()),
		BodyBytes:        s.stats.bodyBytes.Load(),
	}
}

// NewS3Getter signs requests (in-AWS instance role / configured creds). CommonCrawl's
// S3 API denies anonymous access — off-AWS use httpRangeGetter instead.
// concurrency sizes the HTTP transport: every request hits the single CommonCrawl S3 endpoint,
// and the SDK's default transport keeps only 10 idle conns per host — at high --concurrency that
// means a fresh TLS handshake for most requests instead of connection reuse (same sizing as
// NewHTTPGetter).
func NewS3Getter(ctx context.Context, region string, concurrency int) (RangeGetter, error) {
	if concurrency <= 0 {
		concurrency = 16
	}
	stats := &s3Counters{}
	transport := &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		MaxConnsPerHost:     concurrency,
	}
	httpClient := &http.Client{Transport: s3StatsTransport{base: transport, stats: stats}}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region),
		// CommonCrawl's bucket returns 503 SlowDown under our fetch concurrency (tech = conc×8 in flight).
		// Adaptive mode adds a client-side token-bucket rate limiter that backs off automatically on
		// throttling responses and ramps back up — so the client self-tunes to the bucket's ceiling
		// instead of hammering it. More attempts absorb a transient 503 rather than dropping the page
		// (a dropped page counts toward the C1 error-rate trip).
		awsconfig.WithRetryMode(aws.RetryModeAdaptive),
		awsconfig.WithRetryMaxAttempts(10))
	if err != nil {
		return &S3Getter{}, err
	}
	// Validate creds upfront so we fail fast with a clear message off-AWS instead of
	// hanging on the 169.254 IMDS lookup for every fetch.
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := cfg.Credentials.Retrieve(cctx); err != nil {
		return &S3Getter{}, fmt.Errorf("no usable AWS credentials — export AWS_ACCESS_KEY_ID + "+
			"AWS_SECRET_ACCESS_KEY (or run with --s3-anonymous): %w", err)
	}
	// Install the measured transport only after credential resolution so IMDS/STS traffic does not
	// contaminate the S3 request counters.
	cfg.HTTPClient = httpClient
	return &S3Getter{Client: s3.NewFromConfig(cfg), stats: stats}, nil
}

func (s *S3Getter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	var lastReadErr error
	for attempt := 1; attempt <= maxS3BodyReadAttempts; attempt++ {
		getStarted := time.Now()
		out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Range:  aws.String(fmt.Sprintf("bytes=%d-%d", start, end)),
		})
		if s.stats != nil {
			s.stats.getObjectCalls.Add(1)
			s.stats.getObjectNs.Add(time.Since(getStarted).Nanoseconds())
		}
		if err != nil {
			return nil, errors.Wrap(err, "get S3 range")
		}

		readStarted := time.Now()
		body, readErr := io.ReadAll(out.Body)
		_ = out.Body.Close()
		if s.stats != nil {
			s.stats.bodyAttempts.Add(1)
			s.stats.bodyNs.Add(time.Since(readStarted).Nanoseconds())
			s.stats.bodyBytes.Add(int64(len(body)))
		}
		if readErr == nil {
			return body, nil
		}

		lastReadErr = readErr
		if s.stats != nil {
			s.stats.bodyErrors.Add(1)
		}
		if ctx.Err() != nil {
			return nil, errors.Wrap(ctx.Err(), "read S3 range body")
		}
		if attempt < maxS3BodyReadAttempts && s.stats != nil {
			s.stats.bodyRetries.Add(1)
		}
	}
	return nil, errors.Wrapf(lastReadErr, "read S3 range body after %d attempts", maxS3BodyReadAttempts)
}

// httpRangeGetter fetches byte ranges over CommonCrawl's anonymous HTTPS CDN
// (data.commoncrawl.org), the only anonymous path — the S3 API denies unsigned reads.
type httpRangeGetter struct {
	base   string
	client *http.Client
}

func NewHTTPGetter(base string, concurrency int) RangeGetter {
	if base == "" {
		base = "https://data.commoncrawl.org/"
	}
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	if concurrency <= 0 {
		concurrency = 16
	}
	return httpRangeGetter{base: base, client: &http.Client{Transport: &http.Transport{
		MaxIdleConns:        concurrency * 2,
		MaxIdleConnsPerHost: concurrency * 2,
		MaxConnsPerHost:     concurrency,
	}}}
}

func (g httpRangeGetter) GetRange(ctx context.Context, bucket, key string, start, end int64) ([]byte, error) {
	rng := fmt.Sprintf("bytes=%d-%d", start, end)
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if attempt > 0 { // exponential backoff for transient throttling (429/5xx)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt*attempt) * 250 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+key, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Range", rng)
		resp, err := g.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusPartialContent {
			b, rerr := io.ReadAll(resp.Body)
			resp.Body.Close()
			return b, rerr
		}
		status := resp.StatusCode
		resp.Body.Close()
		lastErr = fmt.Errorf("http %d fetching %s", status, key)
		if status != http.StatusTooManyRequests && status < 500 {
			return nil, lastErr // 403/404 etc. are permanent — don't retry
		}
	}
	return nil, lastErr
}
