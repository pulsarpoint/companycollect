package fetch

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strconv"
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
	HeadObjectCalls  int64
	HeadObjectTime   time.Duration
	GetObjectCalls   int64
	GetObjectTime    time.Duration
	HTTPAttempts     int64
	HTTP429s         int64
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
		HeadObjectCalls:  s.HeadObjectCalls - previous.HeadObjectCalls,
		HeadObjectTime:   s.HeadObjectTime - previous.HeadObjectTime,
		GetObjectCalls:   s.GetObjectCalls - previous.GetObjectCalls,
		GetObjectTime:    s.GetObjectTime - previous.GetObjectTime,
		HTTPAttempts:     s.HTTPAttempts - previous.HTTPAttempts,
		HTTP429s:         s.HTTP429s - previous.HTTP429s,
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
	headObjectCalls, headObjectNs              atomic.Int64
	getObjectCalls, getObjectNs                atomic.Int64
	httpAttempts, http429s, http503s, headerNs atomic.Int64
	bodyAttempts, bodyErrors                   atomic.Int64
	bodyRetries, bodyNs, bodyBytes             atomic.Int64
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
	if response != nil {
		switch response.StatusCode {
		case http.StatusTooManyRequests:
			t.stats.http429s.Add(1)
		case http.StatusServiceUnavailable:
			t.stats.http503s.Add(1)
		}
	}
	return response, err
}

// S3Getter is the production RangeGetter backed by aws-sdk-go-v2.
type S3Getter struct {
	Client *s3.Client
	stats  *s3Counters
}

var _ ObjectGetter = (*S3Getter)(nil)

func (s *S3Getter) Stats() S3Stats {
	if s.stats == nil {
		return S3Stats{}
	}
	return S3Stats{
		HeadObjectCalls:  s.stats.headObjectCalls.Load(),
		HeadObjectTime:   time.Duration(s.stats.headObjectNs.Load()),
		GetObjectCalls:   s.stats.getObjectCalls.Load(),
		GetObjectTime:    time.Duration(s.stats.getObjectNs.Load()),
		HTTPAttempts:     s.stats.httpAttempts.Load(),
		HTTP429s:         s.stats.http429s.Load(),
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
func NewS3Getter(ctx context.Context, region string, concurrency int) (*S3Getter, error) {
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

func (s *S3Getter) ObjectSize(ctx context.Context, bucket, key string) (int64, error) {
	headStarted := time.Now()
	out, err := s.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if s.stats != nil {
		s.stats.headObjectCalls.Add(1)
		s.stats.headObjectNs.Add(time.Since(headStarted).Nanoseconds())
	}
	if err != nil {
		return 0, errors.Wrap(err, "head S3 object")
	}
	if out.ContentLength == nil || *out.ContentLength < 0 {
		return 0, errors.Newf("S3 object has invalid content length bucket=%s key=%s", bucket, key)
	}
	return *out.ContentLength, nil
}

func (s *S3Getter) DownloadObject(ctx context.Context, bucket, key string, destination *os.File) error {
	if destination == nil {
		return errors.New("download S3 object: destination is nil")
	}

	var lastReadErr error
	for attempt := 1; attempt <= maxS3BodyReadAttempts; attempt++ {
		if err := resetDestination(destination); err != nil {
			return errors.Wrap(err, "reset S3 object destination")
		}

		getStarted := time.Now()
		out, err := s.Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if s.stats != nil {
			s.stats.getObjectCalls.Add(1)
			s.stats.getObjectNs.Add(time.Since(getStarted).Nanoseconds())
		}
		if err != nil {
			return errors.Wrap(err, "get S3 object")
		}

		readStarted := time.Now()
		written, readErr := io.Copy(destination, out.Body)
		closeErr := out.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		if s.stats != nil {
			s.stats.bodyAttempts.Add(1)
			s.stats.bodyNs.Add(time.Since(readStarted).Nanoseconds())
			s.stats.bodyBytes.Add(written)
		}
		if readErr == nil {
			return nil
		}

		lastReadErr = readErr
		if s.stats != nil {
			s.stats.bodyErrors.Add(1)
		}
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "download S3 object body")
		}
		if attempt < maxS3BodyReadAttempts && s.stats != nil {
			s.stats.bodyRetries.Add(1)
		}
	}
	return errors.Wrapf(lastReadErr, "download S3 object body after %d attempts", maxS3BodyReadAttempts)
}

func resetDestination(destination *os.File) error {
	if err := destination.Truncate(0); err != nil {
		return errors.Wrap(err, "truncate destination")
	}
	if _, err := destination.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "seek destination")
	}
	return nil
}

// httpRangeGetter fetches byte ranges over CommonCrawl's anonymous HTTPS CDN
// (data.commoncrawl.org), the only anonymous path — the S3 API denies unsigned reads.
type httpRangeGetter struct {
	base   string
	client *http.Client
}

var _ ObjectGetter = httpRangeGetter{}

func NewHTTPGetter(base string, concurrency int) ObjectGetter {
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
	if start < 0 || end < start || end-start == math.MaxInt64 {
		return nil, errors.Newf("invalid HTTP range %d-%d", start, end)
	}
	rng := fmt.Sprintf("bytes=%d-%d", start, end)
	expectedBytes := end - start + 1
	expectedContentRange := fmt.Sprintf("bytes %d-%d/", start, end)
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if err := waitForHTTPRetry(ctx, attempt); err != nil {
			return nil, errors.Wrap(err, "wait to retry HTTP range")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+key, nil)
		if err != nil {
			return nil, errors.Wrap(err, "create HTTP range request")
		}
		req.Header.Set("Range", rng)
		resp, err := g.client.Do(req)
		if err != nil {
			lastErr = errors.Wrap(err, "get HTTP range")
			continue
		}
		if resp.StatusCode == http.StatusPartialContent {
			if !strings.HasPrefix(resp.Header.Get("Content-Range"), expectedContentRange) {
				_ = resp.Body.Close()
				lastErr = errors.Newf("invalid HTTP Content-Range %q for requested %s", resp.Header.Get("Content-Range"), rng)
				continue
			}
			body, readErr := io.ReadAll(io.LimitReader(resp.Body, expectedBytes+1))
			_ = resp.Body.Close()
			if readErr != nil {
				lastErr = errors.Wrap(readErr, "read HTTP range body")
				continue
			}
			if int64(len(body)) != expectedBytes {
				lastErr = errors.Newf("HTTP range returned %d bytes, want %d", len(body), expectedBytes)
				continue
			}
			return body, nil
		}
		status := resp.StatusCode
		_ = resp.Body.Close()
		if status == http.StatusOK {
			return nil, errors.Newf("HTTP server ignored %s for %s; refusing to read the complete object", rng, key)
		}
		lastErr = errors.Newf("HTTP %d fetching %s", status, key)
		if !isRetryableHTTPStatus(status) {
			return nil, lastErr // 403/404 etc. are permanent — don't retry
		}
	}
	return nil, errors.Wrap(lastErr, "fetch HTTP range after retries")
}

func (g httpRangeGetter) ObjectSize(ctx context.Context, bucket, key string) (int64, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if err := waitForHTTPRetry(ctx, attempt); err != nil {
			return 0, errors.Wrap(err, "wait to retry HTTP object size")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, g.base+key, nil)
		if err != nil {
			return 0, errors.Wrap(err, "create HTTP object size request")
		}
		resp, err := g.client.Do(req)
		if err != nil {
			lastErr = errors.Wrap(err, "head HTTP object")
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
			contentLength := resp.Header.Get("Content-Length")
			size, parseErr := strconv.ParseInt(contentLength, 10, 64)
			if parseErr != nil {
				return 0, errors.Wrapf(parseErr, "parse HTTP object content length %q for key=%s", contentLength, key)
			}
			if size < 0 {
				return 0, errors.Newf("HTTP object has negative content length %d for key=%s", size, key)
			}
			return size, nil
		}
		lastErr = errors.Newf("HTTP %d heading %s", resp.StatusCode, key)
		if !isRetryableHTTPStatus(resp.StatusCode) {
			return 0, lastErr
		}
	}
	return 0, errors.Wrap(lastErr, "head HTTP object after retries")
}

func (g httpRangeGetter) DownloadObject(ctx context.Context, bucket, key string, destination *os.File) error {
	if destination == nil {
		return errors.New("download HTTP object: destination is nil")
	}

	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		if err := waitForHTTPRetry(ctx, attempt); err != nil {
			return errors.Wrap(err, "wait to retry HTTP object download")
		}
		if err := resetDestination(destination); err != nil {
			return errors.Wrap(err, "reset HTTP object destination")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.base+key, nil)
		if err != nil {
			return errors.Wrap(err, "create HTTP object download request")
		}
		resp, err := g.client.Do(req)
		if err != nil {
			lastErr = errors.Wrap(err, "get HTTP object")
			continue
		}
		if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
			status := resp.StatusCode
			_ = resp.Body.Close()
			lastErr = errors.Newf("HTTP %d fetching %s", status, key)
			if !isRetryableHTTPStatus(status) {
				return lastErr
			}
			continue
		}
		_, readErr := io.Copy(destination, resp.Body)
		closeErr := resp.Body.Close()
		if readErr == nil {
			readErr = closeErr
		}
		if readErr == nil {
			return nil
		}
		lastErr = errors.Wrap(readErr, "read HTTP object body")
	}
	return errors.Wrap(lastErr, "download HTTP object after retries")
}

func waitForHTTPRetry(ctx context.Context, attempt int) error {
	if attempt == 0 {
		return nil
	}
	timer := time.NewTimer(time.Duration(attempt*attempt) * 250 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isRetryableHTTPStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}
