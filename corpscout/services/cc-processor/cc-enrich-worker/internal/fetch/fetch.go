package fetch

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
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

// NewS3Getter signs requests (in-AWS instance role / configured creds); CommonCrawl's
// S3 API denies anonymous access.
// concurrency sizes the HTTP transport: every request hits the single CommonCrawl S3 endpoint,
// and the SDK's default transport keeps only 10 idle conns per host — at high --concurrency that
// means a fresh TLS handshake for most requests instead of connection reuse.
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
			"AWS_SECRET_ACCESS_KEY: %w", err)
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
